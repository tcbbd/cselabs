// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  initial_mutex = new pthread_mutex_t;
  pthread_mutex_init(initial_mutex, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire begin\n",
            lid, id.data(), pthread_self());
    //lock's corresponding mutex hasn't been initialized
    pthread_mutex_lock(initial_mutex);
    if (lock_mutexes.find(lid) == lock_mutexes.end()) {
        pthread_mutex_t mutex;
        pthread_mutex_init(&mutex, NULL);
        pthread_cond_t cond;
        pthread_cond_init(&cond, NULL);
        lock_mutexes[lid] = std::make_pair(mutex, cond);
    }
    pthread_mutex_unlock(initial_mutex);

    lock_protocol::status ret;
    bool acquire = false;
    pthread_mutex_lock(&lock_mutexes[lid].first);
    if (lock_states.find(lid) == lock_states.end()) {
        //know nothing about the lock, try to acquire through RPC
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                " state NOT EXIST -> NONE, do rpc call\n", lid, id.data(), pthread_self());
        lock_states[lid] = NONE;
        acquire = true;
    }
    else {
        switch (lock_states[lid]) {
        case FREE:
            log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                    " state FREE -> LOCKED\n", lid, id.data(), pthread_self());
            lock_states[lid] = LOCKED;
            break;
        case NONE:
        case LOCKED:
        case ACQUIRING:
        case RETRIED: case REVOKED: case REVOKED_RETRIED:
        case RELEASING: case RELEASING_RETRYING:
            log("Lock ID: %llu, Client ID: %s, Client Thread: %lu,"
                    " acquire, start waiting\n", lid, id.data(), pthread_self());
            while (lock_states[lid] != FREE && lock_states[lid] != RELEASED &&
                    lock_states[lid] != PRE_RELEASING &&
                    lock_states[lid] != PRE_RELEASING_RETRYING) {
                pthread_cond_wait(&lock_mutexes[lid].second, &lock_mutexes[lid].first);
            }
            if (lock_states[lid] == FREE) {
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " awaken, state FREE -> LOCKED\n", lid, id.data(), pthread_self());
                lock_states[lid] = LOCKED;
            }
            else if (lock_states[lid] == RELEASED) {
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire, awaken,"
                        " state RELEASED -> NONE, do rpc call\n", lid, id.data(), pthread_self());
                lock_states[lid] = NONE;
                acquire = true;
            }
            else if (lock_states[lid] == PRE_RELEASING) {
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire, awaken,"
                        " state PRE_RELEASING -> RELEASING\n", lid, id.data(), pthread_self());
                lock_states[lid] = RELEASING;
            }
            else if (lock_states[lid] == PRE_RELEASING_RETRYING) {
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " awaken, state PRE_RELEASING_RETRYING -> RELEASING_RETRYING\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = RELEASING_RETRYING;
            }
            break;
        case PRE_RELEASING_RETRYING:
            log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                    " state PRE_RELEASING_RETRYING -> RELEASING_RETRYING\n",
                    lid, id.data(), pthread_self());
            lock_states[lid] = RELEASING_RETRYING;
            break;
        case PRE_RELEASING:
            log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                    " state PRE_RELEASING -> RELEASING\n", lid, id.data(), pthread_self());
            lock_states[lid] = RELEASING;
            break;
        case RELEASED:
            //acquire after released, try to acquire through RPC
            log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                    " state RELEASED -> NONE, do rpc call\n", lid, id.data(), pthread_self());
            lock_states[lid] = NONE;
            acquire = true;
            break;
        default:
            break;
        }
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    while (acquire) {
        acquire = false;
        int r;
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu,"
                " acquire, acquire RPC call begin\n", lid, id.data(), pthread_self());
        ret = cl->call(lock_protocol::acquire, lid, id, r);
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu,"
                " acquire, acquire RPC call end\n", lid, id.data(), pthread_self());
        pthread_mutex_lock(&lock_mutexes[lid].first);
        if (ret == lock_protocol::OK) {
            switch(lock_states[lid]) {
            case NONE:
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond OK, state NONE -> LOCKED\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = LOCKED;
                break;
            case REVOKED:
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond OK, state REVOKED -> RELEASING\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = RELEASING;
                break;
            default:
                //impossible to be in such states
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond OK, state ERROR!\n",
                        lid, id.data(), pthread_self());
                break;
            }
        }
        else if (ret == lock_protocol::RETRY) {
            switch(lock_states[lid]) {
            case NONE:
                //The order is correct
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond RETRY, state NONE -> ACQUIRING\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = ACQUIRING;
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond RETRY, state ACQUIRING, start waiting\n",
                        lid, id.data(), pthread_self());
                while (lock_states[lid] != FREE && lock_states[lid] != RELEASED &&
                        lock_states[lid] != PRE_RELEASING &&
                        lock_states[lid] != PRE_RELEASING_RETRYING) {
                    pthread_cond_wait(&lock_mutexes[lid].second, &lock_mutexes[lid].first);
                }
                if (lock_states[lid] == FREE) {
                    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                            " acquire RPC call respond RETRY, awaken, state FREE -> LOCKED\n",
                            lid, id.data(), pthread_self());
                    lock_states[lid] = LOCKED;
                }
                else if (lock_states[lid] == RELEASED) {
                    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                            " acquire RPC call respond RETRY, awaken, state RELEASED -> NONE,"
                            " do rpc call\n", lid, id.data(), pthread_self());
                    lock_states[lid] = NONE;
                    acquire = true;
                }
                else if (lock_states[lid] == PRE_RELEASING) {
                    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                            " acquire RPC call respond RETRY, awaken,"
                            " state PRE_RELEASING -> RELEASING\n",
                            lid, id.data(), pthread_self());
                    lock_states[lid] = RELEASING;
                }
                else if (lock_states[lid] == PRE_RELEASING_RETRYING) {
                    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                            " acquire RPC call respond RETRY, awaken,"
                            " state PRE_RELEASING_RETRYING -> RELEASING_RETRYING\n",
                            lid, id.data(), pthread_self());
                    lock_states[lid] = RELEASING_RETRYING;
                }
                break;
            case RETRIED:
                //first a retry call, then RETRY response
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond RETRY, state RETRIED -> LOCKED\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = LOCKED;
                break;
            case REVOKED:
                //first a revoke call, then RETRY response
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond RETRY, state REVOKED -> RELEASING_RETRYING\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = RELEASING_RETRYING;
                break;
            case REVOKED_RETRIED:
                //first a retry call and a revoke call (can be in either
                //order), then RETRY response
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond RETRY, state REVOKED_RETRIED -> RELEASING\n",
                        lid, id.data(), pthread_self());
                lock_states[lid] = RELEASING;
                break;
            default:
                //impossible to be in such states
                log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire,"
                        " acquire RPC call respond OK, state ERROR!\n",
                        lid, id.data(), pthread_self());
                break;
            }
        }
        pthread_mutex_unlock(&lock_mutexes[lid].first);
    }
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, acquire end\n",
            lid, id.data(), pthread_self());
    return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release begin\n",
            lid, id.data(), pthread_self());
    bool release = false;
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch (lock_states[lid]) {
    case LOCKED:
        //just free it and signal a thread to acquire the lock
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " state LOCKED -> FREE\n", lid, id.data(), pthread_self());
        lock_states[lid] = FREE;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    case RELEASING_RETRYING:
        //should wait until a retry call comes
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " state RELEASING_RETRYING, start waiting\n", lid, id.data(), pthread_self());
        while (lock_states[lid] != RELEASING) {
            pthread_cond_wait(&lock_mutexes[lid].second, &lock_mutexes[lid].first);
        }
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " awaken, state RELEASING\n", lid, id.data(), pthread_self());
    case RELEASING:
        //tell the server we've released the previously holding lock
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " state RELEASING -> RELEASED, do rpc call\n", lid, id.data(), pthread_self());
        lock_states[lid] = RELEASED;
        pthread_cond_signal(&lock_mutexes[lid].second);
        release = true;
        break;
    default:
        //should not release in such state, it's a logical error
        //(although one can release a FREE lock without side effect)
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " state ERROR!\n", lid, id.data(), pthread_self());
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    if (release) {
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " release RPC call begin\n", lid, id.data(), pthread_self());
        int r;
        cl->call(lock_protocol::release, lid, id, r);
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release,"
                " release RPC call end\n", lid, id.data(), pthread_self());
    }
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, release end\n",
            lid, id.data(), pthread_self());
    return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke begin\n",
            lid, id.data(), pthread_self());
    bool release = false;
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch(lock_states[lid]) {
    case FREE:
        //if free, release immedaitely
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state FREE -> RELEASED, do rpc call\n", lid, id.data(), pthread_self());
        lock_states[lid] = RELEASED;
        release = true;
        break;
    case NONE:
        //revoke call before OK or RETRY response
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state NONE -> REVOKED\n", lid, id.data(), pthread_self());
        lock_states[lid] = REVOKED;
        break;
    case LOCKED:
        //wait for the thread owning the lock to release
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state LOCKED -> RELEASING\n", lid, id.data(), pthread_self());
        lock_states[lid] = RELEASING;
        break;
    case ACQUIRING:
        //revoke call after RETRY response but before retry call
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state ACQUIRING -> PRE_RELEASING_RETRYING\n", lid, id.data(), pthread_self());
        lock_states[lid] = PRE_RELEASING_RETRYING;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    case RETRIED:
        //first a retry call, then a revoke call, all before RETRY response
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state RETRIED -> REVOKED_RETRIED\n", lid, id.data(), pthread_self());
        lock_states[lid] = REVOKED_RETRIED;
        break;
    default:
        //otherwise a revoke is impossible
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " state ERROR!\n", lid, id.data(), pthread_self());
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    if (release) {
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " release RPC call begin\n", lid, id.data(), pthread_self());
        int r;
        cl->call(lock_protocol::release, lid, id, r);
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke,"
                " release RPC call end\n", lid, id.data(), pthread_self());
    }
    int ret = rlock_protocol::OK;
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, revoke end\n",
            lid, id.data(), pthread_self());
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry begin\n",
            lid, id.data(), pthread_self());
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch (lock_states[lid]) {
    case NONE:
        //retry call before RETRY response
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state NONE -> RETRIED\n", lid, id.data(), pthread_self());
        lock_states[lid] = RETRIED;
        break;
    case ACQUIRING:
        //the correct order, retry call after RETRY response
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state ACQUIRING -> FREE\n", lid, id.data(), pthread_self());
        lock_states[lid] = FREE;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    case REVOKED:
        //first a revoke call, then retry call, both befor RETRY response
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state REVOKED -> REVOKED_RETRIED\n", lid, id.data(), pthread_self());
        lock_states[lid] = REVOKED_RETRIED;
        break;
    case RELEASING_RETRYING:
        //first revoke call and RETRY response (can be in either order), and
        //then retry call
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state RELEASING_RETRYING -> RELEASING\n", lid, id.data(), pthread_self());
        lock_states[lid] = RELEASING;
        //signal every thread so that we can inform the thread waiting for release
        pthread_cond_broadcast(&lock_mutexes[lid].second);
        break;
    case PRE_RELEASING_RETRYING:
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state PRE_RELEASING_RETRYING -> PRE_RELEASING\n",
                lid, id.data(), pthread_self());
        lock_states[lid] = PRE_RELEASING;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    default:
        //otherwise a retry call is impossible
        log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry,"
                " state ERROR!\n", lid, id.data(), pthread_self());
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    int ret = rlock_protocol::OK;
    log("Lock ID: %llu, Client ID: %s, Client Thread: %lu, retry end\n",
            lid, id.data(), pthread_self());
    return ret;
}




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
    printf("Acquire %llu\n", lid);
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
        lock_states[lid] = NONE;
        acquire = true;
    }
    else {
        switch (lock_states[lid]) {
        case FREE:
            lock_states[lid] = LOCKED;
            break;
        case NONE:
        case LOCKED:
        case ACQUIRING:
        case RELEASING:
            while (lock_states[lid] != FREE && lock_states[lid] != RELEASED) {
                pthread_cond_wait(&lock_mutexes[lid].second, &lock_mutexes[lid].first);
            }
            if (lock_states[lid] == FREE)
                lock_states[lid] = LOCKED;
            else if (lock_states[lid] == RELEASED) {
                lock_states[lid] = NONE;
                acquire = true;
            }
            break;
        case RELEASED:
            //acquire after released, try to acquire through RPC
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
        printf("acquire rpc call\n");
        ret = cl->call(lock_protocol::acquire, cl->id(), lid, id, r);
        printf("acquire rpc call done\n");
        pthread_mutex_lock(&lock_mutexes[lid].first);
        if (ret == lock_protocol::OK) {
            switch(lock_states[lid]) {
            case NONE:
                lock_states[lid] = LOCKED;
                break;
            case RELEASING:
                //it's possible that a revoke arrive before the OK response, which
                //causes state to be changed to RELEASING. But we should do nothing
                //with this case.
            default:
                //impossible to be in FREE, LOCKED, ACQUIRING or RELEASED state
                break;
            }
        }
        else if (ret == lock_protocol::RETRY) {
            switch(lock_states[lid]) {
            case FREE:
                //one can skip ACQUIRING state and straightly enter FREE state when
                //retry arrive before RETRY response
                lock_states[lid] = LOCKED;
                break;
            case RELEASED:
                //it's possible that a revoke arrive before the RETRY response, which
                //causes state to be chaned to RELEASED. Try to acquire through RPC.
                lock_states[lid] = NONE;
                acquire = true;
                break;
            case NONE:
                lock_states[lid] = ACQUIRING;
            case LOCKED:
                //one can skip ACQUIRING state and straightly enter FREE state
                //(then he can be in LOCKED state) when retry arrive before
                //RETRY response
            case RELEASING:
                //it's possible that a revoke arrive before the RETRY response, which
                //causes state to be changed to RELEASING. We should wait for the owner
                //thread to release.
            case ACQUIRING:
                //It's the most confusing case.
                //NOTE that it's possible that before this RETRY response
                //arrive, a retry arrived, and then a revoke, and then the lock
                //had been released, finnally another acquire RPC call was made
                //and the second RETRY response arrived. Now, the lock is in
                //ACQUIRING state, with a second acquire and RETRY done.
                //Actually, this can be further extended so that we can even
                //recieve the first RETRY after many cycles of retry, revoke,
                //acquire and RETRY, without awareness of what happened.
                while (lock_states[lid] != FREE && lock_states[lid] != RELEASED) {
                    pthread_cond_wait(&lock_mutexes[lid].second, &lock_mutexes[lid].first);
                }
                if (lock_states[lid] == FREE)
                    lock_states[lid] = LOCKED;
                else if (lock_states[lid] == RELEASED) {
                    lock_states[lid] = NONE;
                    acquire = true;
                }
                break;
            default:
                break;
            }
        }
        pthread_mutex_unlock(&lock_mutexes[lid].first);
    }
    return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    printf("Release %llu\n", lid);
    bool release = false;
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch (lock_states[lid]) {
    case LOCKED:
        //just free it and signal a thread to acquire the lock
        lock_states[lid] = FREE;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    case RELEASING:
        //tell the server we've released the previously holding lock
        lock_states[lid] = RELEASED;
        pthread_cond_signal(&lock_mutexes[lid].second);
        release = true;
        break;
    default:
        //if lock is already free, one should not release it again
        //should not release when NONE, ACQUIRING or RELEASED, otherwise
        //there is a logical error
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    if (release) {
        int r;
        cl->call(lock_protocol::release, cl->id(), lid, id, r);
    }
    return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
    bool release = false;
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch(lock_states[lid]) {
    case FREE:
        //if free, release immedaitely
        lock_states[lid] = RELEASED;
        release = true;
        break;
    case NONE:
        //a revoke can arrive before the OK response of acquire
        //(the OK response takes so much time that some other client
        //try to acquire the "ACQUIRED" lock which hasn't been acquired
        //at this client!)
    case LOCKED:
        lock_states[lid] = RELEASING;
        break;
    default:
        //a revoke when ACQUIRING is impossible, should not happen at all.
        //(because one must receive a RETRY to enter ACQUIRING state, and then
        //waiting for a retry from server. In the code of server_cache, the
        //following revoke must happen after the preceding retry returned)
        //
        //a revoke when RELEASING is impossible, should not happen at all.
        //(because server won't send revoke to the client twice)
        //
        //a revoke when RELEASED is impossible, should not happen at all.
        //(because at server side, either the client has't released the lock
        //(release hasn't arrived server yet) or the client has released but
        //hasn't reaquire the lock yet, the server won't revoke the client when
        //in such state )
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    if (release) {
        int r;
        cl->call(lock_protocol::release, cl->id(), lid, id, r);
    }
    int ret = rlock_protocol::OK;
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
    pthread_mutex_lock(&lock_mutexes[lid].first);
    switch (lock_states[lid]) {
    case NONE:
        //a retry can arrive before the RETRY response of acquire
        //(for a relatively long RETRY response, see preceding explanation
        //of NONE in revoke)
    case ACQUIRING:
        lock_states[lid] = FREE;
        pthread_cond_signal(&lock_mutexes[lid].second);
        break;
    default:
        //a retry when FREE, LOCKED or RELEASING is impossible, should not happen at all.
        //(because it means the client owns the lock)
        //
        //a retry when RELEASED is impossible, should not happen at all.
        //(because to make an acquire call to server, the client must enter
        //NONE state at first)
        break;
    }
    pthread_mutex_unlock(&lock_mutexes[lid].first);
    int ret = rlock_protocol::OK;
    return ret;
}




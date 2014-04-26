// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
    pthread_mutex_init(&initial_mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &)
{
    printf("Acquire lockid:%llu, clientid: %s\n", lid, id.data());
    //lock's corresponding mutex hasn't been initialized
    pthread_mutex_lock(&initial_mutex);
    if (lock_mutexes.find(lid) == lock_mutexes.end()) {
        pthread_mutex_t mutex;
        pthread_mutex_init(&mutex, NULL);
        lock_mutexes[lid] = mutex;
    }
    pthread_mutex_unlock(&initial_mutex);

    lock_protocol::status ret;
    bool revoke = false;
    std::string revoke_cl;
    pthread_mutex_lock(&lock_mutexes[lid]);
    if (locks.find(lid) == locks.end()) {
        //it's the first time this lock required by some client
        locks[lid] = new lockinfo_t(ACQUIRED, id);
        ret = lock_protocol::OK;
    }
    else {
        if (locks[lid]->state == FREE) {
            //actually, this should not happen
            locks[lid]->state = ACQUIRED;
            locks[lid]->client_id = id;
            ret = lock_protocol::OK;
        }
        else if (locks[lid]->state == ACQUIRED) {
            locks[lid]->waiting_clients.push(id);
            ret = lock_protocol::RETRY;
            //if no one else waiting, revoke the lock and give it to the client identified by id
            if(locks[lid]->waiting_clients.empty())
                revoke = true;
        }
    }
    pthread_mutex_unlock(&lock_mutexes[lid]);
    if (revoke) {
        handle h(revoke_cl);
        rpcc *cl = h.safebind();
        int r;
        if (cl != NULL)
            cl->call(rlock_protocol::revoke, cl->id(), lid, r);
    }
    return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
         int &r)
{
    lock_protocol::status ret;
    bool retry = false;
    bool revoke = false;
    std::string retry_cl;
    pthread_mutex_lock(&lock_mutexes[lid]);
    if (locks[lid]->client_id != id)
        //should be released by owner, otherwise it's a logical error
        ret = lock_protocol::RPCERR;
    else {
        if (locks[lid]->waiting_clients.empty())
            //actually, this should not happen
            locks[lid]->state = FREE;
        else {
            retry_cl = locks[lid]->client_id = locks[lid]->waiting_clients.front();
            locks[lid]->waiting_clients.pop();
            retry = true;
            if (!locks[lid]->waiting_clients.empty())
                revoke = true;
        }
    }
    pthread_mutex_unlock(&lock_mutexes[lid]);
    if (retry) {
        handle h(retry_cl);
        rpcc *cl = h.safebind();
        if (cl != NULL)
            cl->call(rlock_protocol::retry, cl->id(), lid, r);
        if (cl != NULL && revoke)
            cl->call(rlock_protocol::revoke, cl->id(), lid, r);
    }
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}


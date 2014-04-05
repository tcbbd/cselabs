// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
    pthread_mutex_init(&lock_mutex, NULL);
    pthread_cond_init(&lock_cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lock_mutex);
  if (lock_states.find(lid) == lock_states.end()) {
      //free now
      lock_states[lid] = true;
      nacquire++;
  }
  else {
      while (lock_states[lid] == true) {
          pthread_cond_wait(&lock_cond, &lock_mutex);
      }
      lock_states[lid] = true;
      nacquire++;
  }
  pthread_mutex_unlock(&lock_mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&lock_mutex);
  if (lock_states.find(lid) == lock_states.end())
      ret = lock_protocol::RETRY;
  else {
      lock_states[lid] = false;
      pthread_cond_signal(&lock_cond);
  }
  pthread_mutex_unlock(&lock_mutex);
  return ret;
}

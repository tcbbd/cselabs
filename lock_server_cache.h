#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include <queue>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include "handle.h"

struct revoke_info_t {
    rpcc *cl;
    lock_protocol::lockid_t lid;
    std::string id;
};

class lock_server_cache {
 private:
  int nacquire;

  enum lock_state_t { FREE, ACQUIRED };
  struct lockinfo_t {
      lockinfo_t (lock_state_t _state, std::string id) :
          state(_state), client_id(id) {}
      lock_state_t state;
      std::string client_id;
      std::queue<std::string> waiting_clients;
  };
  pthread_mutex_t initial_mutex;
  std::map<lock_protocol::lockid_t, pthread_mutex_t> lock_mutexes;
  std::map<lock_protocol::lockid_t, lockinfo_t *> locks;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif

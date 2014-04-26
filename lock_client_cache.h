// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;

  //NONE means client knows nothing about the lock
  //NOTE that one can enter FREE, LOCKED or RELEASING state from NONE state
  //
  //FREE means client owns the lock, and no thread owns the lock
  //
  //LOCKED means client owns the lock, and a thread owns the lock
  //
  //ACQUIRING means client doesn't own the lock, any thread try to
  //acquire the lock would be suspended, until a retry RPC call by server
  //NOTE that one must receive a RETRY to enter ACQUIRING state
  //
  //RELEASING means client should release the ownership of the lock after
  //the currently locked lock released by the owner thread
  //
  //RELEASED means client has just released the lock, and a following
  //acquisition should send an acquire RPC call to server

  enum lock_state_t { NONE, FREE, LOCKED, ACQUIRING, RELEASING, RELEASED };
  pthread_mutex_t *initial_mutex;
  std::map<lock_protocol::lockid_t, std::pair<pthread_mutex_t, pthread_cond_t> > lock_mutexes;
  std::map<int, lock_state_t> lock_states;
 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif

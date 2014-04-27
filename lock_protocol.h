// lock protocol

#ifndef lock_protocol_h
#define lock_protocol_h

#include "rpc.h"

//#define LAB_DEBUG

#ifdef LAB_DEBUG
#define log(args...) do { printf(args); } while (0);
#else
#define log(args...)
#endif

class lock_protocol {
 public:
  enum xxstatus { OK, RETRY, RPCERR, NOENT, IOERR };
  typedef int status;
  typedef unsigned long long lockid_t;
  enum rpc_numbers {
    acquire = 0x7001,
    release,
    stat
  };
};

class rlock_protocol {
public:
    enum xxstatus { OK, RPCERR };
    typedef int status;
    enum rpc_numbers {
        revoke = 0x8001,
        retry = 0x8002
    };
};

#endif 

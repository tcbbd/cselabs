// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "extent_server.h"
#include "lock_client_cache.h"

class extent_client {
 friend class lock_release_helper;

 private:
  rpcc *cl;
  struct cache_entry_t {
      cache_entry_t(std::string &str, extent_protocol::attr _attr) :
          extent(str), attr(_attr), dirty(false) {}
      std::string extent;
      extent_protocol::attr attr;
      bool dirty;
  };
  std::map<extent_protocol::extentid_t, cache_entry_t *> cache;
  pthread_mutex_t mutex;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
};

class lock_release_helper : public lock_release_user {
 public:
  lock_release_helper(extent_client *_ec) : ec(_ec) {}
  void dorelease(lock_protocol::lockid_t);
 private:
  extent_client *ec;
};

#endif 


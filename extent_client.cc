// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "tprintf.h"

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  pthread_mutex_init(&mutex, NULL);
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  log("GETATTR Cache, ID: %llu>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", eid);
  pthread_mutex_lock(&mutex);
  if (cache.find(eid) != cache.end()) {
    attr = cache[eid]->attr;
  }
  else {
    std::string buf;
    cl->call(extent_protocol::get, eid, buf);
    cl->call(extent_protocol::getattr, eid, attr);
    cache[eid] = new cache_entry_t(buf, attr);
  }
  pthread_mutex_unlock(&mutex);
  log("GETATTR Cache, ID: %llu<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n", eid);
  return ret;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&mutex);
  ret = cl->call(extent_protocol::create, type, id);
  if (ret == extent_protocol::OK) {
    extent_protocol::attr attr;
    std::string extent;
    cl->call(extent_protocol::getattr, id, attr);
    cache[id] = new cache_entry_t(extent, attr);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  log("GET Cache, ID: %llu>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", eid);
  pthread_mutex_lock(&mutex);
  if (cache.find(eid) != cache.end()) {
    cache[eid]->attr.atime = time(NULL);
    buf = cache[eid]->extent;
  }
  else {
    extent_protocol::attr attr;
    cl->call(extent_protocol::get, eid, buf);
    cl->call(extent_protocol::getattr, eid, attr);
    cache[eid] = new cache_entry_t(buf, attr);
  }
  pthread_mutex_unlock(&mutex);
  log("GET Cache, ID: %llu<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n", eid);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  log("PUT Cache, ID: %llu>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", eid);
  pthread_mutex_lock(&mutex);
  if (cache.find(eid) != cache.end()) {
    cache[eid]->attr.mtime = time(NULL);
    if (buf.size() != cache[eid]->attr.size) {
      cache[eid]->attr.ctime = cache[eid]->attr.mtime;
      cache[eid]->attr.size = buf.size();
    }
    cache[eid]->extent = buf;
    cache[eid]->dirty = true;
  }
  else {
    extent_protocol::attr attr;
    cl->call(extent_protocol::getattr, eid, attr);
    attr.mtime = time(NULL);
    if (buf.size() != attr.size) {
      attr.ctime = attr.mtime;
      attr.size = buf.size();
    }
    cache[eid] = new cache_entry_t(buf, attr);
    cache[eid]->dirty = true;
  }
  pthread_mutex_unlock(&mutex);
  log("PUT Cache, ID: %llu<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n", eid);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  log("REMOVE Cache, ID: %llu>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", eid);
  pthread_mutex_lock(&mutex);
  if (cache.find(eid) != cache.end()) {
    cache_entry_t *cache_entry = cache[eid];
    delete cache_entry;
    cache.erase(eid);
  }
  pthread_mutex_unlock(&mutex);
  log("REMOVE Cache, ID: %llu<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n", eid);
  return ret;
}

void
lock_release_helper::dorelease(lock_protocol::lockid_t lid) {
  int r = 0;
  log("FLUSH Cache, ID: %llu>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n", lid);
  pthread_mutex_lock(&ec->mutex);
  if (ec->cache.find(lid) != ec->cache.end()) {
    if (ec->cache[lid]->dirty)
      ec->cl->call(extent_protocol::put, lid, ec->cache[lid]->extent, r);
    extent_client::cache_entry_t *cache_entry = ec->cache[lid];
    delete cache_entry;
    ec->cache.erase(lid);
  }
  else
    ec->cl->call(extent_protocol::remove, lid, r);
  pthread_mutex_unlock(&ec->mutex);
  log("FLUSH Cache, ID: %llu<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n", lid);
}

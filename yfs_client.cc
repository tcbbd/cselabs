// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// Allowed characters in filenames:
// All bytes except NUL('\0') and '/'
// Directory entry looks like this: name\0inum\0
// where inum is stored as plain text(i.e. ASCII code)
// instead of binary word

//#define DEBUG

yfs_client::yfs_client()
{
    ec = new extent_client();
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir or a symlink\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is a file or a symlink\n", inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("issymlink: %lld is a file or a dir\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int
yfs_client::getsymlink(inum inum, symlinkinfo &sin)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;
    printf("getsymlink %016llx -> sz %llu\n", inum, sin.size);

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    printf("setattr %016llx sz->%lu\n", ino, size);
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("error reading file\n");
        r = IOERR;
        goto release;
    }

    content.resize(size, '\0');
    ec->put(ino, content);

release:
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    // mode should be ignored

    printf("create %s\n", name);
    std::string content;
    bool found;

    lookup(parent, name, found, ino_out);
    if (found) {
        r = EXIST;
        goto release;
    }

    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        printf("error creating file\n");
        r = IOERR;
        goto release;
    }

    ec->get(parent, content);
    content += std::string(name) + '\0';
    content += filename(ino_out) + '\0';
    ec->put(parent, content);

release:
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    // mode should be ignored

    printf("mkdir %s\n", name);
    std::string content;
    bool found;

    lookup(parent, name, found, ino_out);
    if (found) {
        r = EXIST;
        goto release;
    }

    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        printf("error creating directory\n");
        r = IOERR;
        goto release;
    }

    ec->get(parent, content);
    content += std::string(name) + '\0';
    content += filename(ino_out) + '\0';
    ec->put(parent, content);

release:
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    printf("lookup %s\n", name);
    std::list<dirent> entries;
    dirent entry;
    found = false;

    r = readdir(parent, entries);
    if (r != OK)
        goto release;

    for (std::list<dirent>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        if (iter->name == std::string(name)) {
            found = true;
            ino_out = iter->inum;
            goto release;
        }
    }

release:
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    printf("readdir %016llx\n", dir);
    std::string content;
    std::string name;
    std::string index;
    dirent entry;

    // not necessary to check dir is a directory
    // it would be checked in fuse.cc before invoking this method

    if (ec->get(dir, content) != extent_protocol::OK) {
        printf("error getting content\n");
        r = IOERR;
        goto release;
    }

    for (int i = 0; i < content.size();) {
        for (; i < content.size(); i++) {
            if (content[i] != '\0')
                name.push_back(content[i]);
            else {
                i++;
                break;
            }
        }
        for (; i < content.size(); i++) {
            if (content[i] != '\0')
                index.push_back(content[i]);
            else {
                i++;
                break;
            }
        }
        entry.name = name;
        entry.inum = n2i(index);
        list.push_back(entry);
        name.clear();
        index.clear();
    }

release:
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    printf("read %016llx size:%lu offset:%ld\n", ino, size, off);
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("error reading file\n");
        r = IOERR;
        goto release;
    }

#ifdef DEBUG
    printf("read content:\n");
    for (int i = 0; i < content.size(); i++) {
        printf("%c", content[i]);
    }
    printf("\n");
#endif
    // less than size bytes available
    if(off + size > content.size())
        data = content.substr(off);
    else
        data = content.substr(off, size);
    printf("read %lu bytes\n", data.size());

release:
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    
    printf("write %016llx size:%lu offset:%ld\nwrite content:%s\n", ino, size, off, data);
    std::string content;

    if (ec->get(ino, content) != extent_protocol::OK) {
        printf("error reading file\n");
        r = IOERR;
        goto release;
    }

#ifdef DEBUG
    printf("write before:\n");
    for (int i = 0; i < content.size(); i++) {
        printf("%c", content[i]);
    }
    printf("\n");
#endif
    // fill "holes" with '\0's
    if (off >= content.size())
        bytes_written = size + off - content.size();
    else
        bytes_written = size;
    if (off + size > content.size())
        content.resize(off + size, '\0');
    content.replace(off, size, data, size);
    ec->put(ino, content);
#ifdef DEBUG
    printf("write after:\n");
    for (int i = 0; i < content.size(); i++) {
        printf("%c", content[i]);
    }
    printf("\n");
#endif
    printf("write %lu bytes\n", bytes_written);

release:
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    printf("unlink %s\n", name);
    std::string content;
    bool found;
    inum ino_out;
    size_t pos, len;

    lookup(parent, name, found, ino_out);
    if (!found) {
        r = NOENT;
        goto release;
    }

    if (ec->remove(ino_out) != extent_protocol::OK) {
        printf("error deleting file\n");
        r = IOERR;
        goto release;
    }

    ec->get(parent, content);
    pos = content.find(name);
    len = 0;
    while (content[pos + len] != '\0') { len++; }
    len++;
    while (content[pos + len] != '\0') { len++; }
    len++;
    content.erase(pos, len);
    printf("unlink: pos = %lu, len = %lu\n", pos, len);
    ec->put(parent, content);

release:
    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
{
    int r = OK;

    printf("symlink %s -> %s\n", name, link);

    std::string content;
    bool found;
    size_t bytes_written;

    lookup(parent, name, found, ino_out);
    if (found) {
        r = EXIST;
        goto release;
    }

    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        printf("error symlinking file\n");
        r = IOERR;
        goto release;
    }

    ec->get(parent, content);
    content += std::string(name) + '\0';
    content += filename(ino_out) + '\0';
    ec->put(parent, content);

    write(ino_out, strlen(link), 0, link, bytes_written);

release:
    return r;
}

int yfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;

    printf("readlink %016llx\n", ino);

    if (ec->get(ino, data) != extent_protocol::OK) {
        printf("error reading file\n");
        r = IOERR;
    }

    return r;
}

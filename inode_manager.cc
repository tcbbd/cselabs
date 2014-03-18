#include "inode_manager.h"

#define DEBUG

// First data block
#define DBLOCK (BLOCK_NUM / BPB + INODE_NUM + 4)

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */

  blockid_t id;
  uint32_t off;
  char buf[BLOCK_SIZE];

  printf("\tim: alloc_block\n");

  for (id = DBLOCK; id < BLOCK_NUM; id++) {
    off = id % BPB;
    read_block(BBLOCK(id), buf);

    // if block is free
    if ((~(buf[off / 8] | (~(1 << (off % 8))))) != 0) {
      // allocate this block
      buf[off / 8] |= 1 << (off % 8);
      write_block(BBLOCK(id), buf);
      printf("allocated id:%d\n", id);
      return id;
    }
  }

  printf("\tim: cannot alloc new block\n");

  //block id should never be 0(MBR block), represents failed
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  
  uint32_t off;
  char buf[BLOCK_SIZE];

  printf("\tim: free_block %d\n", id);

  if (id < DBLOCK || id >= BLOCK_NUM) {
    printf("\tim: id out of range\n");
    return;
  }

  off = id % BPB;
  read_block(BBLOCK(id), buf);

  //free block in bitmap
  buf[off / 8] &= ~(1 << (off % 8));

  write_block(BBLOCK(id), buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  char buf[BLOCK_SIZE];

  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
  
  // block 0 is for MBR
  // block 1 is superblock
  bzero(buf, sizeof(buf));
  memcpy(buf, &sb, sizeof(sb));
  write_block(1, buf);

  // several blocks for free block bitmap
  // initialized to be all zero
  // from block 2 to block (BLOCK_NUM / BPB + 1)

  // block (BLOCK_NUM / BPB + 2) and (BLOCK_NUM / BPB + 3) remain unused
  // I don't know why, just declared in header file originally
  // And I'm not suggested to modify the header

  // several blocks for inode table
  // initialized to be all zero
  // inode.type = 0 represents that this inode if free
  // only one inode per block
  // inode_num count from 1 instead of 0
  // from block (BLOCK_NUM / BPB + 4) to block (BLOCK_NUM / BPB + INODE_NUM + 3)

  // data block start from block (BLOCK_NUM / BPB + INODE_NUM + 4)
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */

  struct inode *ino;
  char buf[BLOCK_SIZE];

  printf("\tim: alloc_inode type:%d\n", type);

  for (int i = 1; i <= INODE_NUM; i++) {
    bm->read_block(IBLOCK(i, bm->sb.nblocks), buf);
    ino = (struct inode*) buf;
    if (ino->type == 0) {
      ino->type = type;
      ino->size = 0;
      ino->ctime = ino->mtime = ino->atime = time(NULL);
      bm->write_block(IBLOCK(i, bm->sb.nblocks), buf);
      return i;
    }
  }

  return 0; //failed
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  struct inode *ino;
  char buf[BLOCK_SIZE];

  printf("\tim: free_inode inum:%d\n", inum);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino = (struct inode*) buf;

  ino->type = 0;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  // modified, "inum < 0" ==> "inum <= 0"
  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  
  struct inode *ino;
  char buf[BLOCK_SIZE];
  char buf_file[BLOCK_SIZE];
  char *file;
  int rest_size;
  blockid_t *blocks;

  printf("\tim: read_file inum:%d\n", inum);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino = (struct inode*) buf;
  ino->atime = time(NULL);
  bm->write_block(IBLOCK(inum. bm->sb.nblocks), buf);

  if (ino->size == 0)
    return;
  *size = ino->size;
  file = (char *) malloc(ino->size);
  *buf_out = file;

  printf("\tim.read : size = %d\n", ino->size);

  // direct blocks
  rest_size = ino->size;
  for (int i = 0; i < NDIRECT; i++) {
    if (rest_size > 0) {
      bm->read_block(ino->blocks[i], buf_file);
      memcpy(file + i * BLOCK_SIZE, buf_file, MIN(BLOCK_SIZE, rest_size));
#ifdef DEBUG
      printf("  im.read: %dth block, block num = %d\n  im.read: block content:", i, ino->blocks[i]);
      for (int j = 0; j < MIN(BLOCK_SIZE, rest_size); j++)
          printf("%c", buf_file[j]);
      printf("\n");
#endif
      rest_size -= BLOCK_SIZE;
    }
    else
      return;
  }

  // indirect block
  bm->read_block(ino->blocks[NDIRECT], buf);
  blocks = (blockid_t *) buf;
  for (uint i = 0; i < NINDIRECT; i++) {
    if (rest_size > 0) {
      bm->read_block(blocks[i], buf_file);
      memcpy(file + (NDIRECT + i) * BLOCK_SIZE, buf_file, MIN(BLOCK_SIZE, rest_size));
#ifdef DEBUG
      printf("  im.read: %dth block, block num = %d\n  im.read: block content:", i + NDIRECT, blocks[i]);
      for (int j = 0; j < MIN(BLOCK_SIZE, rest_size); j++)
          printf("%c", buf_file[j]);
      printf("\n");
#endif
      rest_size -= BLOCK_SIZE;
    }
    else
      return;
  }
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  
  struct inode *ino;
  char buf_ino[BLOCK_SIZE];
  char buf_file[BLOCK_SIZE];
  char buf_blk[BLOCK_SIZE];
  int org_blkcount, new_blkcount, blk;
  int rest_size;
  blockid_t *blocks;

  printf("\tim: write_file inum:%d size:%d\n", inum, size);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return;
  }

  if (size < 0 || size > MAXFILE * BLOCK_SIZE) {
    printf("\tim: size out of range\n");
    return;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf_ino);
  ino = (struct inode*) buf_ino;
  ino->mtime = time(NULL);
  if (size != ino->size)
      ino->ctime = ino->mtime;
  if (ino->size == 0)
    org_blkcount = -1;
  else
    org_blkcount = ((int)(ino->size) - 1) / BLOCK_SIZE;
  new_blkcount = (size - 1) / BLOCK_SIZE;

  printf("\torgsize: %u, newsize: %d, orgblk: %d, newblk: %d\n", ino->size, size, org_blkcount, new_blkcount);

  // direct blocks
  rest_size = size;
  for (blk = 0; blk < NDIRECT; blk++) {
    if (rest_size > 0) {
      memcpy(buf_file, buf + blk * BLOCK_SIZE, MIN(BLOCK_SIZE, rest_size));
      rest_size -= BLOCK_SIZE;
      // larger than original file
      if (blk > org_blkcount)
        ino->blocks[blk] = bm->alloc_block();
#ifdef DEBUG
      printf("  im.write: %dth block, block num = %d, rest size = %d\n  im.write: block content:", blk, ino->blocks[blk], rest_size);
      for (int j = 0; j < MIN(BLOCK_SIZE, rest_size + BLOCK_SIZE); j++)
          printf("%c", buf_file[j]);
      printf("\n");
#endif
      bm->write_block(ino->blocks[blk], buf_file);
    }
    // smaller than original file
    else if (blk <= org_blkcount)
      bm->free_block(ino->blocks[blk]);
  }

  // indirect block
  // allocate indirect block
  if (org_blkcount < NDIRECT && new_blkcount >= NDIRECT)
    ino->blocks[NDIRECT] = bm->alloc_block();
  bm->read_block(ino->blocks[NDIRECT], buf_blk);
  blocks = (blockid_t *) buf_blk;
  for (; blk < MAXFILE; blk++) {
    if (rest_size > 0) {
      memcpy(buf_file, buf + blk * BLOCK_SIZE, MIN(BLOCK_SIZE, rest_size));
      rest_size -= BLOCK_SIZE;
      // larget than original file
      if (blk > org_blkcount)
        blocks[blk - NDIRECT] = bm->alloc_block();
#ifdef DEBUG
      printf("  im.write: %dth block, block num = %d, rest size = %d\n  im.write: block content:", blk, blocks[blk - NDIRECT], rest_size);
      for (int j = 0; j < MIN(BLOCK_SIZE, rest_size + BLOCK_SIZE); j++)
          printf("%c", buf_file[j]);
      printf("\n");
#endif
      bm->write_block(blocks[blk - NDIRECT], buf_file);
    }
    // smaller than original file
    else if (blk <= org_blkcount)
      bm->free_block(blocks[blk - NDIRECT]);
  }
  // free indirect block
  if (new_blkcount < NDIRECT && org_blkcount >= NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);
  else
    bm->write_block(ino->blocks[NDIRECT], buf_blk);

  ino->size = size;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf_ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  
  struct inode *ino;
  char buf_ino[BLOCK_SIZE];

  printf("\tim: getattr %d\n", inum);
  
  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf_ino);
  ino = (struct inode*) buf_ino;
  a.type = ino->type;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  a.size = ino->size;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  struct inode *ino;
  char buf_ino[BLOCK_SIZE];
  char buf_blk[BLOCK_SIZE];
  uint32_t blk;
  int rest_size;
  blockid_t *blocks;

  printf("\tim: remove_file inum:%d\n", inum);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf_ino);
  ino = (struct inode*) buf_ino;

  // direct blocks
  rest_size = ino->size;
  for (blk = 0; blk < NDIRECT; blk++) {
    if (rest_size > 0) {
      bm->free_block(ino->blocks[blk]);
      rest_size -= BLOCK_SIZE;
    }
    else
      break;
  }

  // indirect block
  if (rest_size > 0) {
    bm->read_block(ino->blocks[NDIRECT], buf_blk);
    blocks = (blockid_t *) buf_blk;
    for (; blk < MAXFILE; blk++) {
      if (rest_size > 0) {
        bm->free_block(blocks[blk - NDIRECT]);
        rest_size -= BLOCK_SIZE;
      }
      else
        break;
    }
    // free indirect block
    bm->free_block(ino->blocks[NDIRECT]);
  }
  
  free_inode(inum);
}

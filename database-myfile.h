#ifndef DATABASE_MYFILE_HEADER
#define DATABASE_MYFILE_HEADER

#include "database.h"
#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <vector>
#include <memory>
#include <list>
#include "util/file_system.h"
#include <atomic>

#define MYSQL_BLOCK_TABLE_NUM 10

#define MAX_NODE 14 * 104 * 1024 // 1M, each map need 160M(x64 build)
#define MAX_CACHE MAX_NODE / 56
#define MAX_CACHE_LENGTH 20 * 1024 * 1024  // each map need 200M
#define MAX_DATA_LENGTH    65535

#pragma pack(1)

struct KeyNode
{
private:
  int32_t pos;
public:
  int16_t capacity;
  int16_t len;
  char    flag[2];
  int64_t getPos() const { return pos * 1024;  }
  void setPos(int64_t pos_) { assert(pos_ % 1024 == 0);  pos = pos_ / 1024; }
};

struct MyfileHeader
{
  int16_t version;
  int64_t sequence;
  int32_t count;
  KeyNode node[MAX_NODE]; //18M
};

struct NodeHeader
{
  uint32_t headsize;
  uint32_t crc;
  uint32_t index;
  uint64_t timestamp;
  uint32_t reserved;
};

#pragma pack()

struct CacheValue
{
  CacheValue() { refcount = 0; len = 0; data = 0; }
  int64_t refcount;
  int32_t len;
  char* data;
};

#define ROUND(x, mod) (((x) + (mod) - 1) / (mod) * (mod))

const int64_t VALUE_OFFSET = ROUND(sizeof(MyfileHeader), 1024);

enum KVCommandType
{
  KVCT_GET = 1,
  KVCT_SET,
  KVCT_LIST,
  KVCT_DELETE,
  KVCT_RESET,
  KVCT_DUMMY = 0xFF,
};

struct KvCommand
{
  KVCommandType commandType;
  int32_t mapId;
  int64_t seq;
  int64_t key;
  std::string val;
};

class MyFileFlushCallback
{
public:
  virtual int OnFlushed(const std::list<KvCommand>& commands) = 0;
};

//point (8 BYTE) -> uint32_t (4 BYTE), memory optimize
using CacheValueHandle = uint32_t;

class CacheValueAllocator
{
public:
  CacheValueAllocator();
  ~CacheValueAllocator();
  
  CacheValueHandle alloc();
  CacheValue* getValue(CacheValueHandle h);
  void free(CacheValueHandle h);

  static const CacheValueHandle INVALID_HANDLE = -1;
private:
  CacheValueHandle m_initHandle;
  CacheValue* m_alloced[MAX_CACHE];
  std::list<std::pair<CacheValueHandle, CacheValue*>> m_freelist;
};

struct MyfilePartition
{
public:
  MyfilePartition();
  ~MyfilePartition();
public:
  int Init(const std::string &savedir, const std::string &dbfile, int i, CacheMode cacheMode);
  int UnInit();

  bool saveBlock(int16_t x, int16_t y, int16_t z, const std::string &data, bool changed);
  std::string loadBlock(int16_t x, int16_t y, int16_t z, bool& bCacheHit);
  std::string __directLoadBlock(int16_t x, int16_t y, int16_t z, bool& changed);

  bool deleteBlock(int16_t x, int16_t y, int16_t z);
  bool listAllLoadableBlocks(std::vector<int64_t> &dst);

  int32_t getLocalIndex(int16_t x, int16_t y, int16_t z);
  int64_t getGlobalIndex(int32_t localindex);

  void flush();

  void GetCacheSummary(int32_t& cacheCount, int32_t& cacheMemoryBytes) { cacheCount = m_cacheNodeCount; cacheMemoryBytes = m_cacheMemoryByte; }

  bool GetModifyList(std::vector<int64_t>& v);
private:
  int cacheBlock(int32_t index, const std::string& value, bool rewrite_value, bool is_pread);

  int32_t AllocCacheIndex();

  std::string ProcessReadBuffer(int& readBytes, int& readPos, int index);
public:
  File* m_datafile;
  File* m_metafile;
  MyfileHeader* m_header;
  char* m_buffer; // 64K

  std::list<int32_t> m_accessCacheFIFO;
  std::list<int32_t> m_prereadCacheFIFO;
  CacheValueAllocator m_cacheAllocator;
  CacheValueHandle* m_node;   //2M * 4 = 8M
  bool m_metadataChanged;
#ifdef WIN32
  HANDLE m_hFileMapping;
#endif
  uv_mutex_t m_fileLock;
  uint32_t m_cacheNodeCount;

  uint32_t m_cacheMemoryByte;

  CacheMode m_cacheMode;
  int32_t m_index;
};

enum MyFileState
{
  MFS_NEEDSYNC,
  MFS_SYNCED,
};

class Database_Myfile : public Database
{
public:
  Database_Myfile(const std::string &savedir, const std::string &dbfile);
  virtual ~Database_Myfile();

  virtual int Init(CacheMode cacheMode);
  virtual int UnInit();

  virtual std::string loadBlock(int64_t pos);
  virtual bool listAllLoadableBlocks(std::vector<int64_t> &dst);
  void listAllLoadableBlocks(std::vector<v3s16> &dst) override;

  virtual bool checkflush();

  void SetFlushCallback(MyFileFlushCallback* callback) { m_callback = callback; }

  bool ProcessSetCommand(const KvCommand& command);
  bool ProcessDeleteCommand(const KvCommand& command);

  void setId(int32_t id) { m_configId = id; }
  int32_t getId() const  { return m_configId; }

  bool __directSaveBlock(int64_t pos, const std::string &data, bool changed);
  bool __directDeleteBlock(int64_t pos);
  std::string __directLoadBlock(int64_t pos, bool& changed);

  bool forceflush();

  int64_t getTotalLoadCount() const { return m_totalLoadCount; }
  int64_t getCache1HitCount() const { return m_cache1HitCount; }
  int64_t getCache2HitCount() const { return m_cache2HitCount; }

  int PrintHitRate();

  void GetCacheSummary(int32_t& cacheCount, int32_t& cacheMemoryBytes);

  MyFileState GetState() const { return m_state; }
  void SetState(MyFileState state) { m_state = state; }

  bool GetModifyList(std::vector<int64_t>& v);

  int64_t GetCreateTime() const { return m_createTime; }
    
  // 地图生成工具保存block接口
  bool saveBlock(const v3s16 &pos, const std::string &data) override;
    
  std::string loadBlock(const v3s16 &pos) override;
  bool deleteBlock(const v3s16 &pos) override;
    
private:
  bool saveBlock(int64_t pos, const std::string &data);
  bool deleteBlock(int64_t pos);
    
private:
  int getTableIndex(int64_t x);
private:
  std::string m_savedir;
  std::string m_dbfile;

  MyFileFlushCallback* m_callback;

  MyfilePartition m_stmt[MYSQL_BLOCK_TABLE_NUM];
  int m_wheelIndex;

  uv_timer_t m_timerWrite;
  uv_timer_t m_timerFlush;

  std::map<int64_t, KvCommand>   m_modifyCommands;
  std::map<int64_t, std::string> m_valueCache;
  uv_mutex_t m_cacheLock;

  std::list<KvCommand>    m_pendingWriteRequest[MYSQL_BLOCK_TABLE_NUM];
  uv_mutex_t m_flushLock;

  int32_t    m_configId;

  std::atomic<int64_t>    m_totalLoadCount;
  std::atomic<int64_t>    m_cache1HitCount;
  std::atomic<int64_t>    m_cache2HitCount;

  std::atomic<int64_t>    m_tpsCounterR;
  std::atomic<int64_t>    m_tpsCounterW;
  int64_t                 m_lastTpsResetTime;
  MyFileState             m_state;

  int64_t                 m_createTime;
};

#endif  //! #ifndef DATABASE_MYFILE_HEADER


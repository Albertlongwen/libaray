#include "database-myfile.h"
#include "easylogging++.h"
#include "boost/crc.hpp"
#include <thread>
#include <time.h>

#ifndef WIN32

# include <sys/mman.h>
# include <sys/stat.h>
# include <fcntl.h>

#endif

static void GetTimeSecond(int64_t* curTime)
{
    if (!curTime)
        return;
    
    std::chrono::time_point<std::chrono::high_resolution_clock> time_point;
    time_point = std::chrono::high_resolution_clock().now();
    int64_t t = time_point.time_since_epoch().count();
    t = t / std::chrono::high_resolution_clock::period::den;
    *curTime = t;
}

CacheValueAllocator::CacheValueAllocator()
{
  m_initHandle = 0;
  memset(m_alloced, 0, MAX_CACHE * sizeof(CacheValue*));
}

CacheValueAllocator::~CacheValueAllocator()
{
  for (int i = 0; i < MAX_CACHE; ++i)
    delete m_alloced[i];

  for (auto it = m_freelist.begin(); it != m_freelist.end(); ++it)
    delete it->second;
  m_freelist.clear();
}

CacheValueHandle CacheValueAllocator::alloc()
{
  CacheValueHandle h = INVALID_HANDLE;
  CacheValue* v = 0;
  if (!m_freelist.empty())
  {
    std::pair<CacheValueHandle, CacheValue*> pair = m_freelist.front();
    h = pair.first;
    v = pair.second;
    m_freelist.pop_front();
  }
  else if (m_initHandle < MAX_CACHE)
  {
    h = m_initHandle++;
    v = new CacheValue();
  }
  else
  {
    LOG(ERROR) << "CacheValue alloc fail!";
    return INVALID_HANDLE;
  }

  if (m_alloced[h] != 0)
  {
    LOG(ERROR) << "m_alloced[h] != 0! h: " << h;
    return INVALID_HANDLE;
  }

  m_alloced[h] = v;
  return h;
}

void CacheValueAllocator::free(CacheValueHandle h)
{
  if (h >= MAX_CACHE || m_alloced[h] == 0)
    return;

  std::pair<CacheValueHandle, CacheValue*> pair;
  pair.first = h;
  pair.second = m_alloced[h];
  assert(pair.second->refcount == 0);
  m_freelist.push_back(pair);
  m_alloced[h] = 0;
}

CacheValue* CacheValueAllocator::getValue(CacheValueHandle h)
{
  if (h >= MAX_CACHE)
    return nullptr;

  return m_alloced[h];
}

MyfilePartition::MyfilePartition()
{
  m_index = 0;
  m_cacheMode = CM_CACHE;
  m_cacheNodeCount = 0;
  m_cacheMemoryByte = 0;
  m_datafile = nullptr;
  m_metafile = nullptr;
  m_header = NULL;
  m_buffer = NULL;
  m_node = NULL;
  m_metadataChanged = false;
#ifdef WIN32
  m_hFileMapping = INVALID_HANDLE_VALUE;
#endif
  uv_mutex_init(&m_fileLock);
}

MyfilePartition::~MyfilePartition()
{
  uv_mutex_destroy(&m_fileLock);
}

int MyfilePartition::Init(const std::string &savedir, const std::string &dbfile, int i, CacheMode cacheMode)
{
  UnInit();

  if (!fs_system::PathExists(savedir) && !fs_system::CreateAllDirs(savedir))
    return -1;

  char filename[1024] = { 0 };
#ifdef WIN32
  sprintf_s(filename, 1024, dbfile.c_str(), i);
  std::string dbp = savedir + DIR_DELIM + filename;
  m_datafile = new File(dbp, GENERIC_WRITE | GENERIC_READ);
  std::string dbpmeta = savedir + DIR_DELIM + filename + "meta";
  m_metafile = new File(dbpmeta, GENERIC_WRITE | GENERIC_READ);
#else
  snprintf(filename, 1024, dbfile.c_str(), i);
  std::string dbp = savedir + DIR_DELIM + filename;
  std::string dbpmeta = savedir + DIR_DELIM + filename + "meta";
  m_datafile = new File(dbp, O_RDWR);
  m_metafile = new File(dbpmeta, O_RDWR);
#endif

  if (!m_datafile->IsValid())
    return -1;

  if (!m_metafile->IsValid())
    return -1;

  bool isNewMetaFile = m_metafile->GetLength() == 0;

#ifdef WIN32
  m_hFileMapping = CreateFileMapping(m_metafile->file_, NULL, PAGE_READWRITE,
    0, VALUE_OFFSET, NULL);
  if (m_hFileMapping == INVALID_HANDLE_VALUE)
  {
    LOG(ERROR) << "Unable to mmap file: " << dbfile;
    return -1;
  }

  m_header = (MyfileHeader*)MapViewOfFile(m_hFileMapping, FILE_MAP_ALL_ACCESS, 0, 0, 0);
  if (!m_header)
  {
    LOG(ERROR) << "Unable to mmap file: " << dbfile;
    return -1;
  }
#else
  if (isNewMetaFile)
  {
    char* buff = (char*)malloc(VALUE_OFFSET);
    memset(buff, 0, VALUE_OFFSET);
    m_metafile->Write(0, buff, VALUE_OFFSET);
    free(buff);
  }
  m_header = (MyfileHeader*)mmap(0, VALUE_OFFSET, PROT_READ | PROT_WRITE, MAP_SHARED, m_metafile->file_, 0);
  if (m_header == (MyfileHeader*)-1)
  { 
    LOG(ERROR) << "Unable to mmap file: " << dbfile;
    return -1;
  }
#endif

  if (isNewMetaFile)
  {
    LOG(ERROR) << "NewMetaFile: " << dbfile;
    memset(m_header, 0, VALUE_OFFSET);
    m_header->version = 1;
  }
  else if (m_header->version != 1)
  {
    return -1;
  }

  m_buffer = new char[MAX_DATA_LENGTH];

  if (cacheMode == CM_CACHE)
  {
    m_node = (CacheValueHandle*)malloc(MAX_NODE * sizeof(CacheValueHandle));
    memset(m_node, CacheValueAllocator::INVALID_HANDLE, MAX_NODE * sizeof(CacheValueHandle));
  }
  else
  {
    m_node = nullptr;
  }

  m_cacheMode = cacheMode;

  m_index = i;
  return 0;
}

int MyfilePartition::UnInit()
{
  delete[] m_buffer;
  m_buffer = NULL;

  for (int i = 0; m_node && i < MAX_NODE; ++i)
  {
    if (m_node[i] != CacheValueAllocator::INVALID_HANDLE)
      m_cacheAllocator.free(m_node[i]);
  }
  m_accessCacheFIFO.clear();
  m_prereadCacheFIFO.clear();

  free(m_node);
  m_node = NULL;

#ifdef WIN32
  if (m_header)
  {
    BOOL b = FlushViewOfFile(m_header, VALUE_OFFSET);
    if (!b)
      LOG(ERROR) << "FlushViewOfFile error!";
    UnmapViewOfFile(m_header);
  }

  if (m_hFileMapping != INVALID_HANDLE_VALUE)
    CloseHandle(m_hFileMapping);
  m_hFileMapping = INVALID_HANDLE_VALUE;
#else
  if (m_header)
  {
    msync((void *)m_header, VALUE_OFFSET, MS_SYNC);
    munmap(m_header, VALUE_OFFSET);
  }
    
#endif
  m_header = NULL;

  if (m_datafile)
    m_datafile->Flush(false);
  delete m_datafile;
  m_datafile = nullptr;

  delete m_metafile;
  m_metafile = nullptr;

  return 0;
}

bool MyfilePartition::saveBlock(int16_t x, int16_t y, int16_t z, const std::string &data, bool changed)
{
  int32_t index = getLocalIndex(x, y, z);
  int64_t globalIndex = Database::getBlockAsInteger(x, y, z);
  if (index < 0 || index >= MAX_NODE)
  {
    LOG(ERROR) << "saveBlock invalid x:" << x << " y: " << y << " z: " << z << " index:" << index;
    return true;
  }

  int len = data.length() + sizeof(NodeHeader);
  int capacity = ROUND(len, 1024);
  if (capacity >= MAX_DATA_LENGTH)
  {
    LOG(ERROR) << "saveBlock data too large: " << capacity << "index: " << globalIndex;
    return true;
  }

  uv_mutex_lock(&m_fileLock);

  memset(m_buffer, 0, capacity);
  NodeHeader* header = (NodeHeader*)m_buffer;
  header->headsize = sizeof(NodeHeader);
  header->index = index;
  if (!data.empty())
  {
    boost::crc_32_type crc32;
    crc32.process_bytes(data.c_str(), data.length());
    header->crc = crc32();
  }
  else
  {
    header->crc = 0;
  }
  time_t rawtime;
  time(&rawtime);
  header->timestamp = (uint64_t)rawtime;
  header->reserved = 0xCDCDCDCD;
  memcpy(m_buffer + sizeof(NodeHeader), data.c_str(), data.length());

  KeyNode& node = m_header->node[index];
  if (node.len == 0 && len != 0)
    ++m_header->count;

  node.len = len;
  node.flag[0] = changed ? 1 : 0;
  node.flag[1] = 0;
  bool ret = false;
  if (node.capacity >= capacity && m_cacheMode != CM_APPEND)
  {
    m_datafile->Seek(File::FROM_BEGIN, node.getPos());
    ret = (m_datafile->Write(node.getPos(), m_buffer, len) == len);
  }
  else
  {
    node.capacity = capacity;
    int64_t pos = m_datafile->Seek(File::FROM_END, 0);
    if (pos % 1024 != 0)
      LOG(ERROR) << "saveBlock data pos % 1024 != 0, index: " << globalIndex;
    node.setPos(pos);
    ret = (m_datafile->Write(node.getPos(), m_buffer, capacity) == capacity);
    m_metadataChanged = true;
  }

  if (ret)
  {
    cacheBlock(index, data, true, false);
    m_datafile->TryFlush(node.getPos(), capacity);
  }
  else
  {
    LOG(ERROR) << "saveBlock write fail! index: " << globalIndex;
  }

  uv_mutex_unlock(&m_fileLock);

  return ret;
}

int32_t MyfilePartition::getLocalIndex(int16_t x, int16_t y, int16_t z)
{ 
  if (x < 0 || z < 0)
  {
    return -1;
  }

  int16_t local_x = x / MYSQL_BLOCK_TABLE_NUM;
  int16_t local_y = y + 14;
  int16_t local_z = z;
  if (local_z < 0 || local_z >= 1024)
  {
    //LOG(ERROR) << "invalid local_z:" << z;
    return -1;
  }
  if (local_y < 0 || local_y >= 23)
  {
    //LOG(ERROR) << "invalid local_y:" << local_y;
    return -1;
  }
  if (local_x < 0 || local_x >= 64)
  {
    //LOG(ERROR) << "invalid local_x:" << local_x;
    return -1;
  }
  return local_z + (local_x << 10) + (local_y << 16);
}

int64_t MyfilePartition::getGlobalIndex(int32_t localindex)
{
  int16_t z = localindex & 1023;
  localindex = localindex >> 10;
  int16_t x = localindex & 63;
  localindex = localindex >> 6;
  int16_t y = localindex;
  return Database::getBlockAsInteger(x * MYSQL_BLOCK_TABLE_NUM + m_index, y - 14, z);
}

void MyfilePartition::flush()
{
  uv_mutex_lock(&m_fileLock);
  bool onlyData = !m_metadataChanged;
  m_metadataChanged = false;
  uv_mutex_unlock(&m_fileLock);
  if (m_datafile)
    m_datafile->Flush(onlyData);

#ifdef WIN32
  if (m_header)
  {
    BOOL b = FlushViewOfFile(m_header, VALUE_OFFSET); 
    if (!b)
      LOG(ERROR) << "FlushViewOfFile error!";
  }
#else
  if (m_header)
    msync((void *)m_header, VALUE_OFFSET, MS_SYNC);
#endif  //! #ifdef WIN32
}

#define CHECK_DELETE(p) \
CacheValue* cache = m_cacheAllocator.getValue(p);\
if (cache && --(cache)->refcount == 0) \
{ \
  m_cacheMemoryByte -= cache->len;\
  cache->len = 0;\
  free(cache->data);\
  cache->data = 0;\
  m_cacheAllocator.free(p); \
  (p) = CacheValueAllocator::INVALID_HANDLE; \
  --m_cacheNodeCount; \
}

int32_t MyfilePartition::AllocCacheIndex()
{
  int32_t index = -1;
  if (m_prereadCacheFIFO.size() > 1024)
  {
    index = m_prereadCacheFIFO.front();
    m_prereadCacheFIFO.pop_front();
    return index;
  }

  if (!m_accessCacheFIFO.empty())
  {
    index = m_accessCacheFIFO.front();
    m_accessCacheFIFO.pop_front();
  }
  return index;
}

int MyfilePartition::cacheBlock(int32_t index, const std::string& value, bool rewrite_value, bool is_pread)
{
  if (!m_node || m_cacheMode != CM_CACHE)
    return -1;

  while (m_cacheNodeCount >= MAX_CACHE || m_cacheMemoryByte >= MAX_CACHE_LENGTH)
  {
    int32_t newIndex = AllocCacheIndex();
    CHECK_DELETE(m_node[newIndex]);
  }

  if (!rewrite_value && m_node[index] == CacheValueAllocator::INVALID_HANDLE)
  {
    //LOG(ERROR) << "myself poped when not rewrite, force rewrite_value = true, index." << index;
    rewrite_value = true;
  }

  CacheValueHandle h = 0;
  if (m_node[index] == CacheValueAllocator::INVALID_HANDLE)
  {
    h = m_cacheAllocator.alloc();
    ++m_cacheNodeCount;
    m_node[index] = h;
  }
  else if (is_pread)
  {
    //LOG(ERROR) << "pread node alread in cache, index." << index;
    return -1;
  }
  else
  {
    h = m_node[index];
  }

  CacheValue* cacheV = m_cacheAllocator.getValue(h);
  if (!cacheV)
    return -1;

  if (rewrite_value)
  {
    m_cacheMemoryByte -= cacheV->len;
    if (cacheV->data)
      free(cacheV->data);
    cacheV->data = (char*)malloc(value.length());
    memcpy(cacheV->data, value.c_str(), value.length());
    cacheV->len = value.length();
    m_cacheMemoryByte += cacheV->len;
  }

  if (cacheV->refcount < 3)
  {
    if (is_pread)
      m_prereadCacheFIFO.push_back(index);
    else
      m_accessCacheFIFO.push_back(index);
    ++cacheV->refcount;
  }
  
  return 0;
}

std::string MyfilePartition::loadBlock(int16_t x, int16_t y, int16_t z, bool& bCacheHit)
{
  int32_t index = getLocalIndex(x, y, z);
  if (index < 0 || index >= MAX_NODE)
  {
    //LOG(ERROR) << "loadBlock invalid x:" << x << "y : " << y << "z : " << z;
    bCacheHit = true;
    return "";
  }

  uv_mutex_lock(&m_fileLock);
  KeyNode& node = m_header->node[index];
  if (node.len == 0)  // not exist
  {
    bCacheHit = true;
    uv_mutex_unlock(&m_fileLock);
    return "";
  }

  if (m_node && m_node[index] != CacheValueAllocator::INVALID_HANDLE)  // read from cache
  {
    bCacheHit = true;
    CacheValue* cache = m_cacheAllocator.getValue(m_node[index]);
    std::string val(cache->data, cache->len);
    cacheBlock(index, val, false, false);
    uv_mutex_unlock(&m_fileLock);
    return val;
  }

  bCacheHit = false;
  int readBytes = m_datafile->Read(node.getPos(), m_buffer, ROUND(node.capacity, 4096 * 2));
  int readPos = 0;
  std::string ret = ProcessReadBuffer(readBytes, readPos, index);
  uv_mutex_unlock(&m_fileLock);
  return ret;
}

std::string MyfilePartition::__directLoadBlock(int16_t x, int16_t y, int16_t z, bool& changed)
{
  bool bCacheHit = false;
  std::string data = loadBlock(x, y, z, bCacheHit);
  if (!data.empty())
  {
    int32_t index = getLocalIndex(x, y, z);
    changed = m_header->node[index].flag[0] == 0;
  }
  else
  {
    changed = false;
  }
  return data;
}

std::string MyfilePartition::ProcessReadBuffer(int& readBytes, int& readPos, int index)
{
  std::string ret = "ERROR";

  if (readBytes < sizeof(NodeHeader))
    return ret;

  NodeHeader* header = (NodeHeader*)(m_buffer + readPos);
  uint32_t saveIndex = 0;
  uint32_t saveCrc = 0;
  uint32_t headSize = 0;
  if (header->headsize == sizeof(NodeHeader) && sizeof(NodeHeader) == 24)
  {
    saveIndex = header->index;
    saveCrc = header->crc;
    headSize = sizeof(NodeHeader);
  }
  else
  {
    if (readPos == 0)
      LOG(ERROR) << "headsize: " << header->headsize << " not match!";
    return ret;
  }

  if (index != saveIndex)
  {
    if (readPos == 0)
      LOG(ERROR) << "index: " << index << " not match!";
    return ret;
  }

  if (readBytes < m_header->node[index].capacity)
  {
    if (readPos == 0)
      LOG(ERROR) << "index: " << index << " need capacity:" << m_header->node[index].capacity;
    return ret;
  }

  std::string data = std::string(m_buffer + readPos + headSize, m_header->node[index].len - headSize);
  uint32_t crc = 0;
  if (!data.empty())
  {
    boost::crc_32_type crc32;
    crc32.process_bytes(data.c_str(), data.length());
    crc = crc32();
  }

  if (saveCrc != crc)
  {
    if (readPos == 0)
      LOG(ERROR) << "index: " << index << " crc failed!" << "datalen: " << data.length() << "oldcrc: " << saveCrc << "newcrc: " << crc;
    return ret;
  }

  //LOG(ERROR) << "precache index: " << index;
  cacheBlock(index, data, true, readPos != 0);

  ret = data;
  readBytes -= m_header->node[index].capacity;
  readPos += m_header->node[index].capacity;

  ProcessReadBuffer(readBytes, readPos, index + 1);
  return ret;
}

bool MyfilePartition::deleteBlock(int16_t x, int16_t y, int16_t z)
{
  int32_t index = getLocalIndex(x, y, z);
  if (index < 0 || index >= MAX_NODE)
  {
    //LOG(ERROR) << "loadBlock invalid x:" << x << "y : " << y << "z : " << z;
    return "";
  }

  uv_mutex_lock(&m_fileLock);
  KeyNode& node = m_header->node[index];
  if (node.len != 0)
    --m_header->count;
  node.len = 0;
  uv_mutex_unlock(&m_fileLock);
  return true;
}

bool MyfilePartition::GetModifyList(std::vector<int64_t>& v)
{
  for (int i = 0; i < MAX_NODE; ++i)
  {
    KeyNode& node = m_header->node[i];
    if (node.flag[0] != 0)
      v.push_back(getGlobalIndex(i));
  }
  return true;
}

bool MyfilePartition::listAllLoadableBlocks(std::vector<int64_t> &dst)
{
  return true;
}

Database_Myfile::Database_Myfile(const std::string &savedir, const std::string &dbfile)
{
  GetTimeSecond(&m_createTime);

  m_state = MFS_NEEDSYNC;

  m_tpsCounterR = 0;
  m_tpsCounterW = 0;
  m_lastTpsResetTime = 0;
  m_totalLoadCount = 0;
  m_cache1HitCount = 0;
  m_cache2HitCount = 0;
  m_configId = -1;
  m_callback = nullptr;
  m_savedir = savedir;
  m_dbfile = dbfile;

  m_wheelIndex = 0;
  uv_mutex_init(&m_cacheLock);
  uv_mutex_init(&m_flushLock);
}

Database_Myfile::~Database_Myfile()
{
  UnInit();
  uv_mutex_destroy(&m_cacheLock);
  uv_mutex_destroy(&m_flushLock);
}

int Database_Myfile::Init(CacheMode cacheMode)
{
  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
  {
    if (m_stmt[i].Init(m_savedir, m_dbfile, i, cacheMode) < 0)
      return -1;
  }

  m_wheelIndex = 0;

  return 0;
}

int Database_Myfile::UnInit()
{

  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
    m_stmt[i].UnInit();

  m_wheelIndex = 0;
  return 0;
}

int Database_Myfile::getTableIndex(int64_t x)
{
  return abs(x % MYSQL_BLOCK_TABLE_NUM);
}

bool Database_Myfile::checkflush()
{
  uv_mutex_lock(&m_cacheLock);
  bool bCacheEmpty = m_modifyCommands.empty();
  uv_mutex_unlock(&m_cacheLock);
  if (!bCacheEmpty)
    return false;

  uv_mutex_lock(&m_flushLock);
  bool bFlushed = true;
  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
    bFlushed &= (m_pendingWriteRequest[i].empty());
  uv_mutex_unlock(&m_flushLock);

  return bFlushed;
}

bool Database_Myfile::ProcessSetCommand(const KvCommand& command)
{
  ++m_tpsCounterW;
  uv_mutex_lock(&m_cacheLock);
  m_modifyCommands[command.seq] = command;
  m_valueCache[command.key] = command.val;
  uv_mutex_unlock(&m_cacheLock);
  return true;
}

bool Database_Myfile::ProcessDeleteCommand(const KvCommand& command)
{
  ++m_tpsCounterW;
  uv_mutex_lock(&m_cacheLock);
  m_modifyCommands[command.seq] = command;
  m_valueCache[command.key] = "";
  uv_mutex_unlock(&m_cacheLock);
  return true;
}

bool Database_Myfile::__directSaveBlock(int64_t pos, const std::string &data, bool changed)
{
  int16_t x, y, z;
  Database::getIntegerAsBlock(pos, x, y, z);
  int index = getTableIndex(x);
  m_stmt[index].saveBlock(x, y, z, data, changed);

  return true;
}

bool Database_Myfile::__directDeleteBlock(int64_t pos)
{
  int16_t x, y, z;
  Database::getIntegerAsBlock(pos, x, y, z);
  int index = getTableIndex(x);
  m_stmt[index].deleteBlock(x, y, z);

  return true;
}

std::string Database_Myfile::__directLoadBlock(int64_t pos, bool& changed)
{
  int16_t x, y, z;
  Database::getIntegerAsBlock(pos, x, y, z);
  int index = getTableIndex(x);
  return m_stmt[index].loadBlock(x, y, z, changed);
}

bool Database_Myfile::forceflush()
{
  uv_mutex_lock(&m_cacheLock);
  std::map<int64_t, KvCommand> copyCommands = m_modifyCommands;
  uv_mutex_unlock(&m_cacheLock);

  size_t try_count = 0;
  const static size_t max_try_count = 100;
  while (!copyCommands.empty() && try_count < max_try_count)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ++try_count;
    uv_mutex_lock(&m_cacheLock);
    copyCommands = m_modifyCommands;
    uv_mutex_unlock(&m_cacheLock);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  if (try_count == max_try_count)
    LOG(ERROR) << "forceflush while m_modifyCommands not clean! count: " << copyCommands.size();

  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM;  ++i)
    m_stmt[i].flush();

  return true;
}

void Database_Myfile::GetCacheSummary(int32_t& cacheCount, int32_t& cacheMemoryBytes)
{
  cacheCount = cacheMemoryBytes = 0;
  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
  {
    int32_t subCacheCount = 0;
    int32_t subCacheMemoryBytes = 0;
    m_stmt[i].GetCacheSummary(subCacheCount, subCacheMemoryBytes);
    cacheCount += subCacheCount;
    cacheMemoryBytes += subCacheMemoryBytes;
  }
}

bool Database_Myfile::GetModifyList(std::vector<int64_t>& v)
{
  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
    m_stmt[i].GetModifyList(v);

  return true;
}

int Database_Myfile::PrintHitRate()
{
  int64_t curTime = 0;
  GetTimeSecond(&curTime);
  if (curTime - m_lastTpsResetTime < 30)
    return 0;

  float hitRatio = 0;
  if (m_totalLoadCount != 0)
    hitRatio = (m_cache1HitCount + m_cache2HitCount) * 100.f / m_totalLoadCount;

  int32_t cacheCount = 0;
  int32_t cacheMemoryBytes = 0;
  GetCacheSummary(cacheCount, cacheMemoryBytes);

  time_t tt = time(NULL);
  tm* t = localtime(&tt);
  char buffer[64] = { 0 };
#ifdef WIN32
    sprintf_s(buffer, 64, "%d-%02d-%02d %02d:%02d:%02d", t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
#else
    snprintf(buffer, 64, "%d-%02d-%02d %02d:%02d:%02d", t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec);
#endif

  std::cout << "------------------------------------------------------------" << std::endl;
  std::cout << "currentTime:" << buffer << std::endl;
  std::cout << "m_configId: " << m_configId << " tpsW: " << m_tpsCounterW / (curTime - m_lastTpsResetTime) << "/s"  << " tpsR: " << m_tpsCounterR / (curTime - m_lastTpsResetTime) << " / s"<< std::endl;
  std::cout << "------------------------------------------------------------" << std::endl;
  std::cout << "m_totalLoadCount: " << m_totalLoadCount << std::endl;
  std::cout << "m_cache1HitCount: " << m_cache1HitCount << std::endl;
  std::cout << "m_cache2HitCount: " << m_cache2HitCount << std::endl;
  std::cout << "hitRatio: " << hitRatio << "%" << std::endl;
  std::cout << "cacheCount: " << cacheCount << " cacheMemory: " << cacheMemoryBytes / 1024 / 1024 << "M" << std::endl;
  std::cout << "------------------------------------------------------------" << std::endl;
  
  m_tpsCounterR = 0;
  m_tpsCounterW = 0;
  m_lastTpsResetTime = curTime;
  return 0;
}

std::string Database_Myfile::loadBlock(const v3s16 &pos) {
    return loadBlock(getBlockAsInteger(pos));
}

std::string Database_Myfile::loadBlock(int64_t pos)
{
  ++m_tpsCounterR;
  ++m_totalLoadCount;
  bool cacheHit = false;
  std::string ret;
  if (!m_valueCache.empty())  // double check
  {
    uv_mutex_lock(&m_cacheLock);
    auto it = m_valueCache.find(pos);
    cacheHit = (it != m_valueCache.end());
    if (cacheHit)
      ret=  it->second;
    uv_mutex_unlock(&m_cacheLock);
  }

  if (cacheHit)
  {
    ++m_cache1HitCount;
    return ret;
  }

  int16_t x, y, z;
  Database::getIntegerAsBlock(pos, x, y, z);
  int index = getTableIndex(x);

  ret = m_stmt[index].loadBlock(x, y, z, cacheHit);
  if (cacheHit)
    ++m_cache2HitCount;

  return ret;
}

bool Database_Myfile::listAllLoadableBlocks(std::vector<int64_t> &dst)
{
  for (int i = 0; i < MYSQL_BLOCK_TABLE_NUM; ++i)
  {
    std::vector<int64_t> dst_child;
    if (!m_stmt[i].listAllLoadableBlocks(dst_child))
      return false;
    std::copy(dst_child.begin(), dst_child.end(), std::back_inserter(dst));
  }

  return true;
}

void Database_Myfile::listAllLoadableBlocks(std::vector<v3s16> &dst)
{
    std::vector<int64_t> param;
    for (auto block : dst) {
        s64 nblock = getBlockAsInteger(block);
        param.push_back(nblock);
    }
   
    listAllLoadableBlocks(param);
}

bool Database_Myfile::saveBlock(const v3s16 &pos, const std::string &data)
{
    int64_t nPos = Database::getBlockAsInteger(pos);
    return __directSaveBlock(nPos, data, false);
}

bool Database_Myfile::saveBlock(int64_t pos, const std::string &data)
{
  return __directSaveBlock(pos, data, true);
}

bool Database_Myfile::deleteBlock(const v3s16 &pos) {
    return deleteBlock(getBlockAsInteger(pos));
}

bool Database_Myfile::deleteBlock(int64_t pos)
{
  return __directDeleteBlock(pos);
}

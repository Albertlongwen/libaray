#include "file_system.h"
#ifdef _WIN32 // WINDOWS
    #include <windows.h>
#else // POSIX
    #include <sys/types.h>
    #include <dirent.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #include <unistd.h>
#endif

namespace fs_system
{

#ifdef _WIN32 // WINDOWS

#include <windows.h>

bool CreateDir(std::string path)
{
  bool r = CreateDirectory(path.c_str(), NULL) == TRUE;
  if (r == true)
    return true;
  if (GetLastError() == ERROR_ALREADY_EXISTS)
    return true;
  return false;
}

bool PathExists(std::string path)
{
  return (GetFileAttributes(path.c_str()) != INVALID_FILE_ATTRIBUTES);
}

bool IsDir(std::string path)
{
  DWORD attr = GetFileAttributes(path.c_str());
  return (attr != INVALID_FILE_ATTRIBUTES &&
    (attr & FILE_ATTRIBUTE_DIRECTORY));
}

bool IsDirDelimiter(char c)
{
  return c == '/' || c == '\\';
}

bool DeleteEmptyDirectory(std::string path)
{
  DWORD attr = GetFileAttributes(path.c_str());
  bool is_directory = (attr != INVALID_FILE_ATTRIBUTES &&
    (attr & FILE_ATTRIBUTE_DIRECTORY));
  if (!is_directory)
    return false;

  bool did = RemoveDirectory(path.c_str()) == TRUE;
  return did;
}

#else // POSIX

bool CreateDir(std::string path)
{
  int r = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  if (r == 0)
    return true;

  // If already exists, return true
  if (errno == EEXIST)
    return true;
  return false;
}

bool PathExists(std::string path)
{
  struct stat st;
  return (stat(path.c_str(), &st) == 0);
}

bool IsDir(std::string path)
{
  struct stat statbuf;
  if (stat(path.c_str(), &statbuf))
    return false; // Actually error; but certainly not a directory
  return ((statbuf.st_mode & S_IFDIR) == S_IFDIR);
}

bool IsDirDelimiter(char c)
{
  return c == '/';
}

bool DeleteEmptyDirectory(std::string path)
{
  if (!IsDir(path))
    return false;

  bool did = (rmdir(path.c_str()) == 0);
  if (!did)
    LOG(ERROR) << "rmdir errno: " << errno << ": " << strerror(errno);
  return did;
}

#endif  // // POSIX

std::string RemoveLastPathComponent(std::string path,
  std::string *removed, int count)
{
  if (removed)
    *removed = "";

  size_t remaining = path.size();

  for (int i = 0; i < count; ++i){
    // strip a dir delimiter
    while (remaining != 0 && IsDirDelimiter(path[remaining - 1]))
      remaining--;
    // strip a path component
    size_t component_end = remaining;
    while (remaining != 0 && !IsDirDelimiter(path[remaining - 1]))
      remaining--;
    size_t component_start = remaining;
    // strip a dir delimiter
    while (remaining != 0 && IsDirDelimiter(path[remaining - 1]))
      remaining--;
    if (removed){
      std::string component = path.substr(component_start,
        component_end - component_start);
      if (i)
        *removed = component + DIR_DELIM + *removed;
      else
        *removed = component;
    }
  }
  return path.substr(0, remaining);
}

std::string RemoveRelativePathComponents(std::string path)
{
  size_t pos = path.size();
  size_t dotdot_count = 0;
  while (pos != 0){
    size_t component_with_delim_end = pos;
    // skip a dir delimiter
    while (pos != 0 && IsDirDelimiter(path[pos - 1]))
      pos--;
    // strip a path component
    size_t component_end = pos;
    while (pos != 0 && !IsDirDelimiter(path[pos - 1]))
      pos--;
    size_t component_start = pos;

    std::string component = path.substr(component_start,
      component_end - component_start);
    bool remove_this_component = false;
    if (component == "."){
      remove_this_component = true;
    }
    else if (component == ".."){
      remove_this_component = true;
      dotdot_count += 1;
    }
    else if (dotdot_count != 0){
      remove_this_component = true;
      dotdot_count -= 1;
    }

    if (remove_this_component){
      while (pos != 0 && IsDirDelimiter(path[pos - 1]))
        pos--;
      path = path.substr(0, pos) + DIR_DELIM +
        path.substr(component_with_delim_end,
        std::string::npos);
      pos++;
    }
  }

  if (dotdot_count > 0)
    return "";

  // remove trailing dir delimiters
  pos = path.size();
  while (pos != 0 && IsDirDelimiter(path[pos - 1]))
    pos--;
  return path.substr(0, pos);
}

bool CreateAllDirs(std::string path)
{
  std::vector<std::string> tocreate;
  std::string basepath = path;
  while (!PathExists(basepath))
  {
    tocreate.push_back(basepath);
    basepath = RemoveLastPathComponent(basepath);
    if (basepath.empty())
      break;
  }
  for (int i = tocreate.size() - 1; i >= 0; i--)
    if (!CreateDir(tocreate[i]))
      return false;
  return true;
}

} // namespace fs

File::~File()
{
  Close();
}

#ifdef WIN32

File::File(const std::string& name, uint32_t flags)
{
  file_ = CreateFile(name.c_str(), flags, 0, NULL,
    OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
}

static int Read_(HANDLE file, int64_t offset, char* data, int size)
{
  if (size < 0)
    return -1;

  LARGE_INTEGER offset_li;
  offset_li.QuadPart = offset;

  OVERLAPPED overlapped = { 0 };
  overlapped.Offset = offset_li.LowPart;
  overlapped.OffsetHigh = offset_li.HighPart;

  DWORD this_read = 0;
  if (::ReadFile(file, data, size, &this_read, &overlapped) == FALSE)
    return 0;

  return this_read;
}

int Write_(HANDLE file, int64_t offset, const char* data, int size)
{
  LARGE_INTEGER offset_li;
  offset_li.QuadPart = offset;

  OVERLAPPED overlapped = { 0 };
  overlapped.Offset = offset_li.LowPart;
  overlapped.OffsetHigh = offset_li.HighPart;

  DWORD this_written = 0;
  if (::WriteFile(file, data, size, &this_written, &overlapped) == FALSE)
    return 0;

  return this_written;
}

int File::Read(int64_t offset, char* data, int size)
{
  int bytes_read = 0;
  do
  {
    int this_read = Read_(file_, offset + bytes_read, data + bytes_read, size - bytes_read);
    if (this_read <= 0)
      break;
    bytes_read += this_read;
  } while (bytes_read < size);

  return bytes_read;
}

int File::Write(int64_t offset, const char* data, int size) 
{
  int bytes_written = 0;
  do
  {
    int this_written = Write_(file_, offset + bytes_written, data + bytes_written, size - bytes_written);
    if (this_written <= 0)
      break;
    bytes_written += this_written;
  } while (bytes_written < size);

  return bytes_written;
}

bool File::IsValid() const
{
  return file_ != INVALID_HANDLE_VALUE;
}

bool File::Flush(bool onlyData)
{
  return ::FlushFileBuffers(file_) != FALSE;
}

bool File::TryFlush(int64_t offset, int64_t size)
{
  return true;
}

int64_t File::GetLength()
{
  LARGE_INTEGER size;
  if (!::GetFileSizeEx(file_, &size))
    return -1;

  return static_cast<int64_t>(size.QuadPart);
}

void File::Close()
{
  CloseHandle(file_);
}

int64_t File::Seek(Whence whence, int64_t offset)
{
  if (offset < 0)
    return -1;

  LARGE_INTEGER distance, res;
  distance.QuadPart = offset;
  DWORD move_method = static_cast<DWORD>(whence);
  if (!SetFilePointerEx(file_, distance, &res, move_method))
    return -1;

  return res.QuadPart;
}

#else

#define HANDLE_EINTR(x) ({ \
  __typeof__(x) eintr_wrapper_result; \
  do { \
    eintr_wrapper_result = (x); \
      } while (eintr_wrapper_result == -1 && errno == EINTR); \
  eintr_wrapper_result; \
})

#define IGNORE_EINTR(x) ({ \
  __typeof__(x) eintr_wrapper_result; \
  do { \
    eintr_wrapper_result = (x); \
    if (eintr_wrapper_result == -1 && errno == EINTR) { \
      eintr_wrapper_result = 0; \
            } \
      } while (0); \
  eintr_wrapper_result; \
})

File::File(const std::string& name, uint32_t flags)
{
  file_ = open(name.c_str(), flags | O_CREAT, S_IRUSR | S_IWUSR);
}

int File::Read(int64_t offset, char* data, int size)
{
  if (size < 0)
    return -1;

  int bytes_read = 0;
  int rv;
  do
  {
    rv = HANDLE_EINTR(pread(file_, data + bytes_read,
      size - bytes_read, offset + bytes_read));
    if (rv <= 0)
      break;

    bytes_read += rv;
  } while (bytes_read < size);

  return bytes_read ? bytes_read : rv;
}

int File::Write(int64_t offset, const char* data, int size)
{
  if (size < 0)
    return -1;

  int bytes_written = 0;
  int rv;
  do 
  {
    rv = HANDLE_EINTR(pwrite(file_, data + bytes_written,
      size - bytes_written, offset + bytes_written));
    if (rv <= 0)
      break;

    bytes_written += rv;
  } while (bytes_written < size);

  return bytes_written ? bytes_written : rv;
}

typedef struct stat64 stat_wrapper_t;
static int CallFstat(int fd, stat_wrapper_t *sb)
{
  return fstat64(fd, sb);
}

int64_t File::GetLength()
{
  stat_wrapper_t file_info;
  if (CallFstat(file_, &file_info))
    return false;

  return file_info.st_size;
}

bool File::Flush(bool onlyData)
{
  {HANDLE_EINTR(sync_file_range(file_, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER));}
  if (onlyData) { 
    return HANDLE_EINTR(fdatasync(file_));
  }
  return HANDLE_EINTR(fsync(file_));
}

bool File::TryFlush(int64_t offset, int64_t size)
{
  return HANDLE_EINTR(sync_file_range(file_, offset, size, SYNC_FILE_RANGE_WRITE));
}

bool File::IsValid() const
{
  return file_ != -1;
}

void File::Close()
{
  close(file_);
}

int64_t File::Seek(Whence whence, int64_t offset) {
  if (file_ < 0 || offset < 0)
    return -1;

  return lseek(file_, static_cast<off_t>(offset), static_cast<int>(whence));
}

#endif

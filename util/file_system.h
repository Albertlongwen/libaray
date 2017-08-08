#ifndef _FILE_SYSTEM_H_
#define _FILE_SYSTEM_H_

#include "uv.h"
#include "easylogging++.h"
#include <assert.h>
#include <string>
#include <iostream>

#ifdef _WIN32 // WINDOWS
#define DIR_DELIM "\\"
#define FILESYS_CASE_INSENSITIVE 1
#else // POSIX
#define DIR_DELIM "/"
#define FILESYS_CASE_INSENSITIVE 0
#endif

namespace fs_system
{
  // Returns true if already exists
  bool CreateDir(std::string path);

  bool PathExists(std::string path);

  bool IsDir(std::string path);

  bool IsDirDelimiter(char c);

  bool DeleteEmptyDirectory(std::string path);

  // Create all directories on the given path that don't already exist.
  bool CreateAllDirs(std::string path);

  // Remove last path component and the dir delimiter before and/or after it,
  // returns "" if there is only one path component.
  // removed: If non-NULL, receives the removed component(s).
  // count: Number of components to remove
  std::string RemoveLastPathComponent(std::string path,
    std::string *removed = NULL, int count = 1);

  // Remove "." and ".." path components and for every ".." removed, remove
  // the last normal path component before it. Unlike AbsolutePath,
  // this does not resolve symlinks and check for existence of directories.
  std::string RemoveRelativePathComponents(std::string path);
}//fs

class File
{
public:
  enum Whence
  {
    FROM_BEGIN = 0,
    FROM_CURRENT = 1,
    FROM_END = 2
  };
public:
  // Creates or opens the given file. This will fail with 'access denied' if the
  // |name| contains path traversal ('..') components.
  File(const std::string& name, uint32_t flags);
  
  ~File();

public:
  // Reads the given number of bytes (or until EOF is reached) starting with the
  // given offset. Returns the number of bytes read, or -1 on error. Note that
  // this function makes a best effort to read all data on all platforms, so it
  // is not intended for stream oriented files but instead for cases when the
  // normal expectation is that actually |size| bytes are read unless there is
  // an error.
  int Read(int64_t offset, char* data, int size);

  // Writes the given buffer into the file at the given offset, overwritting any
  // data that was previously there. Returns the number of bytes written, or -1
  // on error. Note that this function makes a best effort to write all data on
  // all platforms.
  // Ignores the offset and writes to the end of the file if the file was opened
  // with FLAG_APPEND.
  int Write(int64_t offset, const char* data, int size);

  // Flushes the buffers.
  bool Flush(bool onlyData);

  // Flushes the buffers.
  bool TryFlush(int64_t offset, int64_t size);

  bool IsValid() const;

  // Returns the current size of this file, or a negative number on failure.
  int64_t GetLength();

  // Destroying this object closes the file automatically.
  void Close();

  // Changes current position in the file to an |offset| relative to an origin
  // defined by |whence|. Returns the resultant current position in the file
  // (relative to the start) or -1 in case of error.
  int64_t Seek(Whence whence, int64_t offset);

public:
#if defined(WIN32)
  HANDLE file_;
#else
  int file_;
#endif  //! #if defined(WIN32)
};

#endif  //! #ifndef _FILE_SYSTEM_H_

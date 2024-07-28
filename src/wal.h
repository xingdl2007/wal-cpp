//
// Created by xdl-2 on 2024/7/27.
//

#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <deque>
#include <error.h>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include <atomic>
#include <cassert>
#include <string>

using lsn_t = uint64_t;

struct LogRecord {
  size_t len;
  char entry[];
};

struct Option {
  static Option DefaultOption() {
    struct Option opt;
    opt.db_path = ".";                       // current directory
    opt.log_buffer_size = 256 * 1024 * 1024; // 256MB
    opt.page_size = 4 * 1024;                // 4KB
    return opt;
  }

  // data path, default "."
  std::string db_path;

  // in-memory buffer size, default 256MB
  size_t log_buffer_size;

  // IO unit, default 4KB
  size_t page_size;
};

class LSNode;
class WritableFile;

class MinHeap {
public:
  MinHeap() = default;
  ~MinHeap() = default;

  LSNode *Top();
  void Pop();

  bool Add(LSNode *);
  bool IsEmpty();

private:
  std::unordered_map<lsn_t, LSNode *> map_;
  std::deque<lsn_t> heap_;
};

class WALManager {
public:
  WALManager();
  explicit WALManager(const Option &opt);
  virtual ~WALManager();

  bool Write(const LogRecord &record, lsn_t *lsn);

  lsn_t Recovery();

private:
  void TrackLSN();
  void FlushLog();

  void CopyPayload(LSNode *, const LogRecord &record, uint32_t, uint32_t);
  void UpdateHIndex(LSNode *, uint32_t);

private:
  Option option_;

  std::thread worker_thread_; // track lsn
  std::thread flush_thread_;  // flush log to disk

  // in-memory log buffer
  char *log_buffer_;
  const size_t log_buffer_size_;
  const size_t log_buffer_mask_;

  // manifest file: record offset of first whole log in each file
  // FILENAME OFFSET pairs
  WritableFile *manifest_file_;
  std::string manifest_filename_;

  // wal on-disk file
  WritableFile *logfile_;
  std::string log_filename_;

  // hopping distance, e.g. page size
  const size_t hopping_distance_;

  // h-index table
  const size_t index_table_size_;
  std::atomic<int> *h_index_;

  MinHeap min_heap_;

  std::atomic<lsn_t> lsn_; // log sequence number
  std::atomic<lsn_t> sbl_; // sequentially buffered log
  std::atomic<lsn_t> sdl_; // storage durable log
};

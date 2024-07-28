//
// Created by xdl-2 on 2024/7/27.
//

#include <fcntl.h>

#include <atomic>
#include <cstring>
#include <functional>
#include <iostream>
#include <sstream>

#include "crc32.h"
#include "slice.h"
#include "status.h"
#include "wal.h"

Status PosixError(const std::string &context, int error_number);

class WritableFile {
public:
  WritableFile(std::string filename, int fd)
      : fd_(fd), is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)), dirname_(Dirname(filename_)) {}

  ~WritableFile() {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }

  Status Close() {
    Status status;
    const int close_result = ::close(fd_);
    if (close_result < 0) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  Status Sync() {
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    return SyncFd(fd_, filename_);
  }

  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }
    int fd = ::open(dirname_.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  Status Write(const char *data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue; // Retry
        }
        return PosixError(filename_, errno);
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

private:
  // Ensures that all the caches associated with the given file descriptor's
  // data are flushed all the way to durable media, and can withstand power
  // failures.
  //
  // The path argument is only used to populate the description string in the
  // returned Status if an error occurs.
  static Status SyncFd(int fd, const std::string &fd_path) {
    bool sync_success = ::fdatasync(fd) == 0;
    if (sync_success) {
      return Status::OK();
    }
    return PosixError(fd_path, errno);
  }

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  static std::string Dirname(const std::string &filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);
    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  static Slice Basename(const std::string &filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return Slice(filename.data() + separator_pos + 1,
                 filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  static bool IsManifest(const std::string &filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  int fd_;

  const bool is_manifest_; // True if the file's name starts with MANIFEST.
  const std::string filename_;
  const std::string dirname_; // The directory of filename_.
};

Status PosixError(const std::string &context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

std::string ManifestFilename(std::string db_path) {
  std::stringstream ss;

  char buffer[128];
  // Converts an unsigned integer into hexadecimal representation
  sprintf(buffer, "MANIFEST");

  ss << db_path;
  ss << "/";
  ss << buffer;
  return ss.str();
}

std::string LogFilename(std::string db_path, lsn_t sdl) {
  std::stringstream ss;

  char buffer[128];
  // Converts an unsigned integer into hexadecimal representation
  sprintf(buffer, "%016lx.log", sdl);

  ss << db_path;
  ss << "/";
  ss << buffer;
  return ss.str();
}

Status NewWritableFile(const std::string &filename, WritableFile **result) {
  int fd =
      ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    *result = nullptr;
    return PosixError(filename, errno);
  }

  *result = new WritableFile(filename, fd);
  return Status::OK();
}

class GrassHopper;
struct LSNode {
  lsn_t start_lsn;
  lsn_t end_lsn;
  GrassHopper *owner;

  LSNode *c_next; // for c-list
  LSNode *c_prev; // for c-list
  LSNode *gc;

  LSNode *h_next; // for h-list
  LSNode *h_prev; // for h-list

  LSNode() {
    start_lsn = end_lsn = 0;
    owner = nullptr;

    c_next = c_prev = this;
    h_next = h_prev = this;
  }
  ~LSNode() = default;
};

LSNode *MinHeap::Top() {
  if (heap_.empty()) {
    return nullptr;
  }
  auto v = heap_.front();
  return map_[v];
}

void MinHeap::Pop() {
  if (heap_.empty()) {
    return;
  }
  std::pop_heap(heap_.begin(), heap_.end(), std::greater<>());
  auto v = heap_.back();
  heap_.pop_back();
  map_.erase(v);
}

bool MinHeap::Add(LSNode *node) {
  if (map_.find(node->start_lsn) != map_.end()) {
    return false; // already exists
  }
  map_[node->start_lsn] = node;
  heap_.push_back(node->start_lsn);

  // make mini-heap
  std::make_heap(heap_.begin(), heap_.end(), std::greater<>{});
  return true;
}

bool MinHeap::IsEmpty() { return heap_.empty(); }

struct GrassHopper {
  LSNode c_list; // crawling list
  LSNode h_list; // hopping list

  GrassHopper *next_;
  GrassHopper *prev_;

  GrassHopper() { next_ = prev_ = this; }
  ~GrassHopper() = default;

  // add to tail of c-list and h-list
  void AddNodeToTail(LSNode *node, size_t mask) {
    auto *pre_tail = c_list.c_prev;
    bool add_h_list = false;
    if (pre_tail == &c_list ||
        (pre_tail->start_lsn & mask) != (node->start_lsn & mask)) {
      add_h_list = true;
    }

    // add to crawling list tail
    node->c_next = &c_list; // node as new tail
    node->c_prev = pre_tail;
    pre_tail->c_next = node;
    c_list.c_prev = node;

    // add to hopping list tail
    if (add_h_list) {
      auto *h_pre_tail = h_list.h_prev;
      node->h_next = &h_list;
      node->h_prev = h_pre_tail;
      h_pre_tail->h_next = node;
      h_list.h_prev = node;
    }
  }

  void AdvanceCList(LSNode *node) {}

  void AdvanceHList(lsn_t end_lsn) {}

  // deallocate durable LSNode
  void CleanUp() {}
};

// list of all grasshoppers
std::mutex grasshopper_lock;
GrassHopper dummy_grasshopper;
__thread GrassHopper *per_thead_grasshopper;

GrassHopper *GetPerThreadGrassHopper() {
  if (per_thead_grasshopper == nullptr) {
    per_thead_grasshopper = new GrassHopper();
    if (per_thead_grasshopper == nullptr) {
      std::cerr << "GetGrassHopper: malloc failed";
      abort();
    } else {
      std::lock_guard<std::mutex> lock(grasshopper_lock);
      per_thead_grasshopper->next_ = dummy_grasshopper.next_;
      per_thead_grasshopper->prev_ = dummy_grasshopper.next_->prev_;

      dummy_grasshopper.next_->prev_ = per_thead_grasshopper;
      dummy_grasshopper.next_ = per_thead_grasshopper;
    }
  }
  return per_thead_grasshopper;
}

WALManager::WALManager() : WALManager(Option::DefaultOption()) {}

WALManager::WALManager(const Option &option)
    : option_(option), log_buffer_size_(option_.log_buffer_size),
      log_buffer_mask_(log_buffer_size_ - 1),
      hopping_distance_(option_.page_size),
      index_table_size_(option_.log_buffer_size / option_.page_size), lsn_(0),
      sbl_(0), sdl_(0) {

  // log buffer size must be power of 2
  assert((log_buffer_size_ & (log_buffer_size_ - 1)) == 0);
  log_buffer_ = (char *)malloc(log_buffer_size_);
  if (log_buffer_ == nullptr) {
    std::cerr << "WALManager ctor: log buffer malloc failed" << std::endl;
    abort();
  }

  assert(index_table_size_);
  h_index_ = new std::atomic<int>[index_table_size_];
  if (h_index_ == nullptr) {
    std::cerr << "WALManager ctor: h index table malloc failed" << std::endl;
    abort();
  }

  // The following should really be part of Recover
  worker_thread_ = std::thread(std::bind(&WALManager::TrackLSN, this));
  flush_thread_ = std::thread(std::bind(&WALManager::FlushLog, this));

  Status s =
      NewWritableFile(ManifestFilename(option_.db_path), &manifest_file_);
  if (!s.ok()) {
    std::cerr << "WALManager ctor: new manifest file failed, err msg: "
              << s.ToString() << std::endl;
    abort();
  }

  s = NewWritableFile(LogFilename(option_.db_path, sdl_), &logfile_);
  if (!s.ok()) {
    std::cerr << "WALManager ctor: new log file failed, err msg: "
              << s.ToString() << std::endl;
    abort();
  }
}

WALManager::~WALManager() {
  delete h_index_;
  delete log_buffer_;
  if (worker_thread_.joinable()) {
    worker_thread_.joinable();
  }
  if (flush_thread_.joinable()) {
    flush_thread_.joinable();
  }
  if (manifest_file_ != nullptr) {
    manifest_file_->Close();
    delete manifest_file_;
  }
  if (logfile_ != nullptr) {
    logfile_->Close();
    delete logfile_;
  }
}

void WALManager::CopyPayload(LSNode *node, const LogRecord &record,
                             uint32_t crc32, uint32_t size) {
  // copy data to log buffer
  char *dest = log_buffer_ + (node->start_lsn & log_buffer_mask_);
  uint32_t remaining = log_buffer_size_ - (node->start_lsn & log_buffer_mask_);

  // normal condition
  if (remaining >= size) {
    EncodeFixed32(dest, record.len);
    EncodeFixed32(dest + sizeof(uint32_t), crc32);
    memcpy(dest + sizeof(uint32_t) * 2, record.entry, record.len);
  } else {
    // wraparound condition: record wrap around point for recover
    const size_t wraparound_offset = size - remaining;

    char meta_buffer[sizeof(uint32_t) * 2] = {0};
    EncodeFixed32(meta_buffer, record.len);
    EncodeFixed32(meta_buffer + sizeof(uint32_t), crc32);

    // metadata is enough
    if (remaining >= sizeof(uint32_t) * 2) {
      memcpy(dest, meta_buffer, sizeof(meta_buffer));
      remaining -= sizeof(meta_buffer);
      if (remaining != 0) {
        memcpy(dest + sizeof(meta_buffer), record.entry, remaining);
      }
      uint32_t record_left = record.len - remaining;
      memcpy(log_buffer_, record.entry + remaining, record_left);
    } else {
      // metadata is not enough
      memcpy(dest, meta_buffer, remaining);
      auto meta_left = sizeof(meta_buffer) - remaining;
      memcpy(log_buffer_, meta_buffer + remaining, meta_left);
      memcpy(log_buffer_ + meta_left, record.entry, record.len);
    }
  }
}

void WALManager::UpdateHIndex(LSNode *node, uint32_t size) {
  auto start_lsn_offset = node->start_lsn & log_buffer_mask_;
  auto end_lsn_offset = node->end_lsn & log_buffer_mask_;

  auto start_index = (start_lsn_offset / option_.page_size);
  auto end_index = (end_lsn_offset / option_.page_size);

  if (start_index == end_index) {
    h_index_[start_index] += size;
  } else {
    size_t i = start_index;
    auto tmp_size = option_.page_size - (start_lsn_offset % option_.page_size);
    h_index_[i] += tmp_size;
    size -= tmp_size;
    i = (i + 1) % index_table_size_;
    for (; i != end_index; i = (i + 1) % index_table_size_) {
      h_index_[i] += option_.page_size;
      size -= option_.page_size;
    }
    h_index_[i] += size; // left
  }
}

// Append CRC32 (only cover data part) to detect partial record
bool WALManager::Write(const LogRecord &record, lsn_t *lsn) {
  uint32_t crc32 = Value(record.entry, record.len);
  const size_t size =
      sizeof(uint32_t) + record.len + sizeof(uint32_t); // for CRC32
  LSNode *node = new LSNode();
  if (!node) {
    std::cerr << "Write: allocate LSNode failed" << std::endl;
    return false;
  }

  node->start_lsn = lsn_.fetch_add(size);
  node->end_lsn = node->start_lsn + size - 1;

  CopyPayload(node, record, crc32, size);

  // fast path to detect sbl
  UpdateHIndex(node, size);

  GrassHopper *grassHopper = GetPerThreadGrassHopper();
  assert(grassHopper);
  node->owner = grassHopper;

  if (!lsn) {
    *lsn = node->start_lsn;
  }
  const auto end_lsn = node->end_lsn;

  // add node to c-list and h-list
  grassHopper->AddNodeToTail(node, option_.page_size - 1);

  // busy waiting, and do not touch node anymore
  while (sdl_.load(std::memory_order_relaxed) < end_lsn) {
    grassHopper->CleanUp();
  }
  return true;
}

lsn_t WALManager::Recovery() { return 0; }

void WALManager::TrackLSN() {
  while (true) {
    auto lsn = lsn_.load();
    auto sbl = sbl_.load();
    if (lsn - sbl >= option_.page_size) {
      // sbl lagging too much, hopping
      auto i = (sbl & log_buffer_mask_) / option_.page_size;
      if (h_index_[i] == option_.page_size) {
        h_index_[i] = 0;          // reset to zero
        sbl += option_.page_size; // notify flush thread to flush

        for (auto *p = dummy_grasshopper.next_;
             p != nullptr && p != &dummy_grasshopper; p = p->next_) {
          p->AdvanceHList(sbl);
        }
        continue;
      }
    }

    // otherwise, crawling
    // build LSN mini-heap
    for (auto *p = dummy_grasshopper.next_;
         p != nullptr && p != &dummy_grasshopper; p = p->next_) {
      min_heap_.Add(p->c_list.c_next); // head
    }

    // trace LSN hole, update if LSN is sequential
    auto *node = min_heap_.Top();
    if (node->start_lsn == sbl) {
      sbl_.store(node->end_lsn); // advancing sbl
      node->owner->AdvanceCList(node);
    }
  }
}

void WALManager::FlushLog() {}
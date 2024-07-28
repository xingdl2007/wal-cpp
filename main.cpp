#include <iostream>
// 本文件用于参赛选手进行自测

// 测试方法
// 编译：g++ test.cpp wal.h util.h -lpthread -o test
// 运行：./test
// 如果在代码中引入了其它第三方库，需要自行增加链接文件
// 如果想要修改一些运行时参数如线程数、运行时间等，请自行修改下面的参数write_threads、run_time等

// 测试指标
// 1. 正确性评测
//  a. LSN递增
//  b. Recovery()可以返回上次进程退出时的最大LSN

// 2. 性能评测
//  a. 总体吞吐量 ops/sec
//  b. LogWrite 平均延迟 ns
//  c. LogWrite P99延迟 ns

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <thread>

#include "src/wal.h"
#include "util.h"

// 以下参数仅供参考，实际评测时会选择其它参数组合

uint64_t write_threads = 64;
uint64_t run_time = 10; // 单位为秒
bool recover = false;
uint64_t last_lsn = 0; // 上次进程退出时的最大lsn，仅当recover为true时参数有效

std::vector<std::thread> threads;
std::vector<ThreadInfo> thread_info;
WALManager *wal_manager = nullptr;
std::atomic<bool> quit(false);
std::atomic<lsn_t> max_lsn{0};

void Stop(uint64_t delay) {
  std::this_thread::sleep_for(std::chrono::seconds(delay));
  quit.store(true, std::memory_order_relaxed);
  std::cout << "Time Out" << std::endl;
}

void Init() { wal_manager = new WALManager(); }

void LogWrite(ThreadInfo &thread_info) {
  // 生成随机长度LogRecord
  const int minLength = 64;
  const int maxLength = 128;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<int> lengthDistribution(minLength, maxLength);

  int randomLength = lengthDistribution(generator);
  LogRecord *record =
      reinterpret_cast<LogRecord *>(malloc(sizeof(LogRecord) + randomLength));
  record->len = randomLength;
  memset(record->entry, 'x', randomLength);

  lsn_t lsn = 0;
  // 在限定时间内while循环调用Write()
  while (!quit.load(std::memory_order_relaxed)) {
    auto old_max_lsn = max_lsn.load(std::memory_order_relaxed);

    auto start_times = NowNanos();
    wal_manager->Write(*record, &lsn);
    auto end_times = NowNanos();

    if (lsn <= old_max_lsn) {
      std::cout << "Test Fail!" << std::endl;
      std::cout << "LSN " << lsn << " less than " << old_max_lsn << std::endl;
      exit(1);
    }
    auto now_max_lsn = max_lsn.load(std::memory_order_relaxed);
    while (lsn > now_max_lsn &&
           !max_lsn.compare_exchange_strong(now_max_lsn, lsn)) {
    }
    thread_info.stat.Add(end_times - start_times);
    thread_info.key_count++;
  }
}

int main(int argc, char **argv) {
  Init();

  // 如果是recover模式，则验证LSN
  if (recover) {
    uint64_t lsn = wal_manager->Recovery();
    if (lsn != last_lsn) {
      std::cout << "Test Fail!" << std::endl;
      std::cout << "LSN " << lsn << " not equal to " << last_lsn << std::endl;
      exit(-1);
    }
    std::cout << "Recover Test Success!" << std::endl;
  }
  std::thread timerThread(Stop, run_time);

  // 并发写入WAL
  auto start_times = NowNanos();
  thread_info.resize(write_threads);

  for (int i = 0; i < write_threads; ++i) {
    threads.push_back(std::thread(LogWrite, std::ref(thread_info[i])));
  }
  for (auto &thread : threads) {
    thread.join();
  }
  auto end_times = NowNanos();
  std::cout << "Max LSN: " << max_lsn.load(std::memory_order_relaxed)
            << std::endl;
  timerThread.join();

  // 获得统计信息
  uint64_t total_key_count = 0;
  HistogramStat total_stat;
  for (auto &info : thread_info) {
    total_key_count += info.key_count;
    total_stat.Merge(info.stat);
  }

  std::cout << "Average Throughput: "
            << total_key_count * 1000000000 / (end_times - start_times)
            << " ops/sec " << std::endl;

  std::cout << "Average Latency: " << total_stat.Average() << " ns"
            << std::endl;
  std::cout << "P99 Latency: " << total_stat.Percentile(99) << " ns"
            << std::endl;
  std::cout << "Write Test Success!" << std::endl;
  return 0;
}

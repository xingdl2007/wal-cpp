//
// Created by xdl-2 on 2024/7/27.
//

#ifndef WAL_CPP_UTIL_H
#define WAL_CPP_UTIL_H

#include <math.h>
#include <stdint.h>
#include <stdio.h>

#include <cinttypes>
#include <ctime>
#include <map>

uint64_t NowNanos() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

class HistogramBucketMapper {
public:
  HistogramBucketMapper() {
    bucketValues_ = {1, 2};
    valueIndexMap_ = {{1, 0}, {2, 1}};
    double bucket_val = static_cast<double>(bucketValues_.back());
    while ((bucket_val = 1.5 * bucket_val) <= static_cast<double>(UINT64_MAX)) {
      bucketValues_.push_back(static_cast<uint64_t>(bucket_val));
      uint64_t pow_of_ten = 1;
      while (bucketValues_.back() / 10 > 10) {
        bucketValues_.back() /= 10;
        pow_of_ten *= 10;
      }
      bucketValues_.back() *= pow_of_ten;
      valueIndexMap_[bucketValues_.back()] = bucketValues_.size() - 1;
    }
    maxBucketValue_ = bucketValues_.back();
    minBucketValue_ = bucketValues_.front();
  }

  size_t IndexForValue(const uint64_t value) const {
    if (value >= maxBucketValue_) {
      return bucketValues_.size() - 1;
    } else if (value >= minBucketValue_) {
      auto lowerBound = valueIndexMap_.lower_bound(value);
      if (lowerBound != valueIndexMap_.end()) {
        return static_cast<size_t>(lowerBound->second);
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }
  size_t BucketCount() const { return bucketValues_.size(); }

  uint64_t LastValue() const { return maxBucketValue_; }

  uint64_t FirstValue() const { return minBucketValue_; }

  uint64_t BucketLimit(const size_t bucketNumber) const {
    return bucketValues_[bucketNumber];
  }

private:
  std::vector<uint64_t> bucketValues_;
  uint64_t maxBucketValue_;
  uint64_t minBucketValue_;
  std::map<uint64_t, uint64_t> valueIndexMap_;
};

namespace {
const HistogramBucketMapper bucketMapper;
}

struct HistogramStat {
  HistogramStat() : num_buckets_(109) {
    min_ = UINT64_MAX;
    max_ = 0;
    num_ = 0;
    sum_ = 0;
    for (unsigned int b = 0; b < num_buckets_; b++) {
      buckets_[b] = 0;
    }
  }
  ~HistogramStat() {}

  void Add(uint64_t value) {
    const size_t index = bucketMapper.IndexForValue(value);
    buckets_[index] += 1;

    if (value < min_) {
      min_ = value;
    }

    if (value > max_) {
      max_ = value;
    }

    num_ += 1;
    sum_ += value;
  }
  void Merge(const HistogramStat &other) {
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);

    num_ += other.num_;
    sum_ += other.sum_;
    for (unsigned int b = 0; b < num_buckets_; b++) {
      buckets_[b] += other.bucket_at(b);
    }
  }

  inline uint64_t bucket_at(size_t b) const { return buckets_[b]; }

  double Percentile(double p) const {
    double threshold = num_ * (p / 100.0);
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < num_buckets_; b++) {
      uint64_t bucket_value = bucket_at(b);
      cumulative_sum += bucket_value;
      if (cumulative_sum >= threshold) {
        // Scale linearly within this bucket
        uint64_t left_point = (b == 0) ? 0 : bucketMapper.BucketLimit(b - 1);
        uint64_t right_point = bucketMapper.BucketLimit(b);
        uint64_t left_sum = cumulative_sum - bucket_value;
        uint64_t right_sum = cumulative_sum;
        double pos = 0;
        uint64_t right_left_diff = right_sum - left_sum;
        if (right_left_diff != 0) {
          pos = (threshold - left_sum) / right_left_diff;
        }
        double r = left_point + (right_point - left_point) * pos;
        uint64_t cur_min = min_;
        uint64_t cur_max = max_;
        if (r < cur_min)
          r = static_cast<double>(cur_min);
        if (r > cur_max)
          r = static_cast<double>(cur_max);
        return r;
      }
    }
    return static_cast<double>(max_);
  }
  double Average() const {
    if (num_ == 0)
      return 0;
    return static_cast<double>(sum_) / static_cast<double>(num_);
  }

  std::string ToString() const {
    uint64_t cur_num = num_;
    std::string r;
    char buf[1650];
    snprintf(buf, sizeof(buf), "Count: %" PRIu64 " Average: %.4f \n", cur_num,
             Average());
    r.append(buf);

    r.append(buf);
    snprintf(buf, sizeof(buf),
             "Percentiles: "
             "P50: %.2f P75: %.2f P99: %.2f P99.9: %.2f P99.99: %.2f\n",
             Percentile(50), Percentile(75), Percentile(99), Percentile(99.9),
             Percentile(99.99));
    r.append(buf);
    r.append("------------------------------------------------------\n");
    if (cur_num == 0)
      return r; // all buckets are empty
    const double mult = 100.0 / cur_num;
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < num_buckets_; b++) {
      uint64_t bucket_value = bucket_at(b);
      if (bucket_value <= 0.0)
        continue;
      cumulative_sum += bucket_value;
      snprintf(buf, sizeof(buf),
               "%c %7" PRIu64 ", %7" PRIu64 " ] %8" PRIu64 " %7.3f%% %7.3f%% ",
               (b == 0) ? '[' : '(',
               (b == 0) ? 0 : bucketMapper.BucketLimit(b - 1), // left
               bucketMapper.BucketLimit(b),                    // right
               bucket_value,                                   // count
               (mult * bucket_value),                          // percentage
               (mult * cumulative_sum)); // cumulative percentage
      r.append(buf);

      // Add hash marks based on percentage; 20 marks for 100%.
      size_t marks = static_cast<size_t>(mult * bucket_value / 5 + 0.5);
      r.append(marks, '#');
      r.push_back('\n');
    }
    return r;
  }

  uint64_t min_;
  uint64_t max_;
  uint64_t num_;
  uint64_t sum_;
  uint64_t buckets_[109]; // 109==BucketMapper::BucketCount()
  const uint64_t num_buckets_;
};

// thread local info
struct ThreadInfo {
  HistogramStat stat;
  int key_count;
  ThreadInfo() : stat(HistogramStat()), key_count(0) { key_count = 0; }
};

#endif // WAL_CPP_UTIL_H

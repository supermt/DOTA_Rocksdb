//
// Created by jinghuan on 8/24/21.
//

#ifndef ROCKSDB_DOTA_TUNER_H
#define ROCKSDB_DOTA_TUNER_H
#pragma once
#include <iostream>

namespace ROCKSDB_NAMESPACE {

enum ThreadStallLevels : int {
  kNeedMoreFlush,
  kNeedMoreCompaction,
  kGoodArea,
  kQuicksand,
  kBandwidthCongestion,
  kPendingBytes
};
enum BatchSizeStallLevels : int {
  kTinyMemtable,
  kL0Stall,
  kStallFree,
  kOversizeCompaction
};

struct SystemScores {
  // Memory Component
  uint64_t memtable_speed;   // MB per sec
  double active_size_ratio;  // active size / total memtable size
  int immutable_number;      // NonFlush number
  // Flushing
  double flush_speed_avg;
  double flush_speed_var;
  // Compaction speed
  double l0_num;
  // LSM  size
  double l0_drop_ratio;
  double estimate_compaction_bytes;  // given by the system, divided by the soft
                                     // limit
  // System metrics
  double disk_bandwidth;   // avg
  double total_idle_time;  // calculate by idle calculating,flush and compaction
                           // stats separately

  SystemScores() { Reset(); }
  void Reset() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    total_idle_time = 0.0;
  }
  ThreadStallLevels EstimateThreadStat() { return kGoodArea; }
  BatchSizeStallLevels EstimateBatchStat() { return kStallFree; }
  SystemScores operator-(const SystemScores& a);
};

typedef SystemScores ScoreGradient;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
  bool db_width;
};
enum TuningOP { kDouble, kLinear, kDecrease, kKeep, kOPNum = 3 };
class DOTA_Tuner {
 private:
  const Options default_opts;
  uint64_t tuning_rounds;
  Options current_opt;
  Version* version;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;
  const static int locating_rounds = 5;
  const static int locating_before =
      15;  // the first secs, locate the system behavior first
  DBImpl* running_db_;
  int64_t* last_report_ptr;
  std::atomic<int64_t>* total_ops_done_ptr_;
  std::vector<SystemScores> scores;
  std::vector<ScoreGradient> gradients;
  int current_sec;
  uint64_t flush_list_accessed, compaction_list_accessed;

  std::shared_ptr<std::vector<FlushMetrics>> flush_list_from_opt_ptr;
  std::shared_ptr<std::vector<QuicksandMetrics>> compaction_list_from_opt_ptr;

  void UpdateSystemStats() { UpdateSystemStats(running_db_); }

 public:
  DOTA_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr)
      : default_opts(opt),
        tuning_rounds(0),
        running_db_(running_db),
        current_sec(0),
        flush_list_accessed(0),
        compaction_list_accessed(0),
        flush_list_from_opt_ptr(running_db->immutable_db_options().flush_stats),
        compaction_list_from_opt_ptr(
            running_db->immutable_db_options().job_stats) {
    this->last_report_ptr = last_report_op_ptr;
    this->total_ops_done_ptr_ = total_ops_done_ptr;
  }

  void ResetTuner() { tuning_rounds = 0; }
  void UpdateSystemStats(DBImpl* running_db) {
    current_opt = running_db->GetOptions();
    version = running_db->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }
  void DetectTuningOperations(int secs_elapsed,
                              std::vector<ChangePoint>* change_list);

  ScoreGradient CompareWithBefore() { return scores.back() - scores.front(); }
  ScoreGradient CompareWithBefore(SystemScores& past_score) {
    return scores.back() - past_score;
  }
  ScoreGradient CompareWithBefore(SystemScores& past_score,
                                  SystemScores& current_score) {
    return current_score - past_score;
  }
  void LocateByScore(SystemScores& score);
  SystemScores ScoreTheSystem();
  void GenerateTuningOP(std::vector<ChangePoint>* change_list);
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_DOTA_TUNER_H

//
// Created by jinghuan on 8/24/21.
//

#ifndef ROCKSDB_DOTA_TUNER_H
#define ROCKSDB_DOTA_TUNER_H
#pragma once
#include <iostream>

namespace ROCKSDB_NAMESPACE {

enum ThreadStallLevels : int {
  kLowFlush,
  kL0Stall,
  kPendingBytes,
  kGoodArea,
  kIdle,
  kBandwidthCongestion
};
enum BatchSizeStallLevels : int {
  kTinyMemtable,  // tiny memtable
  kStallFree,
  kOverFrequent
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
  int flush_numbers;

  SystemScores() {
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
    flush_numbers = 0;
  }
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
    flush_numbers = 0;
  }
  SystemScores operator-(const SystemScores& a);
};

typedef SystemScores ScoreGradient;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
  bool db_width;
};
enum OpType : int { kDouble, kLinearIncrease, kLinearDecrease, kHalf, kKeep };
struct TuningOP {
  OpType BatchOp;
  OpType ThreadOp;
};
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
  ThreadStallLevels last_thread_states;
  BatchSizeStallLevels last_batch_stat;
  std::shared_ptr<std::vector<FlushMetrics>> flush_list_from_opt_ptr;
  std::shared_ptr<std::vector<QuicksandMetrics>> compaction_list_from_opt_ptr;
  SystemScores max_scores;
  uint64_t last_flush_thread_len;
  uint64_t last_compaction_thread_len;
  Env* env_;
  int tuning_gap;
  void UpdateSystemStats() { UpdateSystemStats(running_db_); }

 public:
  DOTA_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env, int gap_sec)
      : default_opts(opt),
        tuning_rounds(0),
        running_db_(running_db),
        scores(0),
        gradients(0),
        current_sec(0),
        flush_list_accessed(0),
        compaction_list_accessed(0),
        last_thread_states(kL0Stall),
        last_batch_stat(kTinyMemtable),
        flush_list_from_opt_ptr(running_db->immutable_db_options().flush_stats),
        compaction_list_from_opt_ptr(
            running_db->immutable_db_options().job_stats),
        max_scores(),
        last_flush_thread_len(0),
        last_compaction_thread_len(0),
        env_(env),
        tuning_gap(gap_sec) {
    this->last_report_ptr = last_report_op_ptr;
    this->total_ops_done_ptr_ = total_ops_done_ptr;
  }
  inline void UpdateMaxScore(SystemScores& current_score) {
    if (current_score.memtable_speed > scores.front().memtable_speed * 2) {
      // this would be an error
      return;
    }

    if (current_score.memtable_speed > max_scores.memtable_speed) {
      max_scores.memtable_speed = current_score.memtable_speed;
    }
    if (current_score.active_size_ratio > max_scores.active_size_ratio) {
      max_scores.active_size_ratio = current_score.active_size_ratio;
    }
    if (current_score.immutable_number > max_scores.immutable_number) {
      max_scores.immutable_number = current_score.immutable_number;
    }
    if (current_score.flush_speed_avg > max_scores.flush_speed_avg) {
      max_scores.flush_speed_avg = current_score.flush_speed_avg;
    }
    if (current_score.flush_speed_var > max_scores.flush_speed_var) {
      max_scores.flush_speed_var = current_score.flush_speed_var;
    }
    if (current_score.l0_num > max_scores.l0_num) {
      max_scores.l0_num = current_score.l0_num;
    }
    if (current_score.l0_drop_ratio > max_scores.l0_drop_ratio) {
      max_scores.l0_drop_ratio = current_score.l0_drop_ratio;
    }
    if (current_score.estimate_compaction_bytes >
        max_scores.estimate_compaction_bytes) {
      max_scores.estimate_compaction_bytes =
          current_score.estimate_compaction_bytes;
    }
    if (current_score.disk_bandwidth > max_scores.disk_bandwidth) {
      max_scores.disk_bandwidth = current_score.disk_bandwidth;
    }
    if (current_score.total_idle_time > max_scores.total_idle_time) {
      max_scores.total_idle_time = current_score.total_idle_time;
    }
    if (current_score.flush_numbers > max_scores.flush_numbers) {
      max_scores.flush_numbers = current_score.flush_numbers;
    }
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
  ThreadStallLevels LocateThreadStates(SystemScores& score);
  BatchSizeStallLevels LocateBatchStates(SystemScores& score);

  const std::string memtable_size = "write_buffer_size";
  const std::string table_size = "target_file_size_base";
  const std::string max_bg_jobs = "max_background_jobs";

  const uint64_t max_thread = 24;
  const uint64_t min_thread = 2;
  const uint64_t max_memtable_size = 512 << 20;
  const uint64_t min_memtable_size = 32 << 20;

  SystemScores ScoreTheSystem();
  void AdjustmentTuning(std::vector<ChangePoint>* change_list,
                        SystemScores& score, ThreadStallLevels levels,
                        BatchSizeStallLevels stallLevels);
  TuningOP VoteForOP(SystemScores& current_score, ThreadStallLevels levels,
                     BatchSizeStallLevels stallLevels);
  void FillUpChangeList(std::vector<ChangePoint>* change_list, TuningOP op);
  void SetBatchSize(std::vector<ChangePoint>* change_list,
                    uint64_t target_value);
  void SetThreadNum(std::vector<ChangePoint>* change_list,
                    uint64_t target_value);
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_DOTA_TUNER_H

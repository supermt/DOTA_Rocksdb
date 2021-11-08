//
// Created by jinghuan on 8/24/21.
//

#include <vector>

#include "rocksdb/utilities/report_agent.h"

namespace ROCKSDB_NAMESPACE {

void DOTA_Tuner::DetectTuningOperations(
    int secs_elapsed, std::vector<ChangePoint> *change_list_ptr) {
  current_sec = secs_elapsed;
  UpdateSystemStats();
  SystemScores current_score = ScoreTheSystem();
  UpdateMaxScore(current_score);
  scores.push_back(current_score);
  gradients.push_back(current_score - scores.front());

  auto thread_stat = LocateThreadStates(current_score);
  auto batch_stat = LocateBatchStates(current_score);

  AdjustmentTuning(change_list_ptr, current_score, thread_stat, batch_stat);
  // decide the operation based on the best behavior and last behavior
  // update the histories
  last_thread_states = thread_stat;
  last_batch_stat = batch_stat;
  tuning_rounds++;
}
ThreadStallLevels DOTA_Tuner::LocateThreadStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    // speed is slower than before, performance is in the stall area
    if (score.immutable_number >= 1) {
      if (score.flush_speed_avg <= max_scores.flush_speed_avg * 0.5) {
        // it's not influenced by the flushing speed
        if (current_opt.max_background_jobs > 6) {
          return kBandwidthCongestion;
        }
      } else if (score.l0_num > 0.7) {
        // it's in the l0 stall
        return kL0Stall;
      }
    } else if (score.estimate_compaction_bytes > 0.5) {
      return kPendingBytes;
    }
  } else if (score.total_idle_time > 2) {
    return kIdle;
  }
  return kGoodArea;
}

BatchSizeStallLevels DOTA_Tuner::LocateBatchStates(SystemScores &score) {
  if (score.flush_speed_avg < max_scores.flush_speed_avg * 0.5) {
    if (score.active_size_ratio > 0.5 || score.immutable_number > 1) {
      return kTinyMemtable;
    }
    if (score.total_idle_time > 1) {
      // too many idle threads, the memtable is too large.
      return kOversizeCompaction;
    }
    if (score.estimate_compaction_bytes > 0.8) {
      return kOversizeCompaction;
    }
  }
  return kStallFree;
};

SystemScores DOTA_Tuner::ScoreTheSystem() {
  SystemScores current_score;
  current_score.memtable_speed =
      (this->total_ops_done_ptr_->load() - *last_report_ptr) / current_sec;

  uint64_t total_mem_size = 1;
  uint64_t active_mem = 0;
  running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
  running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);

  current_score.active_size_ratio = (double)active_mem / total_mem_size;
  current_score.immutable_number = cfd->imm()->NumNotFlushed();

  std::vector<FlushMetrics> flush_metric_list;
  auto flush_result_length = flush_list_from_opt_ptr->size();
  auto compaction_result_length =
      running_db_->immutable_db_options().job_stats->size();

  for (uint64_t i = flush_list_accessed; i < flush_result_length; i++) {
    auto temp = flush_list_from_opt_ptr->at(i);
    flush_metric_list.push_back(temp);
    current_score.flush_speed_avg += temp.write_out_bandwidth;
    current_score.disk_bandwidth += temp.total_bytes;
  }
  int l0_compaction = 0;

  uint64_t max_pending_bytes = 0;
  for (uint64_t i = compaction_list_accessed; i < compaction_result_length;
       i++) {
    auto temp = compaction_list_from_opt_ptr->at(i);
    if (temp.input_level == 0) {
      current_score.l0_drop_ratio += temp.drop_ratio;
      l0_compaction++;
    }
    if (temp.current_pending_bytes > max_pending_bytes)
      max_pending_bytes = temp.current_pending_bytes;

    current_score.disk_bandwidth += temp.total_bytes;
  }
  auto new_flushes = (flush_result_length - flush_list_accessed);

  // flush_speed_avg,flush_speed_var,l0_drop_ratio
  if (current_score.flush_speed_avg != 0) {
    current_score.flush_speed_avg /= new_flushes;

    for (auto item : flush_metric_list) {
      current_score.flush_speed_var +=
          (item.write_out_bandwidth - current_score.flush_speed_avg) *
          (item.write_out_bandwidth - current_score.flush_speed_avg);
    }
    current_score.flush_speed_var /= new_flushes;
  }
  if (l0_compaction != 0) {
    current_score.l0_drop_ratio /= l0_compaction;
  }
  // l0_num
  current_score.l0_num = (double)(vfs->NumLevelFiles(vfs->base_level())) /
                         current_opt.level0_slowdown_writes_trigger;
  // disk bandwidth,estimate_pending_bytes ratio
  current_score.disk_bandwidth /= kMicrosInSecond;
  current_score.estimate_compaction_bytes =
      (double)max_pending_bytes /
      current_opt.soft_pending_compaction_bytes_limit;

  auto flush_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::HIGH);
  auto compaction_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::LOW);
  std::unordered_map<int, uint64_t> thread_idle_time;
  uint64_t temp = flush_thread_idle_list.size();

  for (uint64_t i = last_flush_thread_len; i < temp; i++) {
    auto temp_entry = flush_thread_idle_list[i];
    int key = -(int)(temp_entry.first + 1);
    auto value = temp_entry.second;
    try {
      thread_idle_time.at(key) += value;
    } catch (std::out_of_range &e) {
      thread_idle_time.emplace(key, value);
    }
  }
  temp = compaction_thread_idle_list.size();
  for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
    auto temp_entry = compaction_thread_idle_list[i];
    int key = (int)(temp_entry.first + 1);
    auto value = temp_entry.second;
    try {
      thread_idle_time.at(key) += value;
    } catch (std::out_of_range &e) {
      thread_idle_time.emplace(key, value);
    }
  }

  for (auto entry : thread_idle_time) {
    double idle_ratio = (double)entry.second / (kMicrosInSecond * tuning_gap);
    current_score.total_idle_time += idle_ratio;
  }

  current_score.total_idle_time /= current_opt.max_background_jobs;

  // clean up
  flush_list_accessed = flush_result_length;
  compaction_list_accessed = compaction_result_length;
  last_compaction_thread_len = compaction_thread_idle_list.size();
  last_flush_thread_len = flush_thread_idle_list.size();

  return current_score;
}

void DOTA_Tuner::AdjustmentTuning(std::vector<ChangePoint> *change_list,
                                  SystemScores &score,
                                  ThreadStallLevels thread_levels,
                                  BatchSizeStallLevels batch_levels) {
  std::cout << thread_levels << " " << batch_levels << std::endl;
  // tune for thread number
  auto tuning_op = VoteForOP(score, thread_levels, batch_levels);
  // tune for memtable
  FillUpChangeList(change_list, tuning_op);
}
TuningOP DOTA_Tuner::VoteForOP(SystemScores & /*current_score*/,
                               ThreadStallLevels thread_level,
                               BatchSizeStallLevels batch_level) {
  TuningOP op;
  switch (thread_level) {
    case kL0Stall:
      op.ThreadOp = kDouble;
      break;
    case kPendingBytes:
      op.ThreadOp = kLinearIncrease;
      break;
    case kGoodArea:
      op.ThreadOp = kKeep;
      break;
    case kIdle:
      op.ThreadOp = kLinearDecrease;
      break;
    case kBandwidthCongestion:
      op.ThreadOp = kHalf;
      break;
  }

  if (batch_level == kTinyMemtable) {
    op.BatchOp = kLinearIncrease;
  } else if (batch_level == kStallFree) {
    op.BatchOp = kKeep;
  } else {
    op.BatchOp = kHalf;
  }

  return op;
}

inline void DOTA_Tuner::SetThreadNum(std::vector<ChangePoint> *change_list,
                                     uint64_t target_value) {
  ChangePoint thread_num_cp;
  thread_num_cp.opt = max_bg_jobs;
  thread_num_cp.db_width = true;
  target_value = std::max(target_value, min_thread);
  target_value = std::min(target_value, max_thread);
  thread_num_cp.value = ToString(target_value);
  change_list->push_back(thread_num_cp);
}

inline void DOTA_Tuner::SetBatchSize(std::vector<ChangePoint> *change_list,
                                     uint64_t target_value) {
  ChangePoint memtable_size_cp;
  memtable_size_cp.db_width = false;
  memtable_size_cp.opt = memtable_size;

  ChangePoint sst_size;
  sst_size.opt = table_size;
  target_value = std::max(target_value, min_memtable_size);
  target_value = std::min(target_value, max_memtable_size);
  memtable_size_cp.value = ToString(target_value);
  sst_size.value = ToString(target_value);
  ChangePoint L1_total_size;
  L1_total_size.opt = table_size;

  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     target_value;
  L1_total_size.value = ToString(l1_size);
  sst_size.db_width = false;
  L1_total_size.db_width = false;
  change_list->push_back(memtable_size_cp);
  change_list->push_back(L1_total_size);
  change_list->push_back(sst_size);
}

void DOTA_Tuner::FillUpChangeList(std::vector<ChangePoint> *change_list,
                                  TuningOP op) {
  uint64_t current_thread_num = current_opt.max_background_jobs;
  uint64_t current_batch_size = current_opt.write_buffer_size;
  switch (op.BatchOp) {
    case kDouble:
      SetBatchSize(change_list, current_batch_size *= 2);
      break;
    case kLinearIncrease:
      SetBatchSize(change_list,
                   current_batch_size += default_opts.write_buffer_size);
      break;
    case kLinearDecrease:
      SetBatchSize(change_list,
                   current_batch_size -= default_opts.write_buffer_size);
      break;
    case kHalf:
      SetBatchSize(change_list, current_batch_size /= 2);
      break;
    case kKeep:
      break;
  }
  switch (op.ThreadOp) {
    case kDouble:
      SetThreadNum(change_list, current_thread_num *= 2);
      break;
    case kLinearIncrease:
      SetThreadNum(change_list, current_thread_num += 1);
      break;
    case kLinearDecrease:
      SetThreadNum(change_list, current_thread_num -= 2);
      break;
    case kHalf:
      SetThreadNum(change_list, current_thread_num /= 2);
      break;
    case kKeep:
      break;
  }
}

// ReporterAgentWithTuning::TuningOP ReporterAgentWithTuning::VoteForMemtable(
//     BatchSizeScore& scores) {
//   // here we use a vote ensemble method to derive the operation;
//   // the weights here will be evaluated  by the ANOVA method
//   double weights[5] = {1.0, 1.0, 1.0, 1.0, 1.0};
//   TuningOP temp_op = last_thread_op;
//   double ensemble[4] = {};
//   {  // decide the op based on l0 score
//     if (scores.l0_stall >= 1)
//       temp_op = kDouble;
//     else if (last_thread_score.l0_stall < scores.l0_stall)
//       temp_op = kLinear;
//     else
//       temp_op = kKeep;
//   }
//   ensemble[temp_op] += (weights[0] * scores.l0_stall);
//   temp_op = last_thread_op;
//   {
//     // decide the op based on memtable
//     if (scores.memtable_stall >= 0.7) {
//       temp_op = kDouble;
//     } else {
//       temp_op = kKeep;
//     }
//   }
//   ensemble[temp_op] += (weights[1] * scores.memtable_stall);
//   {
//     if (scores.pending_bytes_stall > 0.7) {
//       temp_op = kDecrease;
//     } else {
//       temp_op = kLinear;
//     }
//   }
//   ensemble[temp_op] += (weights[2] * scores.pending_bytes_stall);
//   {
//     if (scores.flushing_congestion > last_thread_score.flushing_congestion) {
//       // flushing speed increases
//       temp_op = last_thread_op;
//     } else if (scores.flushing_congestion < 0) {
//       // slower than 0.5 of base flushing speed
//       temp_op = kLinear;
//     } else {
//       // performance is slower than before, but can still keep in a certain
//       // range
//       temp_op = kKeep;
//     }
//   }
//   ensemble[temp_op] += (weights[3] * abs(scores.flushing_congestion));
//   double biggest_score = 0;
//   int vote_result = 0;
//   std::cout << "batch op scores: ";
//   for (int i = 0; i < kKeep + 1; i++) {
//     std::cout << i << " : " << ensemble[i] << " ";
//     if (biggest_score <= ensemble[i]) {
//       biggest_score = ensemble[i];
//       vote_result = i;
//     }
//   }
//   temp_op = TuningOP(vote_result);
//
//   std::cout << "ensemble op: " << temp_op;
//   std::cout << std::endl;
//
//   last_batch_op = temp_op;
//   last_batch_score = scores;
//   return temp_op;
// }
//  void ReporterAgentWithTuning::AdjustBatchChangePoint(
//     ReporterAgentWithTuning::TuningOP stats, std::vector<ChangePoint>*
//     results, Options& current_opt) {
//   uint64_t target_value = current_opt.write_buffer_size;
//   switch (stats) {
//     case kDouble:
//       target_value *= 2;
//       break;
//     case kLinear:
//       target_value += options_when_boost.write_buffer_size;
//       break;
//     case kDecrease:
//       target_value *= 0.5;
//       break;
//     case kKeep:
//       return;
//   }
//   const static uint64_t lower_bound = 32 << 20;
//   const static uint64_t upper_bound = 512 << 20;
//
//   target_value = std::max(lower_bound, target_value);
//   target_value = std::min(upper_bound, target_value);
//
//   ChangePoint write_buffer;
//   ChangePoint sst_size;
//
//   write_buffer.opt = memtable_size;
//   write_buffer.value = ToString(target_value);
//   // sync the size of L0-L1 size;
//   sst_size.opt = table_size;
//   sst_size.value = ToString(target_value);
//   ChangePoint L1_total_size;
//   L1_total_size.opt = table_size;
//
//   uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
//                      current_opt.min_write_buffer_number_to_merge *
//                      target_value;
//   L1_total_size.value = ToString(l1_size);
//   write_buffer.db_width = false;
//   sst_size.db_width = false;
//   L1_total_size.db_width = false;
//
//   results->push_back(write_buffer);
//   results->push_back(sst_size);
//   results->push_back(L1_total_size);
// }

SystemScores SystemScores::operator-(const SystemScores &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed - a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio - a.active_size_ratio;
  temp.immutable_number = this->immutable_number - a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg - a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var - a.flush_speed_var;
  temp.l0_num = this->l0_num - a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio - a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes - a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth - a.disk_bandwidth;
  temp.total_idle_time = this->total_idle_time - a.total_idle_time;

  return temp;
}
}  // namespace ROCKSDB_NAMESPACE

//
// Created by jinghuan on 8/24/21.
//

#include <vector>

#include "rocksdb/utilities/report_agent.h"

namespace ROCKSDB_NAMESPACE {

void DOTA_Tuner::DetectTuningOperations(int secs_elapsed,
                                        std::vector<ChangePoint> *change_list) {
  current_sec = secs_elapsed;
  UpdateSystemStats();
  auto current_score = ScoreTheSystem();
  if (tuning_rounds < locating_rounds || secs_elapsed < locating_before) {
    // locate the starting point according to a short time detecting.
    // In this phase, detect the starting position of the system.
    LocateByScore(current_score);
  }
  GenerateTuningOP(change_list);
  tuning_rounds++;
}
void DOTA_Tuner::LocateByScore(SystemScores &score) {
  std::cout << "First stage, locating the position" << std::endl;
}
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
  current_score.estimate_compaction_bytes /=
      current_opt.soft_pending_compaction_bytes_limit;

  // clean up
  flush_list_accessed = flush_result_length;
  compaction_list_accessed = compaction_result_length;

  return current_score;
}
void DOTA_Tuner::GenerateTuningOP(std::vector<ChangePoint> *change_list) {
  std::cout << "Start generating OPs" << std::endl;
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

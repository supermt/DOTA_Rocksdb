//
// Created by jinghuan on 8/24/21.
//

#include "rocksdb/utilities/DOTA_tuner.h"

#include <vector>

#include "rocksdb/utilities/report_agent.h"

namespace ROCKSDB_NAMESPACE {

DOTA_Tuner::~DOTA_Tuner() = default;
void DOTA_Tuner::DetectTuningOperations(
    int secs_elapsed, std::vector<ChangePoint> *change_list_ptr) {
  current_sec = secs_elapsed;
  //  UpdateSystemStats();
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
        //        else {
        //          return kLowFlush;
        //        }
      } else if (score.l0_num > 0.5) {
        // it's in the l0 stall
        return kL0Stall;
      }
    } else if (score.l0_num > 0.7) {
      // it's in the l0 stall
      return kL0Stall;
    } else if (score.estimate_compaction_bytes > 0.5) {
      return kPendingBytes;
    }
  } else if (score.compaction_idle_time > 2.5) {
    return kIdle;
  }
  return kGoodArea;
}

BatchSizeStallLevels DOTA_Tuner::LocateBatchStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    if (score.flush_speed_avg < max_scores.flush_speed_avg * 0.5) {
      if (score.active_size_ratio > 0.5 && score.immutable_number >= 1) {
        return kTinyMemtable;
      } else if (current_opt.max_background_jobs > 6 || score.l0_num > 0.9) {
        return kTinyMemtable;
      }
    }
  } else if (score.flush_numbers < max_scores.flush_numbers * 0.3) {
    return kOverFrequent;
  }

  return kStallFree;
};

SystemScores DOTA_Tuner::ScoreTheSystem() {
  UpdateSystemStats();
  SystemScores current_score;

  uint64_t total_mem_size = 0;
  uint64_t active_mem = 0;

  uint64_t duration_micros = 0;
  if (last_micros != 0) {
    duration_micros = env_->NowMicros() - last_micros;
  } else {
    duration_micros = env_->NowMicros() - start_micros;
  }

  running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
  running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);

  current_score.active_size_ratio =
      (double)active_mem / (double)current_opt.write_buffer_size;
  current_score.immutable_number =
      cfd->imm() == nullptr ? 0 : cfd->imm()->NumNotFlushed();

  std::vector<FlushMetrics> flush_metric_list;
  auto flush_result_length =
      running_db_->immutable_db_options().flush_stats->size();
  auto compaction_result_length =
      running_db_->immutable_db_options().job_stats->size();

  for (uint64_t i = flush_list_accessed; i < flush_result_length; i++) {
    auto temp = flush_list_from_opt_ptr->at(i);
    flush_metric_list.push_back(temp);
    current_score.flush_speed_avg += temp.write_out_bandwidth;
    current_score.disk_bandwidth += temp.total_bytes;
    current_score.flush_min =
        std::min(current_score.flush_min, temp.write_out_bandwidth);
    if (i > 1)
      current_score.flush_gap_time +=
          (temp.start_time - flush_list_from_opt_ptr->at(i - 1).start_time);

    //        (double)(current_opt.write_buffer_size >> 20) /
    //                                 (current_score.memtable_speed + 1);
    if (current_score.l0_num > temp.l0_files) {
      current_score.l0_num = temp.l0_files;
    }
  }

  int l0_compaction = 0;
  auto num_new_flushes = (flush_result_length - flush_list_accessed);
  assert(num_new_flushes >= 1);
  current_score.flush_numbers = num_new_flushes;

  while (total_mem_size < last_unflushed_bytes) {
    total_mem_size += current_opt.write_buffer_size;
  }
  current_score.memtable_speed += (total_mem_size - last_unflushed_bytes);

  current_score.memtable_speed /= duration_micros;  // MiB
  //  current_score.memtable_speed /= kMicrosInSecond;  // we use MiB to
  //  calculate

  uint64_t max_pending_bytes = 0;

  last_unflushed_bytes = total_mem_size;
  for (uint64_t i = compaction_list_accessed; i < compaction_result_length;
       i++) {
    auto temp = compaction_list_from_opt_ptr->at(i);
    if (temp.input_level == 0) {
      current_score.l0_drop_ratio += temp.drop_ratio;
      l0_compaction++;
    }
    if (temp.current_pending_bytes > max_pending_bytes) {
      max_pending_bytes = temp.current_pending_bytes;
    }
    current_score.disk_bandwidth += temp.total_bytes;
  }

  // flush_speed_avg,flush_speed_var,l0_drop_ratio
  if (num_new_flushes != 0) {
    auto avg_flush = current_score.flush_speed_avg / num_new_flushes;
    current_score.flush_speed_avg /= num_new_flushes;
    for (auto item : flush_metric_list) {
      current_score.flush_speed_var += (item.write_out_bandwidth - avg_flush) *
                                       (item.write_out_bandwidth - avg_flush);
    }
    current_score.flush_speed_var /= num_new_flushes;
    current_score.flush_gap_time /= (kMicrosInSecond * num_new_flushes);
  }

  if (l0_compaction != 0) {
    current_score.l0_drop_ratio /= l0_compaction;
  }
  // l0_num
  current_score.l0_num = (double)(vfs->NumLevelFiles(vfs->base_level())) /
                         current_opt.level0_slowdown_writes_trigger;
  current_score.l0_num = l0_compaction == 0 ? current_score.l0_num : 0;
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
    auto value = temp_entry.second;
    current_score.flush_idle_time += value;
  }
  temp = compaction_thread_idle_list.size();
  for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
    auto temp_entry = compaction_thread_idle_list[i];
    auto value = temp_entry.second;
    current_score.compaction_idle_time += value;
  }
  int flush_thread_num = current_opt.max_background_jobs / 4 + 1;
  current_score.flush_idle_time /= (double)(current_opt.max_background_jobs *
                                            flush_thread_num * duration_micros);
  // flush threads always get 1/4 of all
  current_score.compaction_idle_time /=
      (double)((current_opt.max_background_jobs - flush_thread_num) *
               duration_micros);

  // clean up
  flush_list_accessed = flush_result_length;
  compaction_list_accessed = compaction_result_length;
  last_compaction_thread_len = compaction_thread_idle_list.size();
  last_flush_thread_len = flush_thread_idle_list.size();

  last_micros = env_->NowMicros();
  return current_score;
}

void DOTA_Tuner::AdjustmentTuning(std::vector<ChangePoint> *change_list,
                                  SystemScores &score,
                                  ThreadStallLevels thread_levels,
                                  BatchSizeStallLevels batch_levels) {
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
      //    case kLowFlush:
      //      op.ThreadOp = kDouble;
      //      break;
    case kL0Stall:
      op.ThreadOp = kLinearIncrease;
      break;
    case kPendingBytes:
      op.ThreadOp = kLinearIncrease;
      break;
    case kGoodArea:
      op.ThreadOp = kKeep;
      break;
    case kIdle:
      op.ThreadOp = kHalf;
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
                                     int target_value) {
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
  ChangePoint L1_total_size;
  ChangePoint sst_size_cp;
  //  ChangePoint write_buffer_number;

  sst_size_cp.opt = sst_size;
  L1_total_size.opt = total_l1_size;
  // adjust the memtable size
  memtable_size_cp.db_width = false;
  memtable_size_cp.opt = memtable_size;

  //  int flush_slots = std::ceil((double)current_opt.max_background_jobs / 5);

  //  write_buffer_number.opt = memtable_number;
  //  write_buffer_number.db_width = false;
  //  uint64_t allowed_max_memtable_size;
  // adjust the number of memtable size, and controls the total size of Cm won't
  // exceed
  //  write_buffer_number.value = ToString(flush_slots + 1);
  //  allowed_max_memtable_size =
  //      ((max_memtable_size * default_opts.max_write_buffer_number) >> 20) /
  //      (flush_slots + 1);
  //
  //  allowed_max_memtable_size = allowed_max_memtable_size << 20;

  target_value = std::max(target_value, min_memtable_size);
  target_value = std::min(target_value, max_memtable_size);

  // SST sizes should be controlled to be the same as memtable size
  memtable_size_cp.value = ToString(target_value);
  sst_size_cp.value = ToString(target_value);

  // calculate the total size of L1
  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     target_value;

  L1_total_size.value = ToString(l1_size);
  sst_size_cp.db_width = false;
  L1_total_size.db_width = false;

  //  change_list->push_back(write_buffer_number);
  change_list->push_back(memtable_size_cp);
  change_list->push_back(L1_total_size);
  change_list->push_back(sst_size_cp);
}

void DOTA_Tuner::FillUpChangeList(std::vector<ChangePoint> *change_list,
                                  TuningOP op) {
  uint64_t current_thread_num = current_opt.max_background_jobs;
  uint64_t current_batch_size = current_opt.write_buffer_size;
  switch (op.BatchOp) {
    case kLinearIncrease:
      SetBatchSize(change_list,
                   current_batch_size += default_opts.write_buffer_size);
      break;
    case kHalf:
      SetBatchSize(change_list, current_batch_size /= 2);
      break;
    case kKeep:
      break;
  }
  switch (op.ThreadOp) {
    case kLinearIncrease:
      SetThreadNum(change_list, current_thread_num += 2);
      break;
    case kHalf:
      SetThreadNum(change_list, current_thread_num /= 2);
      break;
    case kKeep:
      break;
  }
}

DOTA_Tuner::DOTA_Tuner(const Options opt, DBImpl *running_db,
                       int64_t *last_report_op_ptr,
                       std::atomic<int64_t> *total_ops_done_ptr, Env *env,
                       uint64_t gap_sec)
    : default_opts(opt),
      tuning_rounds(0),
      running_db_(running_db),
      scores(),
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
      tuning_gap(gap_sec),
      start_micros(env->NowMicros()),
      core_num(running_db->immutable_db_options().core_number),
      max_memtable_size(running_db->immutable_db_options().max_memtable_size) {
  this->last_report_ptr = last_report_op_ptr;
  this->total_ops_done_ptr_ = total_ops_done_ptr;
}
void DOTA_Tuner::UpdateSystemStats(DBImpl *running_db) {
  current_opt = running_db->GetOptions();
  version = running_db->GetVersionSet()
                ->GetColumnFamilySet()
                ->GetDefault()
                ->current();
  cfd = version->cfd();
  vfs = version->storage_info();
}

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
  temp.compaction_idle_time =
      this->compaction_idle_time - a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time - a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time - a.flush_gap_time;
  temp.flush_numbers = this->flush_numbers - a.flush_numbers;

  return temp;
}

SystemScores SystemScores::operator+(const SystemScores &a) {
  SystemScores temp;
  temp.flush_numbers = this->flush_numbers + a.flush_numbers;
  temp.memtable_speed = this->memtable_speed + a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio + a.active_size_ratio;
  temp.immutable_number = this->immutable_number + a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg + a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var + a.flush_speed_var;
  temp.l0_num = this->l0_num + a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio + a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes + a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth + a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time + a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time + a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time + a.flush_gap_time;
  return temp;
}

SystemScores SystemScores::operator/(const int &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed / a;
  temp.active_size_ratio = this->active_size_ratio / a;
  temp.immutable_number = this->immutable_number / a;
  temp.l0_num = this->l0_num / a;
  temp.l0_drop_ratio = this->l0_drop_ratio / a;
  temp.estimate_compaction_bytes = this->estimate_compaction_bytes / a;
  temp.disk_bandwidth = this->disk_bandwidth / a;
  temp.compaction_idle_time = this->compaction_idle_time / a;
  temp.flush_idle_time = this->flush_idle_time / a;

  temp.flush_speed_avg = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_avg / this->flush_numbers;
  temp.flush_speed_var = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_var / this->flush_numbers;
  temp.flush_gap_time =
      this->flush_numbers == 0 ? 0 : this->flush_gap_time / this->flush_numbers;

  return temp;
}

FEAT_Tuner::~FEAT_Tuner() = default;

void FEAT_Tuner::DetectTuningOperations(int /*secs_elapsed*/,
                                        std::vector<ChangePoint> *change_list) {
  //   first, we tune only when the flushing speed is slower than before
  //  UpdateSystemStats();
  auto current_score = this->ScoreTheSystem();
  scores.push_back(current_score);
  if (scores.size() == 1) {
    return;
  }
  this->UpdateMaxScore(current_score);
  if (scores.size() >= (size_t)this->score_array_len) {
    // remove the first record
    scores.pop_front();
  }
  CalculateAvgScore();

  current_score_ = current_score;
  std::cout << "Memtable: " << current_score_.memtable_speed << "/"
            << avg_scores.memtable_speed << " / " << max_scores.memtable_speed
            << std::endl;

  std::cout << "Flush speed: " << current_score_.flush_speed_avg << "/"
            << avg_scores.flush_speed_avg << max_scores.flush_speed_avg
            << std::endl;

  std::cout << "Flush idle: " << current_score_.flush_idle_time << "/"
            << avg_scores.flush_idle_time << " ratio: "
            << current_score.flush_idle_time / avg_scores.flush_idle_time
            << std::endl;
  if (avg_scores.compaction_idle_time > 0) {
    std::cout << "Compaction idle: " << current_score_.compaction_idle_time
              << "/" << avg_scores.compaction_idle_time << "idle ratio: "
              << current_score.compaction_idle_time /
                     avg_scores.compaction_idle_time
              << std::endl;
  }

  // For FEAT 9, the flush speed is always larger than 0
  if (current_score_.memtable_speed <
      avg_scores.memtable_speed * TEA_slow_flush) {
    //<=avg_scores.memtable_speed * TEA_slow_flush) {
    TuningOP result{kKeep, kKeep};
    if (TEA_enable) {
      result = TuneByTEA();
    }
    if (FEA_enable) {
      TuningOP fea_result = TuneByFEA();
      result.BatchOp = fea_result.BatchOp;
    }
    FillUpChangeList(change_list, result);
  }
}

SystemScores FEAT_Tuner::normalize(SystemScores &origin_score) {
  return origin_score;
}

TuningOP FEAT_Tuner::TuneByTEA() {
  // the flushing speed is low.
  TuningOP result{kKeep, kLinearIncrease};
  if (current_score_.flush_min <= max_scores.flush_speed_avg * TEA_slow_flush ||
      avg_scores.flush_speed_avg <=
          max_scores.flush_speed_avg * TEA_slow_flush) {
    result.ThreadOp = kHalf;
    std::cout << "slow flush, decrease thread" << std::endl;
  }

  if (current_score_.compaction_idle_time / avg_scores.compaction_idle_time >
      idle_threshold) {
    result.ThreadOp = kHalf;
    std::cout << "idle threads, thread decrease" << std::endl;
  }

  if (current_score_.estimate_compaction_bytes >= 1 ||
      current_score_.l0_num >= 1) {
    result.ThreadOp = kLinearIncrease;
    std::cout << "lo/ro, increase thread" << std::endl;
  }

  //
  //  std::cout << current_score_.flush_speed_avg << "/"
  //            << avg_scores.flush_speed_avg << std::endl;

  return result;
}

TuningOP FEAT_Tuner::TuneByFEA() {
  TuningOP negative_protocol{kKeep, kKeep};

  if (current_score_.compaction_idle_time > idle_threshold) {
    negative_protocol.BatchOp = kHalf;
    std::cout << "idle threads, reduce batch" << std::endl;
  }

  if (current_score_.immutable_number >= 1) {
    negative_protocol.BatchOp = kLinearIncrease;
    std::cout << "slow flushing, increase" << std::endl;
  }

  if (current_score_.estimate_compaction_bytes >= 1 ||
      current_score_.l0_num >= 1) {
    negative_protocol.BatchOp = kLinearIncrease;
    std::cout << "lo/ro increase" << std::endl;
  }

  //  if (current_score_.flush_idle_time > idle_threshold * 0.2) {
  //    negative_protocol.BatchOp = kHalf;
  //    std::cout << "flush is idle, keep the threads" << std::endl;
  //  }
  return negative_protocol;
}
void FEAT_Tuner::CalculateAvgScore() {
  SystemScores result;
  for (auto score : scores) {
    result = result + score;
  }
  if (scores.size() > 0) result = result / scores.size();
  this->avg_scores = result;
}
FEAT_Tuner::FEAT_Tuner(const Options opt, DBImpl *running_db,
                       int64_t *last_report_op_ptr,
                       std::atomic<int64_t> *total_ops_done_ptr, Env *env,
                       int gap_sec, bool triggerTEA, bool triggerFEA,
                       int average_entry_size)
    : DOTA_Tuner(opt, running_db, last_report_op_ptr, total_ops_done_ptr, env,
                 gap_sec),
      TEA_enable(triggerTEA),
      FEA_enable(triggerFEA),
      current_stage(kSlowStart),
      entry_size(average_entry_size) {
  flush_list_from_opt_ptr =
      this->running_db_->immutable_db_options().flush_stats;

  std::cout << "Using FEAT tuner.\n FEA is "
            << (FEA_enable ? "triggered" : "NOT triggered") << std::endl;
  std::cout << "TEA is " << (TEA_enable ? "triggered" : "NOT triggered")
            << std::endl;
}

Status FEAT_Tuner::ApplyChangePoints(std::vector<ChangePoint> *points) {
  std::unordered_map<std::string, std::string> *new_cf_options;
  std::unordered_map<std::string, std::string> *new_db_options;
  new_cf_options = new std::unordered_map<std::string, std::string>();
  new_db_options = new std::unordered_map<std::string, std::string>();
  Status s;
  if (points->empty()) {
    return s.OK();
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options->emplace(point.opt, point.value);
    } else {
      new_cf_options->emplace(point.opt, point.value);
    }
  }
  points->clear();

  if (!new_db_options->empty()) {
    //    std::thread t();
    applying_changes = true;
    s = running_db_->SetDBOptions(*new_db_options);
    free(new_db_options);
    applying_changes = false;
    if (!s.ok()) {
      return s;
    }
  }
  if (!new_cf_options->empty()) {
    applying_changes = true;
    s = running_db_->SetOptions(*new_cf_options);
    if (!s.ok()) {
      return s;
    }
    free(new_cf_options);
    applying_changes = false;
  }
  return s.OK();
}
bool FEAT_Tuner::IsBusy() { return applying_changes; }

}  // namespace ROCKSDB_NAMESPACE

//
// Created by jinghuan on 5/24/21.
//

#include "rocksdb/utilities/report_agent.h"

#include "rocksdb/utilities/DOTA_tuner.h"

namespace ROCKSDB_NAMESPACE {
ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}
void ReporterAgent::InsertNewTuningPoints(ChangePoint point) {
  std::cout << "can't use change point @ " << point.change_timing
            << " Due to using default reporter" << std::endl;
};
void ReporterAgent::DetectAndTuning(int secs_elapsed) {}
Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = ToString(secs_elapsed) + "," +
                       ToString(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}

void ReporterAgentWithTuning::ApplyChangePoint(ChangePoint point) {
  //    //    db_.db->GetDBOptions();
  //    FLAGS_env->SleepForMicroseconds(point->change_timing * 1000000l);
  //    sleep(point->change_timing);
  std::unordered_map<std::string, std::string> new_options = {
      {point.opt, point.value}};
  Status s = running_db_->SetDBOptions(new_options);
  auto is_ok = s.ok() ? "suc" : "failed";
  std::cout << "Set " << point.opt + "=" + point.value + " " + is_ok
            << " after " << point.change_timing << " seconds running"
            << std::endl;
}

void ReporterAgentWithTuning::DetectChangesPoints(int sec_elapsed) {
  std::vector<ChangePoint> change_points;
  DetectInstantChanges(sec_elapsed,
                       &change_points);  // using TCP style detecting
  ApplyChangePointsInstantly(&change_points);
}

void ReporterAgentWithTuning::print_useless_thing(int secs_elapsed) {
  // clear the vector once we print out the message
  for (auto compaction_stat :
       *this->running_db_->immutable_db_options().job_stats.get()) {
    std::cout << "L0 overlapping ratio: " << compaction_stat.drop_ratio
              << std::endl;
    std::cout << "CPU Time ratio: " << compaction_stat.cpu_time_ratio
              << std::endl;
    std::cout << "Background Limits"
              << "compaction_job: " << compaction_stat.max_bg_compaction << ","
              << "flush_job: " << compaction_stat.max_bg_flush << std::endl;
    std::cout << "Read/Write bandwidth" << compaction_stat.read_in_bandwidth
              << "/" << compaction_stat.write_out_bandwidth << std::endl;
  }
  std::cout << this->running_db_->immutable_db_options().job_stats.get()->size()
            << " Compaction(s) triggered" << std::endl;
  std::cout << "Flushing thread idle time: " << std::endl;
  auto flush_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::HIGH);
  auto compaction_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::LOW);

  uint64_t temp = flush_thread_idle_list.size();
  for (uint64_t i = last_flush_thread_len; i < temp; i++) {
    std::cout << flush_thread_idle_list[i].first << " : "
              << flush_thread_idle_list[i].second << std::endl;
  }
  last_flush_thread_len = temp;

  std::cout << "Compaction thread idle time: " << std::endl;
  temp = compaction_thread_idle_list.size();
  for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
    std::cout << compaction_thread_idle_list[i].first << " : "
              << compaction_thread_idle_list[i].second << std::endl;
  }
  last_compaction_thread_len = temp;
}
void ReporterAgentWithTuning::PrepareOBMap() {
  string_to_attributes_map.clear();

  parameter_map.emplace("test", "write stall");
}
void ReporterAgentWithTuning::WriteBufferChanging(
    int secs_elapsed, Options& current_opt, Version* version,
    ColumnFamilyData* cfd, VersionStorageInfo* vfs,
    std::vector<ChangePoint>* results) {
  // the size of SST should have some kind of limitation
  const uint64_t upper_bound = 512 * 1024 * 1024;
  const uint64_t lower_bound = 32 * 1024 * 1024;
  ChangePoint write_buffer;
  uint64_t target_value = current_opt.write_buffer_size;
  CongestionStatus memtable_stats;

  if (cfd->imm()->NumNotFlushed() > 0) {
    // too many working threads, but why
    memtable_stats = kCongestion;
  }
  switch (memtable_stats) {
    case kCongestion:
      target_value *= 2;
      break;
    case kReachThreshold:
      target_value += options_when_boost.write_buffer_size;
      break;
    case kUnderThreshold:
      target_value *= 0.75;
      break;
    case kIgnore:
      return;
  }

  target_value = std::max(lower_bound, target_value);
  target_value = std::min(upper_bound, target_value);

  write_buffer.opt = memtable_size;
  write_buffer.value = ToString(target_value);
  // sync the size of L0-L1 size;
  ChangePoint sst_size;
  sst_size.opt = table_size;
  sst_size.value = ToString(target_value);
  ChangePoint L1_total_size;
  L1_total_size.opt = table_size;

  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     target_value;
  L1_total_size.value = ToString(l1_size);
  write_buffer.db_width = false;
  sst_size.db_width = false;
  L1_total_size.db_width = false;

  results->push_back(write_buffer);
  results->push_back(sst_size);
  results->push_back(L1_total_size);
}

void ReporterAgentWithTuning::AdjustThreadChangePoint(
    TuningOP stats, std::vector<ChangePoint>* results, Options& current_opt) {
  ChangePoint bg_threads;
  int thread_count = current_opt.max_background_jobs;
  switch (stats) {
    case kDouble:
      thread_count *= 2;
      break;
    case kLinear:
      thread_count++;
      break;
    case kDecrease:
      // when reach the congestion,
      thread_count /= 2;
      break;
    case kKeep:
      return;
  }
  if (thread_count > thread_num_upper_bound)
    thread_count = thread_num_upper_bound;
  if (thread_count < thread_num_lower_bound)
    thread_count = thread_num_lower_bound;

  if (thread_count == current_opt.max_background_jobs) {
    return;
  }
  bg_threads.opt = max_bg_jobs;
  bg_threads.value = ToString(thread_count);
  bg_threads.db_width = true;
  //  ChangePoint sub_compaction;
  //  sub_compaction.opt = max_subcompaction;
  //  sub_compaction.value = ToString(thread_count);
  //  bg_threads.db_width = true;

  results->push_back(bg_threads);
  //  results->push_back(sub_compaction);
}

void ReporterAgentWithTuning::DetectIdleThreads(
    int secs_elapsed, Options& current_opt, Version* version,
    ColumnFamilyData* cfd, VersionStorageInfo* vfs,
    std::vector<ChangePoint>* results) {
  TuningOP stats = kKeep;
  int thread_count = current_opt.max_background_jobs;
  const double ideal_idle_ratio = 0.2;
  const double ideal_overlapping = 0.2;

  std::vector<double> overlapping_ratios;

  int l0_files = vfs->NumLevelFiles(0);

  if (cfd->imm()->NumNotFlushed() > 1) {
    // too many working threads, but why
    stats = kDecrease;
    return AdjustThreadChangePoint(stats, results, current_opt);
  }

  if (l0_files > current_opt.level0_slowdown_writes_trigger) {
    stats = kDouble;
    return AdjustThreadChangePoint(stats, results, current_opt);
  }

  if (this->running_db_->immutable_db_options().job_stats->empty()) {
    //    return;  // there is no compaction, don't tune
  } else {
    for (auto compaction_stat :
         *this->running_db_->immutable_db_options().job_stats.get()) {
      overlapping_ratios.push_back(compaction_stat.drop_ratio);
    }
    double min_overlapping =
        *std::min_element(overlapping_ratios.begin(), overlapping_ratios.end());
    if (min_overlapping < ideal_overlapping) {
      stats = kDecrease;
      return AdjustThreadChangePoint(stats, results, current_opt);
    }
  }

  // in most case we don't care about flushing states
  //  auto idle_ratios = *env_->GetThreadPoolWaitingTime(Env::HIGH);
  auto idle_times = *env_->GetThreadPoolWaitingTime(Env::LOW);
  if (idle_times.empty()) {
    stats = kKeep;
  } else if (idle_times.size() <= (uint64_t)thread_count) {
    stats = kLinear;
  } else {
    uint64_t max_idle =
        std::max_element(idle_times.begin(), idle_times.end(), thread_idle_cmp)
            ->second;
    // for the detecting gap is the same, record here.
    uint64_t target_gap_micro =
        kMicrosInSecond * tuning_gap_secs_ * ideal_idle_ratio;
    if (max_idle >= target_gap_micro) {
      // the threads are too idle
      stats = kDecrease;
    } else {
      // work is too busy
      stats = kDouble;
    }
  }
  return AdjustThreadChangePoint(stats, results, current_opt);
}

void ReporterAgentWithTuning::DetectAndTuning(int secs_elapsed) {
  //  std::cout << "secs elapsed: " << secs_elapsed << std::endl;
  if (secs_elapsed % tuning_gap_secs_ == 0) {
    DetectChangesPoints(secs_elapsed);
    //    print_useless_thing(secs_elapsed);
    // clean up the compaction array
    this->running_db_->immutable_db_options().job_stats->clear();
    //    std::cout << "last metrics collect secs" << last_metrics_collect_secs
    //              << std::endl;
    last_metrics_collect_secs = secs_elapsed;
  }
  if (tuning_points.empty()) {
    return;
  } else {
    PopChangePoints(secs_elapsed);
  }
}

void ReporterAgentWithTuning::DetectInstantChanges(
    int secs_elapsed, std::vector<ChangePoint>* results) {
  auto opts = this->running_db_->GetOptions();
  Version* version = this->running_db_->GetVersionSet()
                         ->GetColumnFamilySet()
                         ->GetDefault()
                         ->current();
  ColumnFamilyData* cfd = version->cfd();
  VersionStorageInfo* vfs = version->storage_info();
  //  WriteBufferChanging(secs_elapsed, opts, version, cfd, vfs, results);
  //  DetectIdleThreads(secs_elapsed, opts, version, cfd, vfs, results);
  ScoreTuneAndVote(secs_elapsed, opts, version, cfd, vfs, results);
}
Status ReporterAgentWithTuning::ReportLine(int secs_elapsed,
                                           int total_ops_done_snapshot) {
  auto opt = this->running_db_->GetOptions();

  std::string report = ToString(secs_elapsed) + "," +
                       ToString(total_ops_done_snapshot - last_report_) + "," +
                       ToString(opt.write_buffer_size >> 20) + "," +
                       ToString(opt.max_background_jobs);
  auto s = report_file_->Append(report);
  return s;
}
void ReporterAgentWithTuning::ApplyChangePointsInstantly(
    std::vector<ChangePoint>* points) {
  std::unordered_map<std::string, std::string> new_cf_options;
  std::unordered_map<std::string, std::string> new_db_options;

  if (points->empty()) {
    return;
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options.emplace(point.opt, point.value);
    } else {
      new_cf_options.emplace(point.opt, point.value);
    }
  }
  points->clear();
  Status s;
  if (!new_db_options.empty()) {
    s = running_db_->SetDBOptions(new_db_options);
    if (!s.ok()) {
      std::cout << "Setting Failed due to " << s.ToString() << std::endl;
    }
  }
  if (!new_cf_options.empty()) {
    s = running_db_->SetOptions(new_cf_options);
    if (!s.ok()) {
      std::cout << "Setting Failed due to " << s.ToString() << std::endl;
    }
  }
}
ReporterAgentWithTuning::ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                                                 const std::string& fname,
                                                 uint64_t report_interval_secs,
                                                 uint64_t dota_tuning_gap_secs)
    : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()),
      options_when_boost(running_db->GetOptions()),
      test_tuner() {
  tuning_points = std::vector<ChangePoint>();
  tuning_points.clear();
  PrepareOBMap();
  std::cout << "using reporter agent with change points." << std::endl;
  if (running_db == nullptr) {
    std::cout << "Missing parameter db_ to apply changes" << std::endl;
    abort();
  } else {
    running_db_ = running_db;
  }
  std::unordered_map<std::string, std::string> test;
  //  test.emplace("max_bytes_for_level_multiplier_additional",
  //  "[10,1,1,1,1,1,1]");
  running_db->SetOptions(test);
  this->tuning_gap_secs_ = std::max(dota_tuning_gap_secs, report_interval_secs);
  this->last_metrics_collect_secs = 0;
  this->last_compaction_thread_len = 0;
  this->last_flush_thread_len = 0;
}

inline double average(std::vector<double>& v) {
  assert(!v.empty());
  return accumulate(v.begin(), v.end(), 0.0) / v.size();
}

void ReporterAgentWithTuning::ScoreTuneAndVote(
    int secs_elapsed, Options& current_opt, Version* version,
    ColumnFamilyData* cfd, VersionStorageInfo* vfs,
    std::vector<ChangePoint>* results) {
  static uint64_t compaction_queue_accessed = 0;
  static uint64_t flush_queue_accessed = 0;
  static uint64_t flush_idle_thread_counting = 0;
  static uint64_t compaction_idle_thread_counting = 0;

  double l0_file_score = (double)vfs->NumLevelFiles(0) /
                         (double)current_opt.level0_slowdown_writes_trigger;
  uint64_t total_mem_size = 1;
  uint64_t active_mem = 0;
  running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
  running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);

  double memtable_score = 0.01;

  double compaction_bytes_score = 0.01;

  auto& compaction_stats_vec = running_db_->immutable_db_options().job_stats;
  auto& flush_stats_vec = running_db_->immutable_db_options().flush_stats;

  for (uint64_t i = compaction_queue_accessed; i < compaction_stats_vec->size();
       i++) {
    double temp = (double)compaction_stats_vec->at(i).current_pending_bytes /
                  (double)current_opt.soft_pending_compaction_bytes_limit;
    if (temp > compaction_bytes_score) compaction_bytes_score = temp;
  }

  double flushing_score = 0.01;
  if (!flush_stats_vec->empty()) {
    double min_flushing_speed =
        flush_stats_vec->at(flush_queue_accessed).write_out_bandwidth;
    double max_flushing_ratio =
        flush_stats_vec->at(flush_queue_accessed).memtable_ratio;
    for (uint64_t i = flush_queue_accessed; i < flush_stats_vec->size(); i++) {
      double temp = flush_stats_vec->at(i).write_out_bandwidth;
      if (temp < min_flushing_speed) min_flushing_speed = temp;
      temp = flush_stats_vec->at(i).memtable_ratio;
      if (max_flushing_ratio > temp) max_flushing_ratio = temp;
    }

    double first_flush_bandwidth = flush_stats_vec->front().write_out_bandwidth;
    flushing_score = (min_flushing_speed / first_flush_bandwidth) - 0.5;
    memtable_score = max_flushing_ratio;
  }

  std::vector<std::pair<size_t, uint64_t>>* flush_idle_queue =
      env_->GetThreadPoolWaitingTime(Env::HIGH);
  auto compaction_idle_queue = env_->GetThreadPoolWaitingTime(Env::LOW);

  double thread_idle_score = 0.0;
  std::vector<std::pair<size_t, uint64_t>> idle_time;
  for (uint64_t i = flush_idle_thread_counting; i < flush_idle_queue->size();
       i++) {
    idle_time.push_back(flush_idle_queue->at(i));
  }
  for (uint64_t i = compaction_idle_thread_counting;
       i < compaction_idle_queue->size(); i++) {
    idle_time.push_back(compaction_idle_queue->at(i));
  }

  int thread_num = current_opt.max_background_jobs;
  if (idle_time.empty()) {
    thread_idle_score += 1.0 * thread_num;  // all threads are sleeping
  } else {
    for (auto idle : idle_time) {
      thread_idle_score +=
          (double)idle.second / (double)(tuning_gap_secs_ * kMicrosInSecond);
    }
  }

  thread_idle_score /= (double)thread_num;
  ThreadNumScore thread_scores = {l0_file_score, memtable_score,
                                  compaction_bytes_score, flushing_score,
                                  thread_idle_score};

  //  uint64_t read_latency = 0.0;
  //  double read_performance_score = 0.0;
  //  static PinnableSlice test_value;
  //  uint64_t start = env_->NowCPUNanos();
  //  running_db_->Get(ReadOptions(), running_db_->DefaultColumnFamily(),
  //  "test",
  //                   &test_value);
  //  uint64_t end = env_->NowCPUNanos();
  //  read_latency = end - start;
  //  std::string read_his;
  //  read_performance_score =
  //      read_latency / running_db_->GetProperty(SST_READ_MICROS, &read_his);

  BatchSizeScore batch_scores = {l0_file_score, memtable_score,
                                 compaction_bytes_score, thread_idle_score,
                                 0.0};

  std::cout << thread_scores.ToString() << std::endl;
  TuningOP thread_op = VoteForThread(thread_scores);
  TuningOP batch_op = VoteForMemtable(batch_scores);

  AdjustThreadChangePoint(thread_op, results, current_opt);
  AdjustBatchChangePoint(batch_op, results, current_opt);

  // before return, set the static variables
  compaction_queue_accessed += compaction_stats_vec->size();
  flush_queue_accessed += compaction_stats_vec->size();
  flush_idle_thread_counting = flush_idle_queue->size();
  compaction_idle_thread_counting = compaction_idle_queue->size();
  // std increasing means
}
ReporterAgentWithTuning::TuningOP ReporterAgentWithTuning::VoteForThread(
    ReporterAgentWithTuning::ThreadNumScore& scores) {
  // here we use a vote ensemble method to derive the operation;
  // the weights here will be evaluated  by the ANOVA method
  double weights[5] = {1.0, 1.0, 1.0, 1.0, 1.0};
  TuningOP temp_op = last_thread_op;
  double ensemble[4] = {};
  {  // decide the op based on l0 score
    if (scores.l0_stall >= 1)
      temp_op = kDouble;
    else if (last_thread_score.l0_stall < scores.l0_stall * 0.5)
      temp_op = kLinear;
    else
      temp_op = kKeep;
  }
  ensemble[temp_op] += (weights[0] * scores.l0_stall);
  temp_op = last_thread_op;
  {
    // although this is the reason that causes the problem, but it can't be
    // tuned
    //  decide the op based on memtable
    if (scores.memtable_stall >= 1) {
      temp_op = kDecrease;
    } else {
      temp_op = kLinear;
    }
  }
  ensemble[temp_op] += (weights[1] * scores.memtable_stall);
  {
    if (scores.pending_bytes_stall > 0.7) {
      temp_op = kDecrease;
    } else {
      temp_op = kLinear;
    }
  }
  ensemble[temp_op] += (weights[2] * scores.pending_bytes_stall);
  {
    if (scores.flushing_congestion > 0.3) {
      // flushing speed increases
      temp_op = kDouble;
    } else if (scores.flushing_congestion < 0) {
      // slower than 0.5 of base flushing speed
      temp_op = kLinear;
    } else {
      // performance is slower than before, but can still keep in a certain
      // range
      temp_op = kKeep;
    }
  }
  ensemble[temp_op] += (weights[3] * abs(scores.flushing_congestion));
  //  {
  //    if (scores.thread_idle >= 0.7) {
  //      temp_op = kDecrease;
  //    } else if (scores.thread_idle >= 0.3) {
  //      temp_op = kKeep;
  //    } else if (scores.thread_idle >= 0.1) {
  //      temp_op = kLinear;
  //    } else {
  //      temp_op = kDouble;
  //    }
  //  }
  //  ensemble[temp_op] += (weights[4] * scores.thread_idle);
  double biggest_score = 0;
  int vote_result = 0;
  std::cout << "thread op scores: ";
  for (int i = 0; i < kKeep + 1; i++) {
    std::cout << i << " : " << ensemble[i] << " ";
    if (biggest_score <= ensemble[i]) {
      biggest_score = ensemble[i];
      vote_result = i;
    }
  }
  temp_op = TuningOP(vote_result);

  std::cout << "ensemble op: " << temp_op;
  std::cout << std::endl;

  last_thread_op = temp_op;
  last_thread_score = scores;
  return temp_op;
}

ReporterAgentWithTuning::TuningOP ReporterAgentWithTuning::VoteForMemtable(
    BatchSizeScore& scores) {
  // here we use a vote ensemble method to derive the operation;
  // the weights here will be evaluated  by the ANOVA method
  double weights[5] = {1.0, 1.0, 1.0, 1.0, 1.0};
  TuningOP temp_op = last_thread_op;
  double ensemble[4] = {};
  {  // decide the op based on l0 score
    if (scores.l0_stall >= 1)
      temp_op = kDouble;
    else if (last_thread_score.l0_stall < scores.l0_stall)
      temp_op = kLinear;
    else
      temp_op = kKeep;
  }
  ensemble[temp_op] += (weights[0] * scores.l0_stall);
  temp_op = last_thread_op;
  {
    // decide the op based on memtable
    if (scores.memtable_stall >= 0.7) {
      temp_op = kDouble;
    } else {
      temp_op = kKeep;
    }
  }
  ensemble[temp_op] += (weights[1] * scores.memtable_stall);
  {
    if (scores.pending_bytes_stall > 0.7) {
      temp_op = kDecrease;
    } else {
      temp_op = kLinear;
    }
  }
  ensemble[temp_op] += (weights[2] * scores.pending_bytes_stall);
  {
    if (scores.flushing_congestion > last_thread_score.flushing_congestion) {
      // flushing speed increases
      temp_op = last_thread_op;
    } else if (scores.flushing_congestion < 0) {
      // slower than 0.5 of base flushing speed
      temp_op = kLinear;
    } else {
      // performance is slower than before, but can still keep in a certain
      // range
      temp_op = kKeep;
    }
  }
  ensemble[temp_op] += (weights[3] * abs(scores.flushing_congestion));
  double biggest_score = 0;
  int vote_result = 0;
  std::cout << "batch op scores: ";
  for (int i = 0; i < kKeep + 1; i++) {
    std::cout << i << " : " << ensemble[i] << " ";
    if (biggest_score <= ensemble[i]) {
      biggest_score = ensemble[i];
      vote_result = i;
    }
  }
  temp_op = TuningOP(vote_result);

  std::cout << "ensemble op: " << temp_op;
  std::cout << std::endl;

  last_batch_op = temp_op;
  last_batch_score = scores;
  return temp_op;
}
void ReporterAgentWithTuning::AdjustBatchChangePoint(
    ReporterAgentWithTuning::TuningOP stats, std::vector<ChangePoint>* results,
    Options& current_opt) {
  uint64_t target_value = current_opt.write_buffer_size;
  switch (stats) {
    case kDouble:
      target_value *= 2;
      break;
    case kLinear:
      target_value += options_when_boost.write_buffer_size;
      break;
    case kDecrease:
      target_value *= 0.5;
      break;
    case kKeep:
      return;
  }
  const static uint64_t lower_bound = 32 << 20;
  const static uint64_t upper_bound = 512 << 20;

  target_value = std::max(lower_bound, target_value);
  target_value = std::min(upper_bound, target_value);

  ChangePoint write_buffer;
  ChangePoint sst_size;

  write_buffer.opt = memtable_size;
  write_buffer.value = ToString(target_value);
  // sync the size of L0-L1 size;
  sst_size.opt = table_size;
  sst_size.value = ToString(target_value);
  ChangePoint L1_total_size;
  L1_total_size.opt = table_size;

  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     target_value;
  L1_total_size.value = ToString(l1_size);
  write_buffer.db_width = false;
  sst_size.db_width = false;
  L1_total_size.db_width = false;

  results->push_back(write_buffer);
  results->push_back(sst_size);
  results->push_back(L1_total_size);
}
void ReporterAgentWithTuning::PopChangePoints(int secs_elapsed) {
  for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
    if (it->change_timing <= secs_elapsed) {
      if (running_db_ != nullptr) {
        ApplyChangePoint(*it);
      }
      tuning_points.erase(it--);
    }
  }
}

void ReporterWithMoreDetails::DetectAndTuning(int secs_elapsed) {
  report_file_->Append(",");
  ReportLine(secs_elapsed, total_ops_done_);
  secs_elapsed++;
}
};  // namespace ROCKSDB_NAMESPACE

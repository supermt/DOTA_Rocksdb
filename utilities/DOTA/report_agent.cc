//
// Created by jinghuan on 5/24/21.
//

#include "report_agent.h"

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
    std::cout << "Some latency" << compaction_stat.io_stat.open_nanos
              << compaction_stat.io_stat.read_nanos
              << compaction_stat.io_stat.write_nanos
              << compaction_stat.io_stat.fsync_nanos
              << compaction_stat.io_stat.range_sync_nanos
              << compaction_stat.io_stat.allocate_nanos
              << compaction_stat.io_stat.cpu_read_nanos
              << compaction_stat.io_stat.cpu_write_nanos
              << compaction_stat.io_stat.prepare_write_nanos << std::endl;

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
    ThreadIdleStatus stats, std::vector<ChangePoint>* results, int thread_count,
    Options& current_opt) {
  ChangePoint bg_threads;
  switch (stats) {
    case kDouble:
      thread_count *= 2;
      thread_num_lower_bound++;
      thread_num_upper_bound *= 2;
      break;
    case kLinear:
      thread_count++;
      thread_num_upper_bound++;
      break;
    case kDecrease:
      // when reach the congestion,
      thread_count = thread_num_lower_bound;
      thread_num_upper_bound = (thread_num_upper_bound + 0.2) / 2;
      thread_num_lower_bound = (thread_num_lower_bound + 0.5) / 2;
      break;
    case kKeep:
      return;
  }

  if (thread_count > thread_num_upper_bound) {
    thread_num_upper_bound = thread_count;
  }
  thread_count = std::min(thread_num_upper_bound, thread_count);
  thread_count = std::max(thread_num_lower_bound, thread_count);

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
  ThreadIdleStatus stats = kKeep;
  int thread_count = current_opt.max_background_jobs;
  const double ideal_idle_ratio = 0.2;
  const double ideal_overlapping = 0.2;

  std::vector<double> overlapping_ratios;

  int l0_files = vfs->NumLevelFiles(0);

  if (cfd->imm()->NumNotFlushed() > 1) {
    // too many working threads, but why
    stats = kDecrease;
    return AdjustThreadChangePoint(stats, results, thread_count, current_opt);
  }

  if (l0_files > current_opt.level0_slowdown_writes_trigger) {
    stats = kDouble;
    return AdjustThreadChangePoint(stats, results, thread_count, current_opt);
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
      return AdjustThreadChangePoint(stats, results, thread_count, current_opt);
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
  return AdjustThreadChangePoint(stats, results, thread_count, current_opt);
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
  WriteBufferChanging(secs_elapsed, opts, version, cfd, vfs, results);
  DetectIdleThreads(secs_elapsed, opts, version, cfd, vfs, results);
}
Status ReporterAgentWithTuning::ReportLine(int secs_elapsed,
                                           int total_ops_done_snapshot) {
  auto opt = this->running_db_->GetOptions();
  auto temp = this->running_db_->GetBGJobLimits(opt.max_background_flushes,
                                                opt.max_background_compactions,
                                                opt.max_background_jobs, true);
  std::string report = ToString(secs_elapsed) + "," +
                       ToString(total_ops_done_snapshot - last_report_) + "," +
                       ToString(opt.write_buffer_size >> 20) + "," +
                       ToString(temp.max_compactions) + "+" +
                       ToString(temp.max_flushes);
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
      options_when_boost(running_db->GetOptions()) {
  tuning_points = std::vector<ChangePoint>();
  thread_num_lower_bound = 1;
  thread_num_upper_bound =
      std::max(options_when_boost.max_background_compactions, 2);
  tuning_points.clear();
  PrepareOBMap();
  std::cout << "using reporter agent with change points." << std::endl;
  if (running_db == nullptr) {
    std::cout << "Missing parameter db_ to apply changes" << std::endl;
    abort();
  } else {
    running_db_ = running_db;
  }
  this->tuning_gap_secs_ = std::max(dota_tuning_gap_secs, report_interval_secs);
  this->last_metrics_collect_secs = 0;
  this->last_compaction_thread_len = 0;
  this->last_flush_thread_len = 0;
}

};  // namespace ROCKSDB_NAMESPACE

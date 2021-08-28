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
void ReporterAgent::DetectAndTuning(int /*secs_elapsed*/) {}
Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = ToString(secs_elapsed) + "," +
                       ToString(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}

void ReporterAgentWithTuning::DetectChangesPoints(int sec_elapsed) {
  std::vector<ChangePoint> change_points;
  tuner.DetectTuningOperations(sec_elapsed, &change_points);
  ApplyChangePointsInstantly(&change_points);
}

// void ReporterAgentWithTuning::print_useless_thing(int secs_elapsed) {
//   // clear the vector once we print out the message
//   for (auto compaction_stat :
//        *this->running_db_->immutable_db_options().job_stats.get()) {
//     std::cout << "L0 overlapping ratio: " << compaction_stat.drop_ratio
//               << std::endl;
//     std::cout << "CPU Time ratio: " << compaction_stat.cpu_time_ratio
//               << std::endl;
//     std::cout << "Background Limits"
//               << "compaction_job: " << compaction_stat.max_bg_compaction <<
//               ","
//               << "flush_job: " << compaction_stat.max_bg_flush << std::endl;
//     std::cout << "Read/Write bandwidth" << compaction_stat.read_in_bandwidth
//               << "/" << compaction_stat.write_out_bandwidth << std::endl;
//   }
//   std::cout <<
//   this->running_db_->immutable_db_options().job_stats.get()->size()
//             << " Compaction(s) triggered" << std::endl;
//   std::cout << "Flushing thread idle time: " << std::endl;
//   auto flush_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::HIGH);
//   auto compaction_thread_idle_list =
//   *env_->GetThreadPoolWaitingTime(Env::LOW);
//
//   uint64_t temp = flush_thread_idle_list.size();
//   for (uint64_t i = last_flush_thread_len; i < temp; i++) {
//     std::cout << flush_thread_idle_list[i].first << " : "
//               << flush_thread_idle_list[i].second << std::endl;
//   }
//   last_flush_thread_len = temp;
//
//   std::cout << "Compaction thread idle time: " << std::endl;
//   temp = compaction_thread_idle_list.size();
//   for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
//     std::cout << compaction_thread_idle_list[i].first << " : "
//               << compaction_thread_idle_list[i].second << std::endl;
//   }
//   last_compaction_thread_len = temp;
// }

void ReporterAgentWithTuning::DetectAndTuning(int secs_elapsed) {
  if (secs_elapsed % tuning_gap_secs_ == 0) {
    DetectChangesPoints(secs_elapsed);
    //    this->running_db_->immutable_db_options().job_stats->clear();
    last_metrics_collect_secs = secs_elapsed;
  }
  if (tuning_points.empty() ||
      tuning_points.front().change_timing < secs_elapsed) {
    return;
  } else {
    PopChangePoints(secs_elapsed);
  }
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
      tuner(options_when_boost, running_db, &last_report_, &total_ops_done_,
            env, dota_tuning_gap_secs) {
  tuning_points = std::vector<ChangePoint>();
  tuning_points.clear();
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
  tuner.ResetTuner();
}

inline double average(std::vector<double>& v) {
  assert(!v.empty());
  return accumulate(v.begin(), v.end(), 0.0) / v.size();
}

void ReporterAgentWithTuning::PopChangePoints(int secs_elapsed) {
  std::vector<ChangePoint> valid_point;
  for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
    if (it->change_timing <= secs_elapsed) {
      if (running_db_ != nullptr) {
        valid_point.push_back(*it);
      }
      tuning_points.erase(it--);
    }
  }
  ApplyChangePointsInstantly(&valid_point);
}

void ReporterWithMoreDetails::DetectAndTuning(int secs_elapsed) {
  report_file_->Append(",");
  ReportLine(secs_elapsed, total_ops_done_);
  secs_elapsed++;
}
};  // namespace ROCKSDB_NAMESPACE

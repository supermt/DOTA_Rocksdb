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
void ReporterAgent::DetectAndTuning(int secs_elapsed) { secs_elapsed++; }

void ReporterAgentWithTuning::ApplyChangePoint(ChangePoint point) {
  //    //    db_.db->GetDBOptions();
  //    FLAGS_env->SleepForMicroseconds(point->change_timing * 1000000l);
  //    sleep(point->change_timing);
  std::unordered_map<std::string, std::string> new_options = {
      {point.opt, point.value}};
  Status s = running_db_->SetOptions(new_options);
  auto is_ok = s.ok() ? "suc" : "failed";
  std::cout << "Set " << point.opt + "=" + point.value + " " + is_ok
            << " after " << point.change_timing << " seconds running"
            << std::endl;
}
bool ReporterAgentWithTuning::reach_lsm_double_line(size_t sec_elapsed,
                                                    Options* opt) {
  int counter[history_lsm_shape] = {0};
  for (LSM_STATE shape : shape_list) {
    int max_level = 0;
    int max_score = -1;
    unsigned long len = shape.size();
    for (unsigned long i = 0; i < len; i++) {
      if (shape[i] > max_score) {
        max_score = shape[i];
        max_level = i;
        if (shape[i] == 0) break;  // there is no file in this level
      }
    }
    //      std::cout << "max level is " << max_level << std::endl;
    counter[max_level]++;
  }
  ChangePoint point;
  for (unsigned long i = 0; i < history_lsm_shape; i++) {
    if (counter[i] > threashold * history_lsm_shape) {
      //        std::cout << "Apply changes due to crowded level  " << i <<
      //        std::endl;
      // for in each detect window, it suppose to happen a lot of single level
      // compaction, but we need to start with level 2, since level 1 is much
      // to common
      if (i <= 1) {
        if (opt->write_buffer_size > 4 * default_memtable_size) {
          point.change_timing = sec_elapsed + history_lsm_shape / 10;
          point.opt = memtable_size;
          point.value = ToString(default_memtable_size);
          tuning_points.push_back(point);
          point.opt = table_size;
          tuning_points.push_back(point);
          report_file_->Append("," + point.opt + "," + point.value);
        }
      } else if (opt->write_buffer_size <= default_memtable_size * 8) {
        point.change_timing = sec_elapsed + history_lsm_shape / 10;
        point.opt = memtable_size;
        point.value = ToString(opt->write_buffer_size * 2);
        tuning_points.push_back(point);
        point.opt = table_size;
        tuning_points.push_back(point);
        report_file_->Append("," + point.opt + "," + point.value);
      }

      return true;
    }
  }
  return false;
}
void ReporterAgentWithTuning::Detect(int sec_elapsed) {
  DBImpl* dbfull = running_db_;
  VersionSet* test = dbfull->GetVersionSet();

  Options opt = running_db_->GetOptions();
  auto cfd = test->GetColumnFamilySet()->GetDefault();
  auto vstorage = cfd->current()->storage_info();
  LSM_STATE temp;
  for (int i = 0; i < vstorage->num_levels(); i++) {
    double score = vstorage->CompactionScore(i);
    temp.push_back(score);
  }
  if (shape_list.size() > history_lsm_shape) {
    shape_list.pop_front();
  }
  shape_list.push_back(temp);
  reach_lsm_double_line(sec_elapsed, &opt);

  ChangePoint testpoint;

  //    uint64_t size = version;
}

};  // namespace ROCKSDB_NAMESPACE

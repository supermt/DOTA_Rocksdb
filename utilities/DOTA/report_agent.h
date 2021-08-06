//
// Created by jinghuan on 5/24/21.
//

#ifndef ROCKSDB_REPORTER_H
#define ROCKSDB_REPORTER_H
#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

typedef std::vector<double> LSM_STATE;
class ReporterAgent;
struct ChangePoint;

class ReporterWithMoreDetails;
class ReporterAgentWithTuning;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
};

class ReporterAgent {
 private:
  std::string header_string_;

 public:
  static std::string Header() { return "secs_elapsed,interval_qps"; }

  ReporterAgent(Env* env, const std::string& fname,
                uint64_t report_interval_secs,
                std::string header_string = Header())
      : header_string_(header_string),
        env_(env),
        total_ops_done_(0),
        last_report_(0),
        report_interval_secs_(report_interval_secs),
        stop_(false) {
    auto s = env_->NewWritableFile(fname, &report_file_, EnvOptions());

    if (s.ok()) {
      s = report_file_->Append(header_string_ + "\n");
      //      std::cout << "opened report file" << std::endl;
    }
    if (s.ok()) {
      s = report_file_->Flush();
    }
    if (!s.ok()) {
      fprintf(stderr, "Can't open %s: %s\n", fname.c_str(),
              s.ToString().c_str());
      abort();
    }
    reporting_thread_ = port::Thread([&]() { SleepAndReport(); });
  }
  virtual ~ReporterAgent();

  // thread safe
  void ReportFinishedOps(int64_t num_ops) {
    total_ops_done_.fetch_add(num_ops);
  }

  virtual void InsertNewTuningPoints(ChangePoint point);

 protected:
  virtual void DetectAndTuning(int secs_elapsed);
  Env* env_;
  std::unique_ptr<WritableFile> report_file_;
  std::atomic<int64_t> total_ops_done_;
  int64_t last_report_;
  const uint64_t report_interval_secs_;
  ROCKSDB_NAMESPACE::port::Thread reporting_thread_;
  std::mutex mutex_;
  // will notify on stop
  std::condition_variable stop_cv_;
  bool stop_;
  uint64_t time_started;

  void SleepAndReport() {
    time_started = env_->NowMicros();
    while (true) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        if (stop_ ||
            stop_cv_.wait_for(lk, std::chrono::seconds(report_interval_secs_),
                              [&]() { return stop_; })) {
          // stopping
          break;
        }
        // else -> timeout, which means time for a report!
      }
      auto total_ops_done_snapshot = total_ops_done_.load();
      // round the seconds elapsed
      //      auto secs_elapsed = env_->NowMicros();
      auto secs_elapsed =
          (env_->NowMicros() - time_started + kMicrosInSecond / 2) /
          kMicrosInSecond;
      std::string report = ToString(secs_elapsed) + "," +
                           ToString(total_ops_done_snapshot - last_report_);

      auto s = report_file_->Append(report);
      this->DetectAndTuning(secs_elapsed);
      s = report_file_->Append("\n");
      if (s.ok()) {
        s = report_file_->Flush();
      }
      if (!s.ok()) {
        fprintf(stderr,
                "Can't write to report file (%s), stopping the reporting\n",
                s.ToString().c_str());
        break;
      }
      last_report_ = total_ops_done_snapshot;
    }
  }
};

class ReporterAgentWithTuning : public ReporterAgent {
 private:
  std::vector<ChangePoint> tuning_points;
  DBImpl* running_db_;
  uint64_t last_metrics_collect_secs;
  uint64_t last_compaction_thread_len;
  uint64_t last_flush_thread_len;
  std::string DOTAHeader() const {
    return "secs_elapsed,interval_qps,batch_size";
  }
  uint64_t tuning_gap_secs_;

 protected:
 public:
  ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs,
                          uint64_t dota_tuning_gap_secs = 1)
      : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()) {
    tuning_points = std::vector<ChangePoint>();
    tuning_points.clear();
    std::cout << "using reporter agent with change points." << std::endl;
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to apply changes" << std::endl;
      abort();
    } else {
      running_db_ = running_db;
    }
    this->tuning_gap_secs_ =
        std::max(dota_tuning_gap_secs, report_interval_secs);
    this->last_metrics_collect_secs = 0;
    this->last_compaction_thread_len = 0;
    this->last_flush_thread_len = 0;
  }

  const std::string memtable_size = "write_buffer_size";
  const std::string table_size = "target_file_size_base";
  const static unsigned long history_lsm_shape =
      10;  // Recorded history lsm shape, here we record 10 secs
  std::deque<LSM_STATE> shape_list;
  const size_t default_memtable_size = 64 << 20;

  const float threashold = 0.5;

  void ApplyChangePoint(ChangePoint point);
  void InsertNewTuningPoints(ChangePoint point) {
    tuning_points.push_back(point);
  }
  bool reach_lsm_double_line(size_t sec_elapsed, Options* opt);

  void Detect(int sec_elapsed);

  void PopChangePoints(int secs_elapsed) {
    for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
      if (it->change_timing <= secs_elapsed) {
        if (running_db_ != nullptr) {
          ApplyChangePoint(*it);
        }
        tuning_points.erase(it--);
      }
    }
  }

  void print_useless_thing();
  void DetectAndTuning(int secs_elapsed) {
    //    Detect(secs_elapsed);
    std::cout << "secs elapsed: " << secs_elapsed << std::endl;
    if (secs_elapsed % tuning_gap_secs_ == 0) {
      print_useless_thing();
      // clean up the compaction array
      this->running_db_->immutable_db_options().job_stats->clear();
      std::cout << "last metrics collect secs" << last_metrics_collect_secs
                << std::endl;
      last_metrics_collect_secs = secs_elapsed;
    }

    //    if (tuning_points.empty()) {
    //      return;
    //    }
    //    PopChangePoints(secs_elapsed);
  }
};  // end ReporterWithTuning
typedef ReporterAgent DOTAAgent;
class ReporterWithMoreDetails : public ReporterAgent {
 private:
  DBImpl* db_ptr;
  std::string detailed_header() {
    return ReporterAgent::Header() + ",num_compaction,num_flushes,lsm_shape";
  }

 public:
  ReporterWithMoreDetails(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs)
      : ReporterAgent(env, fname, report_interval_secs, detailed_header()) {
    if (running_db == nullptr) {
      std::cout << "Missing parameter db_ to record more details" << std::endl;
      abort();

    } else {
      db_ptr = reinterpret_cast<DBImpl*>(running_db);
    }
  }

  void RecordCompactionQueue() {
    //    auto compaction_queue_ptr = db_ptr->getCompactionQueue();
    int num_running_compactions = db_ptr->num_running_compactions();
    int num_running_flushes = db_ptr->num_running_flushes();

    report_file_->Append(ToString(num_running_compactions) + ",");
    report_file_->Append(ToString(num_running_flushes) + ",");
  }
  void RecordLSMShape() {
    auto vstorage = db_ptr->GetVersionSet()
                        ->GetColumnFamilySet()
                        ->GetDefault()
                        ->current()
                        ->storage_info();

    report_file_->Append("[");
    int i = 0;
    for (i = 0; i < vstorage->num_levels() - 1; i++) {
      int file_count = vstorage->NumLevelFiles(i);
      report_file_->Append(ToString(file_count) + ",");
    }
    report_file_->Append(ToString(vstorage->NumLevelFiles(i)) + "]");
  }

  void DetectAndTuning(int secs_elapsed) {
    report_file_->Append(",");
    RecordCompactionQueue();
    RecordLSMShape();
    secs_elapsed++;
  }
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_REPORTER_H

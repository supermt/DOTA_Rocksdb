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
#include "rocksdb/utilities/DOTA_tuner.h"
namespace ROCKSDB_NAMESPACE {

typedef std::vector<double> LSM_STATE;
class ReporterAgent;
struct ChangePoint;
class DOTA_Tuner;

struct SystemScores;

class ReporterWithMoreDetails;
class ReporterAgentWithTuning;

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
  virtual Status ReportLine(int secs_elapsed, int total_ops_done_snapshot);
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
      DetectAndTuning(secs_elapsed);
      auto s = this->ReportLine(secs_elapsed, total_ops_done_snapshot);
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
  const Options options_when_boost;
  uint64_t last_metrics_collect_secs;
  uint64_t last_compaction_thread_len;
  uint64_t last_flush_thread_len;
  std::map<std::string, void*> string_to_attributes_map;
  std::unique_ptr<DOTA_Tuner> tuner;
  static std::string DOTAHeader() {
    return "secs_elapsed,interval_qps,batch_size,thread_num";
  }
  uint64_t tuning_gap_secs_;
  std::map<std::string, std::string> parameter_map;
  std::map<std::string, int> baseline_map;
  const int thread_num_upper_bound = 12;
  const int thread_num_lower_bound = 2;

 public:
  const static unsigned long history_lsm_shape =
      10;  // Recorded history lsm shape, here we record 10 secs
  std::deque<LSM_STATE> shape_list;
  const size_t default_memtable_size = 64 << 20;
  const float threashold = 0.5;
  ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                          const std::string& fname,
                          uint64_t report_interval_secs,
                          uint64_t dota_tuning_gap_secs = 1);

  void ApplyChangePointsInstantly(std::vector<ChangePoint>* points);

  void DetectChangesPoints(int sec_elapsed);

  void PopChangePoints(int secs_elapsed);

  static bool thread_idle_cmp(std::pair<size_t, uint64_t> p1,
                              std::pair<size_t, uint64_t> p2) {
    return p1.second < p2.second;
  }

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override;
  void UseFEATTuner(bool FEA_enable);
  //  void print_useless_thing(int secs_elapsed);
  void DetectAndTuning(int secs_elapsed) override;
  enum CongestionStatus {
    kCongestion,
    kReachThreshold,
    kUnderThreshold,
    kIgnore
  };

  struct BatchSizeScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double read_amp_score;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " reading performance score: " << read_amp_score;
      return ss.str();
    }
    std::string Differences() { return "batch size"; }
  };
  struct ThreadNumScore {
    double l0_stall;
    double memtable_stall;
    double pending_bytes_stall;
    double flushing_congestion;
    double thread_idle;
    std::string ToString() {
      std::stringstream ss;
      ss << "l0 stall: " << l0_stall << " memtable stall: " << memtable_stall
         << " pending bytes: " << pending_bytes_stall
         << " flushing congestion: " << flushing_congestion
         << " thread_idle: " << thread_idle;
      return ss.str();
    }
  };

  TuningOP VoteForThread(ThreadNumScore& scores);
  TuningOP VoteForMemtable(BatchSizeScore& scores);

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

  void DetectAndTuning(int secs_elapsed) override;

  Status ReportLine(int secs_elapsed, int total_ops_done_snapshot) override {
    auto opt = this->db_ptr->GetOptions();
    std::string report = ToString(secs_elapsed) + "," +
                         ToString(total_ops_done_snapshot - last_report_) +
                         "," + ToString(opt.write_buffer_size >> 20) + "," +
                         ToString(opt.max_background_compactions) + "+" +
                         ToString(opt.max_background_flushes);
    auto s = report_file_->Append(report);
    return s;
  }
};

};  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_REPORTER_H

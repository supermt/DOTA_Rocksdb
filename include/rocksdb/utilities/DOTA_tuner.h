//
// Created by jinghuan on 8/24/21.
//

#ifndef ROCKSDB_DOTA_TUNER_H
#define ROCKSDB_DOTA_TUNER_H
#pragma once
#include <iostream>

namespace ROCKSDB_NAMESPACE {

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
  bool db_width;
};

class DOTA_Tuner {
  ChangePoint GenerateOp();

 public:
  DOTA_Tuner() { std::cout << " create DOTA Tuner " << std::endl; }
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_DOTA_TUNER_H

//
// Created by Shiping Yao on 2022/10/28.
//

#ifndef MINIDB_AGGREGATE_FUNC_H
#define MINIDB_AGGREGATE_FUNC_H

#include <vector>
#include "storage/common/field.h"


enum class AggrFuncType {
  COUNT,
  MIN,
  MAX,
  AVG
};
class AggrFunc {
public:
  RC eval();

private:
  Field* field_;
};

#endif  // MINIDB_AGGREGATE_FUNC_H

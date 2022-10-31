//
// Created by Shiping Yao on 2022/10/28.
//

#ifndef MINIDB_AGGREGATE_FUNC_H
#define MINIDB_AGGREGATE_FUNC_H

#include <vector>
#include "storage/common/field.h"
#include "sql/expr/tuple.h"


enum class AggrFuncType {
  COUNT,
  MIN,
  MAX,
  AVG
};
class AggrFunc {
public:
  AggrFunc(FieldMeta* fm, AggrFuncType aggr_func_type): aggr_meta_(fm), aggr_func_type_(aggr_func_type) {}
  RC eval(const TupleCell& input, TupleCell& output);
  FieldMeta* aggr_meta() {
    return aggr_meta_;
  }

private:
  AggrFuncType aggr_func_type_;
  FieldMeta* aggr_meta_;
};

#endif  // MINIDB_AGGREGATE_FUNC_H

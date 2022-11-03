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
  AggrFunc(Field field, AggrFuncType aggr_func_type): field_(field), aggr_func_type_(aggr_func_type) {}
  RC eval(const TupleCell& input, TupleCell& output);
  RC fetch(const TupleCell& input);
  void end();
  const FieldMeta* aggr_meta() {
    return field_.meta();
  }
  const Field* aggr_field() {
    return &field_;
  }
  const TupleCell& value() {
    return value_;
  }

private:
  AggrFuncType aggr_func_type_;
  Field field_;
  TupleCell value_;
  int count_ = 0;

  RC count_fetch(const TupleCell& input);
  void count_end();
};

#endif  // MINIDB_AGGREGATE_FUNC_H

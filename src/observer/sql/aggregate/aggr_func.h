//
// Created by Shiping Yao on 2022/10/28.
//

#ifndef MINIDB_AGGREGATE_FUNC_H
#define MINIDB_AGGREGATE_FUNC_H

#include <string>
#include <vector>
#include <iostream>

#include "storage/common/field.h"
#include "sql/expr/tuple.h"


enum class AggrFuncType {
  COUNT,
  MIN,
  MAX,
  AVG,
  SUM
};

class AggrFunc {
public:
  AggrFunc() = default;
  AggrFunc(Field field, AggrFuncType aggr_func_type): field_(field), aggr_func_type_(aggr_func_type) {
    func_name_ = get_aggr_func_alias();
    init_value();
  }

  AggrFunc(Field field, AggrFuncType aggr_func_type, bool is_count_star): field_(field), aggr_func_type_(aggr_func_type) {
    func_name_ = get_aggr_func_alias(is_count_star);
    init_value();
  }
  RC eval(const TupleCell& input, TupleCell& output);
  RC fetch(const TupleCell& input);
  void end();
  const FieldMeta* aggr_meta() const {
    return field_.meta();
  }
  const Field* aggr_field() const {
    return &field_;
  }
  const TupleCell& value() const {
    return value_;
  }
  AggrFuncType type() const {
    return aggr_func_type_;
  }
  const std::string& name() const;

private:
  AggrFuncType aggr_func_type_;
  Field field_;
  TupleCell value_;
  char value_t[4];
  int count_ = 0;
  std::string func_name_;

  std::string get_aggr_func_alias(bool is_count_star = false);
  void init_value();
  RC count_fetch(const TupleCell& input);
  void count_end();
  RC avg_fetch(const TupleCell& input);
  void avg_end();
  RC sum_fetch(const TupleCell& input);
  void sum_end();
  RC min_max_fetch(const TupleCell& input, bool min = true);
  void min_max_end();
};

#endif  // MINIDB_AGGREGATE_FUNC_H

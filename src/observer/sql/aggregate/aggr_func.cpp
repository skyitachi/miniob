//
// Created by Shiping Yao on 2022/10/28.
//

#include "aggr_func.h"
#include "common/lang/string.h"

RC AggrFunc::eval(const TupleCell &input, TupleCell &output)
{
  return RC::GENERIC_ERROR;
}

RC AggrFunc::fetch(const TupleCell &input)
{
  switch (aggr_func_type_) {
    case AggrFuncType::COUNT:
      return count_fetch(input);
    default:
      return RC::GENERIC_ERROR;
  }
  return RC::GENERIC_ERROR;
}

RC AggrFunc::count_fetch(const TupleCell &input)
{
  if (!common::is_blank(input.data())) {
    count_++;
  }
  return RC::SUCCESS;
}

void AggrFunc::count_end()
{
  value_.set_data(reinterpret_cast<char *>(&count_));
  value_.set_type(AttrType::INTS);
  value_.set_length(4);
}

void AggrFunc::end() {
  switch (aggr_func_type_) {
    case AggrFuncType::COUNT:
      count_end();
      break;
    default:
      break;
  }
}

const std::string& AggrFunc::name() const
{
  return func_name_;
}

std::string AggrFunc::get_aggr_func_alias(bool is_count_star) {
  std::string func_name;
  if (is_count_star) {
    func_name = "count(*)";
    return func_name;
  }
  switch (aggr_func_type_) {
    case AggrFuncType::COUNT:
      func_name = "count";
      break;
    case AggrFuncType::MAX:
      func_name = "max";
      break;
    case AggrFuncType::MIN:
      func_name = "min";
      break;
    case AggrFuncType::AVG:
      func_name = "avg";
      break;
    default:
      func_name = "unknown";
      break;
  }
  return func_name + "(" + field_.meta()->name() + ")";
}
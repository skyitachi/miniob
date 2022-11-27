//
// Created by Shiping Yao on 2022/10/28.
//

#include "aggr_func.h"
#include "common/lang/string.h"

static const int32_t d_int = 0;
static const float d_float = 0.0;

RC AggrFunc::eval(const TupleCell &input, TupleCell &output)
{
  return RC::GENERIC_ERROR;
}

RC AggrFunc::fetch(const TupleCell &input)
{
  switch (aggr_func_type_) {
    case AggrFuncType::COUNT:
      return count_fetch(input);
    case AggrFuncType::AVG:
      return avg_fetch(input);
    case AggrFuncType::SUM:
      return sum_fetch(input);
    case AggrFuncType::MIN:
      return min_max_fetch(input);
    case AggrFuncType::MAX:
      return min_max_fetch(input, false);
    default:
      return RC::GENERIC_ERROR;
  }
}

RC AggrFunc::count_fetch(const TupleCell &input)
{
  if (!common::is_blank(input.data())) {
    count_++;
  }
  return RC::SUCCESS;
}

RC AggrFunc::sum_fetch(const TupleCell &input)
{
  if (!common::is_blank(input.data())) {
    value_.add(input);
  }
  return RC::SUCCESS;
}

RC AggrFunc::avg_fetch(const TupleCell &input)
{
  if (!common::is_blank(input.data())) {
    value_.add(input);
    count_++;
  }
  return RC::SUCCESS;
}

RC AggrFunc::min_max_fetch(const TupleCell &input, bool min)
{
  if (!common::is_blank(input.data())) {
    if (count_++ == 0) {
      value_.copy(input);
      return RC::SUCCESS;
    }
    int ret = value_.compare(input);
    if (min && ret > 0) {
      value_.copy(input);
    } else if (!min && ret < 0) {
      value_.copy(input);
    }
  }
  return RC::SUCCESS;
}

void AggrFunc::min_max_end()
{

}
void AggrFunc::count_end()
{
  value_.set_data(reinterpret_cast<char *>(&count_));
  value_.set_type(AttrType::INTS);
  value_.set_length(4);
}
void AggrFunc::sum_end(){}

void AggrFunc::avg_end()
{
  value_.divide(count_);
}
void AggrFunc::end() {
  switch (aggr_func_type_) {
    case AggrFuncType::COUNT:
      return count_end();
    case AggrFuncType::SUM:
      return sum_end();
    case AggrFuncType::AVG:
      return avg_end();
    case AggrFuncType::MIN:
    case AggrFuncType::MAX:
      return min_max_end();
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
    case AggrFuncType::SUM:
      func_name = "sum";
      break;
    default:
      func_name = "unknown";
      break;
  }
  return func_name + "(" + field_.meta()->name() + ")";
}

void AggrFunc::init_value()
{
  value_.set_type(field_.attr_type());
  switch (value_.attr_type()) {
    case AttrType::INTS:
      memcpy(value_t, &d_int, sizeof(int32_t));
      value_.set_length(sizeof(int32_t));
      break;
    case AttrType::FLOATS:
      memcpy(value_t, &d_float, sizeof(float));
      value_.set_length(sizeof(float));
      break;
    default:
      break;
  }
  value_.set_data(value_t);
}
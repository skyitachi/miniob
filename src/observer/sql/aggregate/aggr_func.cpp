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
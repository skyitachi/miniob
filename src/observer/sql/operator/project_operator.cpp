/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/01.
//

#include "common/log/log.h"
#include "sql/operator/project_operator.h"
#include "storage/record/record.h"
#include "storage/common/table.h"

RC ProjectOperator::open()
{
  if (children_.size() != 1) {
    LOG_WARN("project operator must has 1 child");
    return RC::INTERNAL;
  }

  Operator *child = children_[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectOperator::next()
{
  if (aggr_funcs.empty()) {
    return children_[0]->next();
  }
  if (aggregated_) {
    return RC::RECORD_EOF;
  }
  while(children_[0]->next() == SUCCESS) {
    LOG_DEBUG("in the aggr func loop");
    auto* tuple = children_[0]->current_tuple();
    for(auto& aggr_func: aggr_funcs) {
      TupleCell input;
      auto rc = tuple->find_cell(*aggr_func.aggr_field(), input);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("cannot found field info in tuple");
        return rc;
      }
      aggr_func.fetch(input);
    }
    LOG_DEBUG("after cal the aggr func loop");
  }
  for(auto& aggr_func: aggr_funcs) {
    aggr_func.end();
  }
  aggregated_ = true;
  return RC::SUCCESS;
}

RC ProjectOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *ProjectOperator::current_tuple()
{
  LOG_DEBUG("in the current tuple");
  if (aggr_funcs.empty()) {
    tuple_.set_tuple(children_[0]->current_tuple());
    return &tuple_;
  }

  std::vector<TupleCell> value_cells;
  for(auto& aggr_func: aggr_funcs) {
    value_cells.push_back(aggr_func.value());
  }
  aggr_tuple_ = AggrTuple(std::move(value_cells));
  return &aggr_tuple_;
}

void ProjectOperator::add_projection(const Table *table, const FieldMeta *field_meta)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  TupleCellSpec *spec = new TupleCellSpec(new FieldExpr(table, field_meta));
  spec->set_alias(field_meta->name());
  tuple_.add_cell_spec(spec);
}

void ProjectOperator::add_aggr_func(AggrFunc aggr_func)
{
  aggr_funcs.push_back(aggr_func);
//  auto *aggr_meta = aggr_func.aggr_meta();
//  char** default_value;
//  make_default_value(aggr_meta->type(), *default_value);
//  TupleCell* tc = new TupleCell(const_cast<FieldMeta*>(aggr_meta), *default_value);
//  aggr_values_.push_back(tc);
}

RC ProjectOperator::tuple_cell_spec_at(int index, const TupleCellSpec *&spec) const
{
  return tuple_.cell_spec_at(index, spec);
}

RC ProjectOperator::make_default_value(AttrType attr_type, char *&dest)
{
  switch (attr_type) {
    case INTS: {
      dest = new char[4];
      int32_t value = 0;
      memcpy(dest, &value, 4);
      break;
    }
    case FLOATS: {
      dest = new char[4];
      float value = 0.0f;
      memcpy(dest, &value, 4);
      break;
    }
    default:
      return RC::UNIMPLENMENT;
  }
  return RC::SUCCESS;
}

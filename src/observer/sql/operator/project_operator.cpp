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

#include <iostream>
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
    auto* tuple = children_[0]->current_tuple();
    for(auto& aggr_func: aggr_funcs) {
      TupleCell input;
      LOG_DEBUG("aggr_func name: %s, aggr_field: %p", aggr_func->name().c_str(), aggr_func->aggr_field());
      auto rc = tuple->find_cell(*(aggr_func->aggr_field()), input);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("cannot found field info in tuple");
        return rc;
      }
      aggr_func->fetch(input);
    }
  }
  for(auto& aggr_func: aggr_funcs) {
    aggr_func->end();
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
  if (aggr_funcs.empty()) {
    tuple_.set_tuple(children_[0]->current_tuple());
    return &tuple_;
  }

  std::vector<TupleCell> value_cells;
  for(auto& aggr_func: aggr_funcs) {
    value_cells.push_back(aggr_func->value());
  }
  aggr_tuple_ = AggrTuple(std::move(value_cells));
  return &aggr_tuple_;
}

void ProjectOperator::add_projection(const Table *table, const FieldMeta *field_meta, int idx)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  // NOTE: 共用field_meta会有问题吗
  TupleCellSpec *spec = new TupleCellSpec(new FieldExpr(table, field_meta));
  auto it = aggr_map_.find(idx);
  if (it != aggr_map_.end()) {
    LOG_DEBUG("aggr func type: %d, found aggr name: %s, field_meta: %p",
        it->second->type(), it->second->name().c_str(), field_meta);
    spec->set_alias((it->second)->name().c_str());
  } else {
    spec->set_alias(field_meta->name());
  }
  tuple_.add_cell_spec(spec);
}

void ProjectOperator::add_aggr_func(std::shared_ptr<AggrFunc> aggr_func, int idx)
{
  LOG_DEBUG("aggr_func: %s, index: %d", aggr_func->name().c_str(), idx);
  aggr_funcs.push_back(aggr_func);
  // aggr_meta 可能是一个对应多个aggr_func
  aggr_map_.insert(std::make_pair(idx, aggr_func));
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
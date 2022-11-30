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

#pragma once

#include <unordered_map>

#include "sql/aggregate/aggr_func.h"
#include "sql/operator/operator.h"
#include "rc.h"

class ProjectOperator : public Operator
{
public:
  ProjectOperator()
  {}

  virtual ~ProjectOperator() = default;

  void add_projection(const Table *table, const FieldMeta *field);
  void add_aggr_func(AggrFunc *aggr_func);

  RC open() override;
  RC next() override;
  RC close() override;

  int tuple_cell_num() const
  {
    return tuple_.cell_num();
  }

  RC tuple_cell_spec_at(int index, const TupleCellSpec *&spec) const;

  Tuple * current_tuple() override;
private:
  ProjectTuple tuple_;
  AggrTuple aggr_tuple_;
  std::vector<AggrFunc*> aggr_funcs;
  std::vector<TupleCell*> aggr_values_;
  bool aggregated_ = false;
  std::unordered_map<const FieldMeta*, AggrFunc*> aggr_map_;

};

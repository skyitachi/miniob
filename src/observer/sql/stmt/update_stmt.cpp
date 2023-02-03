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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "storage/common/db.h"
#include "common/log/log.h"
#include "storage/common/table.h"
#include "util/util.h"

UpdateStmt::UpdateStmt(Table *table, const Updates& updates): table_(table), updates_(updates)
{}

RC UpdateStmt::create(Db *db, Updates &update, Stmt *&stmt)
{
  char* table_name = update.relation_name;
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  Table* table = db->find_table(update.relation_name);
  if (table == nullptr) {
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check attr_name
  auto* field_meta = table->table_meta().field(update.attribute_name);
  if (field_meta == nullptr) {
    LOG_WARN("cannot found %s field in table %s", update.attribute_name, table_name);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check value type
  if (update.value.type != field_meta->type()) {
    bool succ = typecast(&update.value, field_meta->type());
    if (!succ) {
      LOG_WARN("unexpect value type");
      return RC::INVALID_ARGUMENT;
    }
  }

  // check condition
  for (int i = 0; i < update.condition_num; i++) {
    Condition cond = update.conditions[i];
    RelAttr attr = cond.left_attr;
    Value attr_value = cond.left_value;
    if (cond.right_is_attr) {
      attr = cond.right_attr;
      attr_value = cond.right_value;
    }
    auto *cond_field_meta = table->table_meta().field(attr.attribute_name);
    if (cond_field_meta == nullptr) {
      LOG_WARN("unexpect condition attr %s in table %s", attr.attribute_name, table_name);
      return RC::INVALID_ARGUMENT;
    }
    if (!typecast(&attr_value, cond_field_meta->type())) {
      LOG_WARN("unexpect attr value type: %s", attr.attribute_name);
      return RC::INVALID_ARGUMENT;
    }
  }
  stmt = new UpdateStmt(table, update);

  return RC::SUCCESS;
}

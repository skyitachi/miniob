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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

static std::unordered_map<std::string, AggrFuncType> g_aggr_funcs_dict = {
    {"count", AggrFuncType::COUNT},
    {"sum", AggrFuncType::SUM},
    {"max", AggrFuncType::MAX},
    {"min", AggrFuncType::MIN},
    {"avg", AggrFuncType::AVG}
};

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

static void get_first_field(Table *table, std::vector<Field> &field_metas) {
  const TableMeta& table_meta = table->table_meta();
  field_metas.push_back(Field(table, table_meta.field(0)));
}

RC SelectStmt::create(Db *db, const Selects &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relation_num; i++) {
    const char *table_name = select_sql.relations[i];
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table*>(table_name, table));
  }
  
  // collect query fields in `select` statement
  std::vector<Field> query_fields;
  std::unordered_map<int, int> query_field_idx;
  // NOTE: 终于知道为啥是逆序的了。。。
  for (int i = select_sql.attr_num - 1; i >= 0; i--) {
    const RelAttr &relation_attr = select_sql.attributes[i];
    bool has_aggr_func = (select_sql.aggr_func_idx[i + 1] == i + 1);
    LOG_DEBUG("found %d attr has aggr func: %d, func_idx: %d, selects_ptr: %p", i, has_aggr_func, select_sql.aggr_func_idx[i], &select_sql);
    if (common::is_blank(relation_attr.relation_name) && 0 == strcmp(relation_attr.attribute_name, "*")) {
      if (has_aggr_func) {
        // NOTE: 不考虑join的情况
        if (tables.size() > 1) {
          LOG_WARN("count(*) cannot support table join");
          return RC::INVALID_ARGUMENT;
        }
        get_first_field(tables[0], query_fields);
        query_field_idx[i + 1] = query_fields.size();
      } else {
        for (Table *table : tables) {
          wildcard_fields(table, query_fields);
        }
      }

    } else if (!common::is_blank(relation_attr.relation_name)) { // TODO
      const char *table_name = relation_attr.relation_name;
      const char *field_name = relation_attr.attribute_name;

      if (0 == strcmp(table_name, "*")) {
        if (0 != strcmp(field_name, "*")) {
          LOG_WARN("invalid field name while table is *. attr=%s", field_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        for (Table *table : tables) {
          wildcard_fields(table, query_fields);
        }
      } else {
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = iter->second;
        if (0 == strcmp(field_name, "*")) {
          wildcard_fields(table, query_fields);
        } else {
          const FieldMeta *field_meta = table->table_meta().field(field_name);
          if (nullptr == field_meta) {
            LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

        query_fields.push_back(Field(table, field_meta));
        query_field_idx[i + 1] = query_fields.size();
        }
      }
    } else {
      if (tables.size() != 1) {
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table *table = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name);
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }

      query_fields.push_back(Field(table, field_meta));
      query_field_idx[i + 1] = query_fields.size();
    }
  }

  LOG_INFO("got %d tables in from stmt and %d fields in query stmt", tables.size(), query_fields.size());

  std::vector<std::shared_ptr<AggrFunc>> aggr_funcs;
  LOG_DEBUG("[select_stmt] got aggr_func count: %d, attr_count: %d", select_sql.aggr_num, select_sql.attr_num);
  for (int i = select_sql.aggr_num - 1; i >= 0; i--) {
    auto aggr_attr = select_sql.aggrs[i];
    int attr_idx = select_sql.aggr_func_idx[i + 1];
    int field_idx = query_field_idx[attr_idx];
    assert(attr_idx == i + 1 && field_idx != 0);
    const RelAttr &relation_attr = select_sql.attributes[attr_idx - 1];
    std::string func_name = aggr_attr.func_name;
    std::string field_name = relation_attr.attribute_name;
    if (field_name == "*" && func_name != "count") {
      return RC::INVALID_ARGUMENT;
    }

    LOG_DEBUG("[select_stmt] got aggr_func: %s, attr_name: %s, field_name: %s", aggr_attr.func_name, relation_attr.attribute_name,
        query_fields[field_idx - 1].field_name());
    auto it = g_aggr_funcs_dict.find(aggr_attr.func_name);
    if (it != g_aggr_funcs_dict.end()) {
      aggr_funcs.push_back(std::make_shared<AggrFunc>(query_fields[field_idx - 1], it->second,
                              strcmp(relation_attr.attribute_name, "*") == 0));
    } else {
      LOG_DEBUG("[select_stmt] unexpect aggr func name: %s", aggr_attr.func_name);
    }
  }


  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, default_table, &table_map,
           select_sql.conditions, select_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->tables_.swap(tables);
  select_stmt->query_fields_.swap(query_fields);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->aggr_func_.swap(aggr_funcs);
  stmt = select_stmt;
  return RC::SUCCESS;
}

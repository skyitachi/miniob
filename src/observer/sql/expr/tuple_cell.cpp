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
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"
#include "storage/common/field.h"
#include "common/log/log.h"
#include "util/comparator.h"
#include "util/util.h"

void TupleCell::to_string(std::ostream &os) const
{
  switch (attr_type_) {
  case INTS: {
    os << *(int *)data_;
  } break;
  case FLOATS: {
    float v = *(float *)data_;
    os << double2string(v);
  } break;
  case CHARS: {
    for (int i = 0; i < length_; i++) {
      if (data_[i] == '\0') {
        break;
      }
      os << data_[i];
    }
  } break;
  case DATES:
    os << timestamp2string(*reinterpret_cast<int*>(data_));
    break;
  default: {
    LOG_WARN("unsupported attr type: %d", attr_type_);
  } break;
  }
}

int TupleCell::compare(const TupleCell &other) const
{
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
    case INTS: return compare_int(this->data_, other.data_);
    case FLOATS: return compare_float(this->data_, other.data_);
    case CHARS: return compare_string(this->data_, this->length_, other.data_, other.length_);
    case DATES: return compare_int(this->data_, other.data_);
    default: {
      LOG_WARN("unsupported type: %d", this->attr_type_);
    }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = *(int *)data_;
    return compare_float(&this_data, other.data_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = *(int *)other.data_;
    return compare_float(data_, &other_data);
  } else if (this->attr_type_ == CHARS && other.attr_type_ == INTS) {
    int32_t new_v = std::atoi(this->data_);
    return compare_int(&new_v, other.data_);
  } else if (this->attr_type_ == CHARS && other.attr_type_ == FLOATS) {
    float new_v = static_cast<float>(std::atof(this->data_));
    return compare_float(&new_v, other.data_);
  } else if (this->attr_type_ == INTS && other.attr_type_ == CHARS) {
    int32_t new_v = std::atoi(other.data_);
    return compare_int(this->data_, &new_v);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == CHARS) {
    float new_v = static_cast<float>(std::atof(other.data_));
    return compare_float(this->data_, &new_v);
  }
  LOG_WARN("not supported");
  return -1; // TODO return rc?
}

RC TupleCell::add(const TupleCell &rhs)
{
  switch (rhs.attr_type()) {
    case AttrType::FLOATS: {
      float lv = *reinterpret_cast<float*>(data_);
      float rv = *reinterpret_cast<float*>(rhs.data_);
      float result = lv + rv;
      memcpy(data_, &result, sizeof(float));
      return RC::SUCCESS;
    }
    case AttrType::INTS: {
      int32_t lv = *reinterpret_cast<int32_t*>(data_);
      int32_t rv = *reinterpret_cast<int32_t*>(rhs.data_);
      int32_t result = lv + rv;
      memcpy(data_, &result, sizeof(int32_t));
      return RC::SUCCESS;
    }
    default:
      return RC::GENERIC_ERROR;
  }
}

RC TupleCell::divide(int32_t count)
{
  switch (attr_type_) {
    case AttrType::FLOATS: {
      float v = *reinterpret_cast<float*>(data_);
      float r = v / count;
      memcpy(data_, &r, sizeof(float));
      return RC::SUCCESS;
    }
    case AttrType::INTS: {
      int32_t v = *reinterpret_cast<int32_t*>(data_);
      float r = v * 1.0 / count;
      memcpy(data_, &r, sizeof(float));
      set_type(AttrType::FLOATS);
    }
    default:
      return RC::GENERIC_ERROR;
  }
}

RC TupleCell::copy(const TupleCell &other, std::string& memory)
{
  if (attr_type_ == AttrType::CHARS && other.attr_type_ != AttrType::CHARS) {
    LOG_ERROR("copy value unexpected type, target_type: %d, src_type: %d", attr_type_, other.attr_type_);
    return RC::GENERIC_ERROR;
  }
  if (attr_type_ == AttrType::CHARS) {
    memory.resize(other.length_);
    set_data(memory.data());
    memcpy(data_, other.data_, other.length_);
    set_length(other.length_);
    return RC::SUCCESS;
  }
  if (length_ == other.length_) {
    memcpy(data_, other.data_, other.length_);
  }
  return RC::SUCCESS;
}
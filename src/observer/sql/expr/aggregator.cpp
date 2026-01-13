/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value& value)
{
  // 根据 SQL 标准，SUM 应该忽略 NULL 值
  if (value.attr_type() == AttrType::UNDEFINED) {
    return RC::SUCCESS;  // 跳过 NULL 值
  }
  
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
    attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value& value)
{
  // 根据 SQL 标准，AVG 应该忽略 NULL 值
  if (value.attr_type() == AttrType::UNDEFINED) {
    return RC::SUCCESS;  // 跳过 NULL 值
  }
  
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    count_ = 1;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
    attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  count_++;
  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value& result)
{
  // 如果没有累积任何值（空输入），AVG 应该返回 NULL（UNDEFINED）
  if (value_.attr_type() == AttrType::UNDEFINED || count_ == 0) {
    result.set_type(AttrType::UNDEFINED);
    return RC::SUCCESS;
  }
  
  if (value_.attr_type() == AttrType::INTS) {
    float avg = static_cast<float>(value_.get_int()) / count_;
    result.set_type(AttrType::FLOATS);
    result.set_float(avg);
  }
  else if (value_.attr_type() == AttrType::FLOATS) {
    float avg = value_.get_float() / count_;
    result.set_type(AttrType::FLOATS);
    result.set_float(avg);
  }
  else {
    // 如果类型不是 INTS 或 FLOATS，返回 UNDEFINED
    result.set_type(AttrType::UNDEFINED);
  }

  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value& value)
{
  // COUNT(*) 的情况，value 总是 Value(1)，不会是 UNDEFINED
  // COUNT(column) 的情况，如果 value 是 UNDEFINED (NULL)，应该忽略
  // 由于 COUNT(*) 的 child 是 Value(1)，它永远不会是 UNDEFINED，所以这样处理是正确的
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_.set_type(AttrType::INTS);
    value_.set_int(0);
  }
  
  // 对于 COUNT(column)，如果值是 NULL，跳过计数
  if (value.attr_type() == AttrType::UNDEFINED) {
    return RC::SUCCESS;  // 跳过 NULL 值
  }

  value_.set_int(value_.get_int() + 1);
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value& result)
{
  // 如果没有累积任何值（空输入），COUNT 应该返回 0，而不是 NULL
  if (value_.attr_type() == AttrType::UNDEFINED) {
    result.set_type(AttrType::INTS);
    result.set_int(0);
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value& value)
{
  // 根据 SQL 标准，MAX 应该忽略 NULL 值
  if (value.attr_type() == AttrType::UNDEFINED) {
    return RC::SUCCESS;  // 跳过 NULL 值
  }
  
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
    attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  if (value.compare(value_) > 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value& value)
{
  // 根据 SQL 标准，MIN 应该忽略 NULL 值
  if (value.attr_type() == AttrType::UNDEFINED) {
    return RC::SUCCESS;  // 跳过 NULL 值
  }
  
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
    attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  if (value.compare(value_) < 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}
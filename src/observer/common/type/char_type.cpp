/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include "common/type/date_type.h"
#include "iostream"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(is_string_type(left.attr_type()) && is_string_type(right.attr_type()), "invalid type");
  // 对于固定长度的字符串，使用二进制比较可以正确处理多字段索引
  // compare_binary 使用 memcmp，可以正确处理包含 null 字节的二进制数据
  return common::compare_binary(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::CHARS: {
      result.set_string(val.value_.pointer_value_, val.length_);
      return RC::SUCCESS;
    }
    case AttrType::TEXTS: {
      result.set_string(val.value_.pointer_value_, val.length_);
      result.set_type(AttrType::TEXTS);
      return RC::SUCCESS;
    }
    case AttrType::DATES: {
      int days = 0;
      std::string s(val.value_.pointer_value_ ? val.value_.pointer_value_ : "");
      if (!s.empty() && (int)s.size() != val.length_) {
        s.assign(val.value_.pointer_value_, val.length_);
      }
      RC rc = DateType::parse(s, days);
      if (OB_FAIL(rc)) {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      std::cout << "Casting CHAR: " << s << " to DATE with days: " << days << std::endl;
      result.set_int(days);
      result.set_type(AttrType::DATES);
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS || type == AttrType::TEXTS) {
    return 0;
  }
  if (type == AttrType::DATES) {
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}
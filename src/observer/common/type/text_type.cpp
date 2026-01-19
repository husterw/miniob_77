#include "common/type/text_type.h"
#include "common/value.h"
#include "storage/common/column.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/lang/sstream.h"
#include <regex>

int TextType::compare(const Value &left, const Value &right) const
{
  ASSERT(is_string_type(left.attr_type()) && is_string_type(right.attr_type()), "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC TextType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC TextType::cast_to(const Value &val, AttrType type, Value &result) const
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
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int TextType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS || type == AttrType::TEXTS) {
    return 0;
  }
  return INT32_MAX;
}

RC TextType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}
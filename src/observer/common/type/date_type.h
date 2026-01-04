#pragma once

#include "common/type/data_type.h"

class DateType : public DataType
{
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() {}

  int compare(const Value& left, const Value& right) const override;
  int compare(const Column& left, const Column& right, int left_idx, int right_idx) const override;

  RC cast_to(const Value& val, AttrType type, Value& result) const override;
  int cast_cost(AttrType type) override;

  RC set_value_from_str(Value& val, const string& data) const override;
  RC to_string(const Value& val, string& result) const override;

  // 解析 YYYY-MM-DD 字符串为天数，非法返回 RC::SCHEMA_FIELD_TYPE_MISMATCH
  static RC parse(const string& text, int& days);
  // 判断日期是否合法
  static bool valid_date(int year, int month, int day);
  // 判断是否为闰年
  static bool leap_year(int year);

  // 将相对天数转换为具体的年月日
  static void days_to_date(int days, int& year, int& month, int& day);
};
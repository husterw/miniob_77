#include "common/type/date_type.h"
#include "common/value.h"
#include "storage/common/column.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/lang/sstream.h"
#include <regex>

static const int EPOCH_YEAR = 1970; // base year from 1970-01-01
static const int DAYS_PER_MONTH[2][12] = {
  // non-leap year
  { 31,28,31,30,31,30,31,31,30,31,30,31 },
  // leap year
  { 31,29,31,30,31,30,31,31,30,31,30,31 }
};

RC DateType::parse(const string& text, int& days) {
  int hy1 = -1, hy2 = -1;
  for (int i = 0; i < (int)text.size(); ++i)
    if (text[i] == '-')
      if (hy1 == -1) hy1 = i;
      else if (hy2 == -1) hy2 = i;
      else return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    else if (text[i] < '0' || text[i] > '9') {
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  if (hy1 <= 0 || hy2 <= hy1 + 1 || hy2 >= (int)text.size() - 1) return RC::SCHEMA_FIELD_TYPE_MISMATCH;

  string sy = text.substr(0, hy1);
  string sm = text.substr(hy1 + 1, hy2 - hy1 - 1);
  string sd = text.substr(hy2 + 1);
  if (sy.empty() || sm.empty() || sd.empty()) return RC::SCHEMA_FIELD_TYPE_MISMATCH;

  int year = stoi(sy);
  int month = stoi(sm);
  int day = stoi(sd);
  if (!valid_date(year, month, day)) return RC::SCHEMA_FIELD_TYPE_MISMATCH;

  int total = 0; // can be negative
  if (year >= EPOCH_YEAR)
    for (int y = EPOCH_YEAR; y < year; ++y)
      total += leap_year(y) ? 366 : 365;
  else
    for (int y = year; y < EPOCH_YEAR; ++y)
      total -= leap_year(y) ? 366 : 365;
  for (int m = 1; m < month; ++m)
    total += DAYS_PER_MONTH[leap_year(year)][m - 1];
  total += (day - 1);
  days = (int)total;
  return RC::SUCCESS;
}

bool DateType::valid_date(int year, int month, int day) {
  if (year < 1 || year > 9999) return false;
  if (month < 1 || month > 12) return false;
  return day >= 1 && day <= DAYS_PER_MONTH[leap_year(year)][month - 1];
}

bool DateType::leap_year(int year) {
  if (year % 400 == 0) return true;
  if (year % 100 == 0) return false;
  return year % 4 == 0;
}

void DateType::days_to_date(int days, int& year, int& month, int& day) {
  // convert days offset back to date
  year = EPOCH_YEAR;
  long long d = days;
  if (d >= 0) {
    while (true) {
      int year_days = leap_year(year) ? 366 : 365;
      if (d >= year_days) { d -= year_days; ++year; }
      else break;
    }
  }
  else {
    while (d < 0) {
      int prev_year = year - 1;
      int year_days = leap_year(prev_year) ? 366 : 365;
      d += year_days;
      year = prev_year;
    }
  }
  static const int mdays[12] = { 31,28,31,30,31,30,31,31,30,31,30,31 };
  month = 1;
  for (int i = 0; i < 12; ++i) {
    int dim = mdays[i];
    if (i == 1 && leap_year(year)) dim = 29;
    if (d >= dim) { d -= dim; ++month; }
    else break;
  }
  day = (int)d + 1;
}

int DateType::compare(const Value& left, const Value& right) const {
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not date");
  return common::compare_int(
    (void*)&left.value_.int_value_, (void*)&right.value_.int_value_);
}

int DateType::compare(const Column& left, const Column& right, int left_idx, int right_idx) const {
  ASSERT(left.attr_type() == AttrType::DATES, "left type is not date");
  ASSERT(right.attr_type() == AttrType::DATES, "right type is not date");
  return common::compare_int(
    (void*)&((int*)left.data())[left_idx],
    (void*)&((int*)right.data())[right_idx]);
}

RC DateType::cast_to(const Value& val, AttrType type, Value& result) const {
  switch (type) {
  case AttrType::INTS: {
    result.set_int(val.get_int()); return RC::SUCCESS;
  }
  case AttrType::FLOATS: {
    result.set_float(static_cast<float>(val.get_int())); return RC::SUCCESS;
  }
  case AttrType::CHARS: {
    string s;
    to_string(val, s);
    result.set_string(s.c_str());
    return RC::SUCCESS;
  }
  default:
    LOG_WARN("unsupported type %d", type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

int DateType::cast_cost(AttrType type) {
  if (type == AttrType::DATES) return 0;
  if (type == AttrType::INTS || type == AttrType::FLOATS) return 1;
  if (type == AttrType::CHARS) return 2; // 与 CHARS 之间更偏向 CHARS -> DATES 的方向
  return INT32_MAX;
}

RC DateType::to_string(const Value& val, string& result) const {
  int year, month, day;
  days_to_date(val.get_int(), year, month, day);
  char buf[13];
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
  result = buf;
  return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value& val, const string& data) const {
  int days;
  RC rc = parse(data, days);
  if (OB_FAIL(rc)) return rc;
  val.set_int(days);
  val.set_type(AttrType::DATES);
  return RC::SUCCESS;
}
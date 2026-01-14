#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

class Table;

class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, string field_name, Value value)
      : table_(table), field_name_(std::move(field_name)), value_(value) {}
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  OpType              get_op_type() const override { return OpType::LOGICALUPDATE; }

  Table *table() const { return table_; }
  const string &field_name() const { return field_name_; }
  const Value  &value() const { return value_; }

private:
  Table  *table_ = nullptr;
  string  field_name_;
  Value   value_;
};
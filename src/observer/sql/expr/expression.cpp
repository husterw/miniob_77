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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "sql/expr/subquery_expr.h"

using namespace std;

RC FieldExpr::get_value(const Tuple& tuple, Value& value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

bool FieldExpr::equal(const Expression& other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto& other_field_expr = static_cast<const FieldExpr&>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk& chunk, Column& column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  }
  else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression& other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto& other_value_expr = static_cast<const ValueExpr&>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple& tuple, Value& value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk& chunk, Column& column)
{
  column.init(value_, chunk.rows());
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{
}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value& value, Value& cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple& tuple, Value& result) const
{
  Value value;
  RC rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::get_column(Chunk& chunk, Column& column)
{
  Column child_column;
  RC rc = child_->get_column(chunk, child_column);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  column.init(cast_type_, child_column.attr_len());
  for (int i = 0; i < child_column.count(); ++i) {
    Value value = child_column.get_value(i);
    Value cast_value;
    rc = cast(value, cast_value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    column.append_value(cast_value);
  }
  return rc;
}

RC CastExpr::try_get_value(Value& result) const
{
  Value value;
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
  : comp_(comp), left_(std::move(left)), right_(std::move(right))
{
}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value& left, const Value& right, bool& result) const
{
  RC  rc = RC::SUCCESS;
  int cmp_result = left.compare(right);
  
  // 如果比较结果是 INT32_MAX，表示涉及NULL值，根据SQL标准，任何与NULL的比较都返回FALSE
  if (cmp_result == INT32_MAX) {
    result = false;
    return rc;
  }
  
  result = false;
  switch (comp_) {
  case EQUAL_TO: {
    result = (0 == cmp_result);
  } break;
  case LESS_EQUAL: {
    result = (cmp_result <= 0);
  } break;
  case NOT_EQUAL: {
    result = (cmp_result != 0);
  } break;
  case LESS_THAN: {
    result = (cmp_result < 0);
  } break;
  case GREAT_EQUAL: {
    result = (cmp_result >= 0);
  } break;
  case GREAT_THAN: {
    result = (cmp_result > 0);
  } break;
  default: {
    LOG_WARN("unsupported comparison. %d", comp_);
    rc = RC::INTERNAL;
  } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value& cell) const
{
  // 处理 IS NULL 和 IS NOT NULL
  if (comp_ == IS_NULL_OP || comp_ == IS_NOT_NULL_OP) {
    // 对于 IS NULL/IS NOT NULL，只需要检查左边表达式的值
    // 右边表达式不需要（IS NULL 的右边是 NULL 字面量）
    if (left_->type() == ExprType::VALUE) {
      ValueExpr* left_value_expr = static_cast<ValueExpr*>(left_.get());
      const Value& left_cell = left_value_expr->get_value();
      
      bool is_null = (left_cell.attr_type() == AttrType::UNDEFINED);
      if (comp_ == IS_NULL_OP) {
        cell.set_boolean(is_null);
      } else {  // IS_NOT_NULL_OP
        cell.set_boolean(!is_null);
      }
      return RC::SUCCESS;
    }
    // 如果左边不是字面量值，无法在编译时确定结果
    return RC::INVALID_ARGUMENT;
  }
  
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr* left_value_expr = static_cast<ValueExpr*>(left_.get());
    ValueExpr* right_value_expr = static_cast<ValueExpr*>(right_.get());
    const Value& left_cell = left_value_expr->get_value();
    const Value& right_cell = right_value_expr->get_value();

    bool value = false;
    RC   rc = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    }
    else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple& tuple, Value& value) const
{
  // 处理 IS NULL 和 IS NOT NULL
  if (comp_ == IS_NULL_OP || comp_ == IS_NOT_NULL_OP) {
    Value left_value;
    RC rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
    
    bool is_null = (left_value.attr_type() == AttrType::UNDEFINED);
    if (comp_ == IS_NULL_OP) {
      value.set_boolean(is_null);
    } else {  // IS_NOT_NULL_OP
      value.set_boolean(!is_null);
    }
    return RC::SUCCESS;
  }
  
  // 处理 IN/NOT IN 操作
  if (comp_ == IN_OP || comp_ == NOT_IN_OP) {
    Value left_value;
    RC rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }

    // 检查右表达式是否为子查询
    if (right_->type() == ExprType::SUBQUERY) {
      SubQueryExpr *subquery_expr = static_cast<SubQueryExpr *>(right_.get());
      vector<Value> subquery_values;
      // 传递外部查询的 tuple，以支持相关子查询
      rc = subquery_expr->execute(subquery_values, &tuple);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to execute subquery. rc=%s", strrc(rc));
        return rc;
      }

      // 检查 left_value 是否在 subquery_values 中
      bool found = false;
      bool has_null = false;
      for (const Value &v : subquery_values) {
        if (v.attr_type() == AttrType::UNDEFINED) {
          has_null = true;
          continue;
        }
        // 尝试比较，如果类型不兼容，尝试类型转换
        int cmp_result = left_value.compare(v);
        if (cmp_result == INT32_MAX) {
          // 类型不兼容，尝试类型转换后比较
          // 先尝试将 v 转换为 left_value 的类型
          Value converted_v;
          RC cast_rc = Value::cast_to(v, left_value.attr_type(), converted_v);
          if (cast_rc == RC::SUCCESS) {
            cmp_result = left_value.compare(converted_v);
          }
          // 如果第一次转换失败或仍然不兼容，尝试将 left_value 转换为 v 的类型
          if (cmp_result == INT32_MAX) {
            Value converted_left;
            cast_rc = Value::cast_to(left_value, v.attr_type(), converted_left);
            if (cast_rc == RC::SUCCESS) {
              cmp_result = converted_left.compare(v);
            }
          }
          // 如果类型转换和比较都失败，跳过这个值
          if (cmp_result == INT32_MAX) {
            LOG_WARN("cannot compare values: left_type=%d, right_type=%d", 
                     static_cast<int>(left_value.attr_type()), 
                     static_cast<int>(v.attr_type()));
            continue;
          }
        }
        if (cmp_result == 0) {
          found = true;
          break;
        }
      }

      bool result = false;
      if (comp_ == IN_OP) {
        result = found;
        // NOT IN 的特殊性：如果遇到 NULL，结果为 NULL (false)
        // 这里简化处理，如果有 NULL 且没找到匹配，返回 false
      } else {  // NOT_IN_OP
        if (has_null && !found) {
          result = false;  // NULL 存在时，NOT IN 结果为 false
        } else {
          result = !found;
        }
      }

      value.set_boolean(result);
      return RC::SUCCESS;
    } else {
      LOG_WARN("IN/NOT IN right operand should be a subquery");
      return RC::INVALID_ARGUMENT;
    }
  }

  // 处理普通比较操作
  Value left_value;
  Value right_value;

  RC rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  // 如果左表达式返回 NULL，SQL 标准：任何与 NULL 的比较都应该返回 NULL (false)
  if (left_value.attr_type() == AttrType::UNDEFINED) {
    value.set_boolean(false);
    return RC::SUCCESS;
  }

  // 检查右表达式是否为子查询
  if (right_->type() == ExprType::SUBQUERY) {
    SubQueryExpr *subquery_expr = static_cast<SubQueryExpr *>(right_.get());
    // 传递外部查询的 tuple，以支持相关子查询
    rc = subquery_expr->execute_single(right_value, &tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to execute subquery for single value. rc=%s", strrc(rc));
      return rc;
    }
    // 如果子查询返回空，right_value 可能是 NULL (UNDEFINED)
    // SQL 标准：任何与 NULL 的比较都应该返回 NULL (false)
    if (right_value.attr_type() == AttrType::UNDEFINED) {
      value.set_boolean(false);
      return RC::SUCCESS;
    }
  } else {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
    // 如果右表达式返回 NULL，也特殊处理
    if (right_value.attr_type() == AttrType::UNDEFINED) {
      value.set_boolean(false);
      return RC::SUCCESS;
    }
  }

  bool bool_value = false;
  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk& chunk, vector<uint8_t>& select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  }
  else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  }
  else if (left_column.attr_type() == AttrType::DATES) {
    rc = compare_column<int>(left_column, right_column, select);
  }
  else if (left_column.attr_type() == AttrType::CHARS) {
    int rows = 0;
    if (left_column.column_type() == Column::Type::CONSTANT_COLUMN) {
      rows = right_column.count();
    }
    else {
      rows = left_column.count();
    }
    for (int i = 0; i < rows; ++i) {
      Value left_val = left_column.get_value(i);
      Value right_val = right_column.get_value(i);
      bool        result = false;
      rc = compare_value(left_val, right_val, result);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
        return rc;
      }
      select[i] &= result ? 1 : 0;
    }

  }
  else {
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column& left, const Column& right, vector<uint8_t>& result) const
{
  RC rc = RC::SUCCESS;

  bool left_const = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T*)left.data(), (T*)right.data(), left.count(), result, comp_);
  }
  else if (left_const && !right_const) {
    compare_result<T, true, false>((T*)left.data(), (T*)right.data(), right.count(), result, comp_);
  }
  else if (!left_const && right_const) {
    compare_result<T, false, true>((T*)left.data(), (T*)right.data(), left.count(), result, comp_);
  }
  else {
    compare_result<T, false, false>((T*)left.data(), (T*)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>>& children)
  : conjunction_type_(type), children_(std::move(children))
{
}

RC ConjunctionExpr::get_value(const Tuple& tuple, Value& value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression>& expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression* left, Expression* right)
  : arithmetic_type_(type), left_(left), right_(right)
{
}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
  : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{
}

bool ArithmeticExpr::equal(const Expression& other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto& other_arith_expr = static_cast<const ArithmeticExpr&>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
    right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if ((left_->value_type() == AttrType::INTS) &&
    (right_->value_type() == AttrType::INTS) &&
    arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value& left_value, const Value& right_value, Value& value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
  case Type::ADD: {
    Value::add(left_value, right_value, value);
  } break;

  case Type::SUB: {
    Value::subtract(left_value, right_value, value);
  } break;

  case Type::MUL: {
    Value::multiply(left_value, right_value, value);
  } break;

  case Type::DIV: {
    Value::divide(left_value, right_value, value);
  } break;

  case Type::NEGATIVE: {
    Value::negative(left_value, value);
  } break;

  default: {
    rc = RC::INTERNAL;
    LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
  } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
  const Column& left, const Column& right, Column& result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
  case Type::ADD: {
    if (attr_type == AttrType::INTS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
        (int*)left.data(), (int*)right.data(), (int*)result.data(), result.capacity());
    }
    else if (attr_type == AttrType::FLOATS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
        (float*)left.data(), (float*)right.data(), (float*)result.data(), result.capacity());
    }
    else {
      rc = RC::UNIMPLEMENTED;
    }
  } break;
  case Type::SUB:
    if (attr_type == AttrType::INTS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
        (int*)left.data(), (int*)right.data(), (int*)result.data(), result.capacity());
    }
    else if (attr_type == AttrType::FLOATS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
        (float*)left.data(), (float*)right.data(), (float*)result.data(), result.capacity());
    }
    else {
      rc = RC::UNIMPLEMENTED;
    }
    break;
  case Type::MUL:
    if (attr_type == AttrType::INTS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
        (int*)left.data(), (int*)right.data(), (int*)result.data(), result.capacity());
    }
    else if (attr_type == AttrType::FLOATS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
        (float*)left.data(), (float*)right.data(), (float*)result.data(), result.capacity());
    }
    else {
      rc = RC::UNIMPLEMENTED;
    }
    break;
  case Type::DIV:
    if (attr_type == AttrType::INTS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
        (int*)left.data(), (int*)right.data(), (int*)result.data(), result.capacity());
    }
    else if (attr_type == AttrType::FLOATS) {
      binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
        (float*)left.data(), (float*)right.data(), (float*)result.data(), result.capacity());
    }
    else {
      rc = RC::UNIMPLEMENTED;
    }
    break;
  case Type::NEGATIVE:
    if (attr_type == AttrType::INTS) {
      unary_operator<LEFT_CONSTANT, int, NegateOperator>((int*)left.data(), (int*)result.data(), result.capacity());
    }
    else if (attr_type == AttrType::FLOATS) {
      unary_operator<LEFT_CONSTANT, float, NegateOperator>(
        (float*)left.data(), (float*)result.data(), result.capacity());
    }
    else {
      rc = RC::UNIMPLEMENTED;
    }
    break;
  default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple& tuple, Value& value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk& chunk, Column& column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column& left_column, const Column& right_column, Column& column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value& value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char* aggregate_name, Expression* child)
  : aggregate_name_(aggregate_name), child_(child)
{
}

UnboundAggregateExpr::UnboundAggregateExpr(const char* aggregate_name, unique_ptr<Expression> child)
  : aggregate_name_(aggregate_name), child_(std::move(child))
{
}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression* child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{
}

RC AggregateExpr::get_column(Chunk& chunk, Column& column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  }
  else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression& other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr& other_aggr_expr = static_cast<const AggregateExpr&>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
  case Type::SUM: {
    aggregator = make_unique<SumAggregator>();
    break;
  }
  case Type::COUNT: {
    aggregator = make_unique<CountAggregator>();
    break;
  }
  case Type::AVG: {
    aggregator = make_unique<AvgAggregator>();
    break;
  }
  case Type::MAX: {
    aggregator = make_unique<MaxAggregator>();
    break;
  }
  case Type::MIN: {
    aggregator = make_unique<MinAggregator>();
    break;
  }
  default: {
    ASSERT(false, "unsupported aggregate type");
    break;
  }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple& tuple, Value& value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char* type_str, AggregateExpr::Type& type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  }
  else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  }
  else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  }
  else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  }
  else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  }
  else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

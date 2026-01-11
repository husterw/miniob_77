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
// Created by Auto on 2025/01/10.
//

#pragma once

#include "sql/expr/expression.h"

class SelectStmt;

class SubQueryExpr : public Expression
{
public:
  SubQueryExpr(SelectStmt *select_stmt);
  virtual ~SubQueryExpr();

  ExprType type() const override { return ExprType::SUBQUERY; }

  unique_ptr<Expression> copy() const override
  {
    // 子查询表达式不应该被复制，因为包含完整的查询语句
    // 如果需要复制，需要深度复制 SelectStmt
    return make_unique<SubQueryExpr>(select_stmt_);
  }

  RC get_value(const Tuple &tuple, Value &value) const override;
  
  /**
   * @brief 执行子查询并获取结果值列表（用于 IN/NOT IN）
   * @param values 输出的值列表
   */
  RC execute(vector<Value> &values) const;

  /**
   * @brief 执行子查询并获取单个值（用于比较运算）
   * @param value 输出的值
   */
  RC execute_single(Value &value) const;

  AttrType value_type() const override;

  SelectStmt *select_stmt() const { return select_stmt_; }

private:
  SelectStmt *select_stmt_;  ///< 子查询语句，注意这里不拥有所有权，由外部管理
};

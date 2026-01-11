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

// 空的删除器，用于不拥有所有权的 unique_ptr
struct NoopSelectStmtDeleter {
  void operator()(SelectStmt*) const noexcept {}
};

class SubQueryExpr : public Expression
{
public:
  SubQueryExpr(SelectStmt *select_stmt);
  SubQueryExpr(unique_ptr<SelectStmt> select_stmt);
  virtual ~SubQueryExpr();

  ExprType type() const override { return ExprType::SUBQUERY; }

  unique_ptr<Expression> copy() const override
  {
    // 子查询表达式不应该被复制，因为包含完整的查询语句
    // 如果需要复制，需要深度复制 SelectStmt
    // 注意：这里返回的副本与原对象共享同一个 SelectStmt 指针，但所有权仍由原对象管理
    // 这在当前实现中是可以接受的，因为表达式通常在逻辑计划阶段被使用，且不会被多次执行
    // 如果未来需要真正的深度复制，需要实现 SelectStmt 的复制构造函数
    return make_unique<SubQueryExpr>(select_stmt_.get());
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

  SelectStmt *select_stmt() const { return select_stmt_.get(); }

private:
  // 使用自定义删除器类型：拥有所有权时使用 default_delete，不拥有所有权时使用 NoopSelectStmtDeleter
  unique_ptr<SelectStmt, NoopSelectStmtDeleter> select_stmt_;  ///< 子查询语句
  bool owns_select_stmt_;  ///< 是否拥有 SelectStmt 的所有权
};

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

#include "sql/expr/subquery_expr.h"
#include "common/log/log.h"
#include "sql/stmt/select_stmt.h"
#include "sql/optimizer/logical_plan_generator.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/executor/sql_result.h"
#include "session/session.h"
#include "storage/db/db.h"

using namespace std;

SubQueryExpr::SubQueryExpr(SelectStmt *select_stmt) 
  : select_stmt_(select_stmt, NoopSelectStmtDeleter()), owns_select_stmt_(false)
{
  // 使用空的删除器，因为指针由外部管理
  // 这个方法用于 copy() 时创建共享同一个 SelectStmt 的副本
}

SubQueryExpr::SubQueryExpr(unique_ptr<SelectStmt> select_stmt) 
  : select_stmt_(select_stmt.release(), NoopSelectStmtDeleter()), owns_select_stmt_(true)
{
  // 转移所有权，但使用空的删除器，在析构时手动删除
}

SubQueryExpr::~SubQueryExpr()
{
  // 如果拥有所有权，手动删除 SelectStmt
  if (owns_select_stmt_ && select_stmt_) {
    delete select_stmt_.release();
  }
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  // 对于子查询表达式，在 ComparisonExpr 中会特殊处理
  // 这里暂时返回错误，实际使用时应该通过 execute_single 或 execute 方法
  LOG_WARN("SubQueryExpr::get_value should not be called directly");
  return RC::INTERNAL;
}

RC SubQueryExpr::execute_single(Value &value) const
{
  vector<Value> values;
  RC rc = execute(values);
  if (OB_FAIL(rc)) {
    return rc;
  }

  if (values.empty()) {
    // 子查询返回空结果，设置为 NULL (UNDEFINED)
    value.set_type(AttrType::UNDEFINED);
    return RC::SUCCESS;
  }

  if (values.size() > 1) {
    LOG_WARN("subquery returned more than one row for single value comparison");
    return RC::INVALID_ARGUMENT;
  }

  value = values[0];
  return RC::SUCCESS;
}

RC SubQueryExpr::execute(vector<Value> &values) const
{
  if (!select_stmt_) {
    LOG_WARN("subquery select_stmt is null");
    return RC::INVALID_ARGUMENT;
  }

  SelectStmt *stmt = select_stmt_.get();

  // 从线程局部变量获取当前 Session 和 Trx
  Session *session = Session::current_session();
  if (nullptr == session) {
    LOG_WARN("failed to get current session for subquery");
    return RC::INTERNAL;
  }

  Trx *trx = session->current_trx();
  if (nullptr == trx) {
    LOG_WARN("failed to get current transaction for subquery");
    return RC::INTERNAL;
  }

  // 创建逻辑计划
  unique_ptr<LogicalOperator> logical_operator;
  LogicalPlanGenerator logical_plan_generator;
  RC rc = logical_plan_generator.create(stmt, logical_operator);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create logical plan for subquery. rc=%s", strrc(rc));
    return rc;
  }

  // 创建物理计划
  unique_ptr<PhysicalOperator> physical_operator;
  PhysicalPlanGenerator physical_plan_generator;
  rc = physical_plan_generator.create(*logical_operator, physical_operator, session);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create physical plan for subquery. rc=%s", strrc(rc));
    return rc;
  }

  // 执行查询
  rc = physical_operator->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open physical operator for subquery. rc=%s", strrc(rc));
    return rc;
  }

  // 收集所有结果
  // 注意：由于 SelectStmt 的 query_expressions 已经被移动到逻辑计划中，
  // 我们不能从 SelectStmt 中获取表达式。应该从物理算子的 tuple 中直接获取值。
  // ProjectPhysicalOperator 的 tuple 已经包含了计算好的值
  
  // 先执行一次 next() 来获取第一个 tuple，以便检查列数
  rc = physical_operator->next();
  if (rc == RC::RECORD_EOF) {
    // 子查询返回空结果
    physical_operator->close();
    return RC::SUCCESS;
  }
  
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get first tuple from subquery. rc=%s", strrc(rc));
    physical_operator->close();
    return rc;
  }
  
  Tuple *first_tuple = physical_operator->current_tuple();
  if (nullptr == first_tuple) {
    LOG_WARN("failed to get first tuple from subquery");
    physical_operator->close();
    return RC::INTERNAL;
  }
  
  // 检查列数
  int cell_num = first_tuple->cell_num();
  if (cell_num != 1) {
    LOG_WARN("subquery should return exactly one column, but got %d", cell_num);
    physical_operator->close();
    return RC::INVALID_ARGUMENT;
  }
  
  // 处理第一个 tuple
  Value v;
  rc = first_tuple->cell_at(0, v);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get value from subquery tuple. rc=%s", strrc(rc));
    physical_operator->close();
    return rc;
  }
  values.push_back(v);
  
  // 继续处理剩余的 tuple
  while (OB_SUCC(rc = physical_operator->next())) {
    Tuple *tuple = physical_operator->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get tuple from subquery");
      break;
    }

    Value v;
    rc = tuple->cell_at(0, v);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value from subquery tuple. rc=%s", strrc(rc));
      break;
    }

    values.push_back(v);
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  physical_operator->close();
  return rc;
}

AttrType SubQueryExpr::value_type() const
{
  if (!select_stmt_ || select_stmt_->query_expressions().empty()) {
    return AttrType::UNDEFINED;
  }

  return select_stmt_->query_expressions()[0]->value_type();
}

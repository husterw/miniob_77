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
#include "sql/expr/tuple.h"
#include "sql/expr/composite_tuple.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/index_scan_physical_operator.h"
#include <functional>
#include "sql/operator/logical_operator.h"

using namespace std;

SubQueryExpr::SubQueryExpr(SelectStmt *select_stmt) 
  : select_stmt_(select_stmt, NoopSelectStmtDeleter()), owns_select_stmt_(false),
    expected_column_count_(select_stmt && !select_stmt->query_expressions().empty() 
                           ? static_cast<int>(select_stmt->query_expressions().size()) : 0)
{
  // 使用空的删除器，因为指针由外部管理
  // 这个方法用于 copy() 时创建共享同一个 SelectStmt 的副本
  // 在构造时保存预期的列数和表达式副本，因为表达式可能在逻辑计划创建时被移动
  if (select_stmt && !select_stmt->query_expressions().empty()) {
    // 保存表达式副本，用于后续恢复
    // 确保保存的表达式数量与 expected_column_count_ 一致
    size_t expr_count = select_stmt->query_expressions().size();
    saved_expressions_.reserve(expr_count);
    for (const auto &expr : select_stmt->query_expressions()) {
      if (expr) {
        saved_expressions_.push_back(expr->copy());
      }
    }
    // 验证保存的表达式数量
    if (saved_expressions_.size() != expr_count) {
      LOG_WARN("saved expression count (%d) does not match SelectStmt expression count (%d). "
               "Some expressions may be null.", 
               static_cast<int>(saved_expressions_.size()), static_cast<int>(expr_count));
    }
  }
}

SubQueryExpr::SubQueryExpr(unique_ptr<SelectStmt> select_stmt) 
  : select_stmt_(nullptr, NoopSelectStmtDeleter()), 
    owns_select_stmt_(true),
    expected_column_count_(0)
{
  // 转移所有权，但使用空的删除器，在析构时手动删除
  // 在构造时保存预期的列数和表达式副本（在 release() 之前），因为表达式可能在逻辑计划创建时被移动
  if (select_stmt) {
    // 在 release() 之前先保存表达式数量和表达式副本
    size_t expr_count = select_stmt->query_expressions().size();
    if (expr_count == 0) {
      LOG_WARN("SelectStmt has no query_expressions when constructing SubQueryExpr. This may cause issues.");
      expected_column_count_ = 0;
    } else {
      expected_column_count_ = static_cast<int>(expr_count);
      saved_expressions_.reserve(expr_count);
      for (const auto &expr : select_stmt->query_expressions()) {
        if (expr) {
          saved_expressions_.push_back(expr->copy());
        }
      }
      // 验证保存的表达式数量
      if (saved_expressions_.empty()) {
        LOG_WARN("failed to save any expressions (all expressions were null). expected_column_count_=%d", 
                 expected_column_count_);
        expected_column_count_ = 0;
      } else if (saved_expressions_.size() != expr_count) {
        LOG_WARN("saved expression count (%d) does not match SelectStmt expression count (%d). "
                 "Some expressions may be null. Using saved count as expected_column_count_.", 
                 static_cast<int>(saved_expressions_.size()), static_cast<int>(expr_count));
        // 如果保存的表达式数量不同，使用实际保存的数量
        expected_column_count_ = static_cast<int>(saved_expressions_.size());
      }
    }
    select_stmt_.reset(select_stmt.release());
  }
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

RC SubQueryExpr::execute_single(Value &value, const Tuple *outer_tuple) const
{
  vector<Value> values;
  RC rc = execute(values, outer_tuple);
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

RC SubQueryExpr::execute(vector<Value> &values, const Tuple *outer_tuple) const
{
  if (!select_stmt_) {
    LOG_WARN("subquery select_stmt is null");
    return RC::INVALID_ARGUMENT;
  }

  SelectStmt *stmt = select_stmt_.get();

  // 使用构造时保存的预期列数（因为表达式可能已经被移动到逻辑计划中）
  // 对于标量子查询，应该只返回一列
  if (expected_column_count_ <= 0) {
    LOG_WARN("subquery has invalid expected column count: %d", expected_column_count_);
    return RC::INVALID_ARGUMENT;
  }

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

  // 每次执行都需要创建新的逻辑计划和物理计划
  // 因为物理计划生成器会移动逻辑计划的表达式，所以逻辑计划不能被重用
  
  // 检查 SelectStmt 的表达式是否已经为空（可能已经被移动到之前的逻辑计划中）
  bool expressions_empty = stmt->query_expressions().empty();
  
  // 如果表达式已经被移动，从保存的副本中恢复
  if (expressions_empty && !saved_expressions_.empty()) {
    LOG_TRACE("restoring expressions for subquery. saved_expressions_.size()=%d, expected_column_count_=%d",
              saved_expressions_.size(), expected_column_count_);
    restore_expressions();
    expressions_empty = stmt->query_expressions().empty();
  }
  
  if (expressions_empty) {
    // 表达式已经被移动，且无法恢复
    LOG_WARN("cannot create logical plan: SelectStmt expressions are already empty and cannot be restored. expected_column_count_=%d, saved_expressions_.size()=%d", 
             expected_column_count_, saved_expressions_.size());
    return RC::INTERNAL;
  }
  
  // 验证恢复后的表达式数量是否与预期一致
  int actual_expr_count = static_cast<int>(stmt->query_expressions().size());
  if (actual_expr_count == 0) {
    LOG_WARN("SelectStmt has no expressions after restore. expected_column_count_=%d, saved_expressions_.size()=%d",
             expected_column_count_, static_cast<int>(saved_expressions_.size()));
    return RC::INTERNAL;
  }
  
  if (actual_expr_count != expected_column_count_ && expected_column_count_ > 0) {
    LOG_WARN("restored expression count (%d) does not match expected count (%d). This may cause issues.",
             actual_expr_count, expected_column_count_);
    // 如果实际表达式数量大于预期，只使用前expected_column_count_个表达式
    if (actual_expr_count > expected_column_count_) {
      LOG_TRACE("trimming expressions from %d to %d", actual_expr_count, expected_column_count_);
      stmt->query_expressions().resize(expected_column_count_);
    }
  }
  
  // 创建新的逻辑计划（每次执行都创建，因为物理计划生成器会移动表达式）
  unique_ptr<LogicalOperator> logical_operator;
  int expr_count_before = static_cast<int>(stmt->query_expressions().size());
  LOG_TRACE("creating logical plan for subquery. expected_column_count_=%d, stmt->query_expressions().size()=%d", 
            expected_column_count_, expr_count_before);
  
  LogicalPlanGenerator logical_plan_generator;
  RC rc = logical_plan_generator.create(stmt, logical_operator);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create logical plan for subquery. rc=%s", strrc(rc));
    return rc;
  }
  
  // 创建逻辑计划后，表达式应该已经被移动到逻辑计划中
  int expr_count_after = static_cast<int>(stmt->query_expressions().size());
  LOG_TRACE("logical plan created for subquery. expr_count_before=%d, expr_count_after=%d", 
            expr_count_before, expr_count_after);
  
  // 验证逻辑计划是否创建成功
  if (!logical_operator) {
    LOG_WARN("logical plan is null after creation");
    return RC::INTERNAL;
  }

  // 创建物理计划（每次执行都创建新的物理计划）
  unique_ptr<PhysicalOperator> physical_operator;
  PhysicalPlanGenerator physical_plan_generator;
  rc = physical_plan_generator.create(*logical_operator, physical_operator, session);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create physical plan for subquery. rc=%s", strrc(rc));
    return rc;
  }

  // 如果有外部查询的 tuple，设置到所有的 TableScanPhysicalOperator 中
  if (outer_tuple != nullptr) {
    function<void(PhysicalOperator*)> set_outer_tuple_recursive = 
      [&outer_tuple, &set_outer_tuple_recursive](PhysicalOperator* op) {
        if (op == nullptr) return;
        
        // 如果是 TableScanPhysicalOperator 或 IndexScanPhysicalOperator，设置外部查询的 tuple
        if (op->type() == PhysicalOperatorType::TABLE_SCAN) {
          TableScanPhysicalOperator* table_scan = 
            static_cast<TableScanPhysicalOperator*>(op);
          table_scan->set_outer_tuple(outer_tuple);
        } else if (op->type() == PhysicalOperatorType::INDEX_SCAN) {
          IndexScanPhysicalOperator* index_scan = 
            static_cast<IndexScanPhysicalOperator*>(op);
          index_scan->set_outer_tuple(outer_tuple);
        }
        
        // 递归处理子操作符
        for (auto& child : op->children()) {
          set_outer_tuple_recursive(child.get());
        }
      };
    
    set_outer_tuple_recursive(physical_operator.get());
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
  // 我们已经在构造时保存了 expected_column_count_，应该使用它来验证列数
  // 直接执行查询并收集结果，假设列数为 expected_column_count_（应该是1）
  
  // 先尝试获取第一个tuple，检查是否有数据
  rc = physical_operator->next();
  if (rc == RC::RECORD_EOF) {
    // 子查询返回空结果集，这是允许的（对于标量子查询会返回NULL）
    LOG_TRACE("subquery returned empty result set");
    physical_operator->close();
    return RC::SUCCESS;
  }
  
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get first tuple from subquery. rc=%s", strrc(rc));
    physical_operator->close();
    return rc;
  }
  
  // 收集所有结果
  do {
    Tuple *tuple = physical_operator->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get tuple from subquery");
      break;
    }

    // 获取第一列的值（子查询只返回一列）
    int tuple_cell_num = tuple->cell_num();
    if (tuple_cell_num == 0) {
      LOG_WARN("subquery tuple has 0 cells, cannot get value. expected_column_count_=%d, physical_operator_type=%d", 
               expected_column_count_, static_cast<int>(physical_operator->type()));
      rc = RC::INVALID_ARGUMENT;
      break;
    }
    
    // 对于标量子查询，只取第一列的值
    // 即使实际返回了多列，我们也只取第一列
    Value v;
    rc = tuple->cell_at(0, v);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value from subquery tuple. cell_num=%d, expected_column_count_=%d, rc=%s", 
               tuple_cell_num, expected_column_count_, strrc(rc));
      break;
    }

    values.push_back(v);
    
    // 如果是标量子查询（expected_column_count_ == 1），但实际返回了多列，记录警告但继续
    if (expected_column_count_ == 1 && tuple_cell_num > 1) {
      LOG_WARN("subquery returned %d columns but expected 1. Using first column only.", tuple_cell_num);
    }
    
    // 获取下一个tuple
    rc = physical_operator->next();
  } while (rc == RC::SUCCESS);

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  physical_operator->close();
  return rc;
}

AttrType SubQueryExpr::value_type() const
{
  // 如果表达式已经被移动，使用保存的预期列数来判断
  // 但 value_type 需要从表达式中获取，所以如果表达式已经被移动，返回 UNDEFINED
  // 理想情况下，我们也应该保存 value_type，但为了简化，这里先这样处理
  if (!select_stmt_) {
    return AttrType::UNDEFINED;
  }
  
  // 尝试从 SelectStmt 获取，如果已经被移动，表达式列表将为空
  const auto &exprs = select_stmt_->query_expressions();
  if (exprs.empty()) {
    // 表达式已经被移动，无法获取类型，返回 UNDEFINED
    // 这在实际使用中应该不会造成问题，因为子查询类型通常在绑定阶段确定
    return AttrType::UNDEFINED;
  }

  return exprs[0]->value_type();
}

void SubQueryExpr::restore_expressions() const
{
  if (!select_stmt_ || saved_expressions_.empty()) {
    LOG_TRACE("cannot restore expressions: select_stmt_=%p, saved_expressions_.size()=%d", 
              select_stmt_.get(), static_cast<int>(saved_expressions_.size()));
    return;
  }
  
  // 清空现有的表达式（应该已经是空的）
  select_stmt_->query_expressions().clear();
  
  // 从保存的副本中恢复表达式
  // 注意：如果 expected_column_count_ > 0，只恢复 expected_column_count_ 个表达式
  // 否则，恢复所有保存的表达式
  size_t restore_count = saved_expressions_.size();
  if (expected_column_count_ > 0 && static_cast<size_t>(expected_column_count_) < saved_expressions_.size()) {
    restore_count = static_cast<size_t>(expected_column_count_);
    LOG_TRACE("restoring only %d expressions (expected_column_count_) out of %d saved expressions",
              static_cast<int>(restore_count), static_cast<int>(saved_expressions_.size()));
  }
  
  for (size_t i = 0; i < restore_count; ++i) {
    if (saved_expressions_[i]) {
      select_stmt_->query_expressions().push_back(saved_expressions_[i]->copy());
    } else {
      LOG_WARN("saved_expressions_[%d] is null, skipping", static_cast<int>(i));
    }
  }
  
  LOG_TRACE("restored %d expressions to SelectStmt (expected: %d, saved: %d)", 
            static_cast<int>(select_stmt_->query_expressions().size()), expected_column_count_, 
            static_cast<int>(saved_expressions_.size()));
}

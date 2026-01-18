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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include "sql/expr/subquery_expr.h"
#include "sql/expr/expression.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt, 
                      const unordered_map<string, Table *> *outer_tables)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context(db);

  // 如果有外部查询的表，先添加到 binder_context 中（用于支持相关子查询）
  if (outer_tables != nullptr) {
    for (const auto &pair : *outer_tables) {
      binder_context.add_table(pair.second);
    }
  }

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }
  
  // 合并外部查询的表到 table_map 中（用于 FilterStmt 查找）
  if (outer_tables != nullptr) {
    for (const auto &pair : *outer_tables) {
      table_map.insert(pair);
    }
  }

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder expression_binder(binder_context);
  
  if (select_sql.expressions.empty()) {
    LOG_WARN("select_sql.expressions is empty");
    return RC::INVALID_ARGUMENT;
  }
  
  LOG_TRACE("binding %d expressions", select_sql.expressions.size());
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    if (nullptr == expression) {
      LOG_WARN("expression is null in select_sql.expressions");
      continue;
    }
    size_t before_size = bound_expressions.size();
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_WARN("bind expression failed. rc=%s, expression type=%d", strrc(rc), static_cast<int>(expression->type()));
      return rc;
    }
    LOG_TRACE("bound expression, type=%d, before_size=%d, after_size=%d", 
              static_cast<int>(expression->type()), before_size, bound_expressions.size());
  }
  
  if (bound_expressions.empty()) {
    LOG_WARN("bound_expressions is empty after binding all expressions");
    return RC::INVALID_ARGUMENT;
  }

  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  vector<pair<unique_ptr<Expression>, bool>> order_by_expressions;
  for (OrderBySqlNode &order_by_node : select_sql.order_by) {
    vector<unique_ptr<Expression>> bound_exprs;
    RC rc = expression_binder.bind_expression(order_by_node.expression, bound_exprs);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind order by expression failed. rc=%s", strrc(rc));
      return rc;
    }
    if (bound_exprs.size() != 1) {
      LOG_WARN("order by expression should bind to exactly one expression");
      return RC::INTERNAL;
    }
    order_by_expressions.emplace_back(std::move(bound_exprs[0]), order_by_node.asc);
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // 分离包含子查询的条件和普通条件
  vector<ConditionSqlNode> normal_conditions;
  vector<ConditionSqlNode> subquery_conditions;
  for (const ConditionSqlNode &cond : select_sql.conditions) {
    if (cond.right_is_subquery) {
      subquery_conditions.push_back(cond);
    } else {
      normal_conditions.push_back(cond);
    }
  }

  // create filter statement for normal conditions
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = RC::SUCCESS;
  if (!normal_conditions.empty()) {
    rc = FilterStmt::create(db,
        default_table,
        &table_map,
        normal_conditions.data(),
        static_cast<int>(normal_conditions.size()),
        filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  // 处理包含子查询的条件，转换为表达式
  vector<unique_ptr<Expression>> subquery_condition_exprs;
  for (const ConditionSqlNode &cond : subquery_conditions) {
    // 创建左表达式
    unique_ptr<Expression> left;
    if (cond.left_is_attr) {
      Table *table = nullptr;
      const FieldMeta *field = nullptr;
      rc = get_table_and_field(db, default_table, &table_map, cond.left_attr, table, field);
      if (OB_FAIL(rc)) {
        LOG_WARN("cannot find attr for subquery condition");
        if (filter_stmt) delete filter_stmt;
        return rc;
      }
      left = make_unique<FieldExpr>(Field(table, field));
    } else {
      left = make_unique<ValueExpr>(cond.left_value);
    }

    // 创建子查询表达式
    // 注意：这里需要复制 SelectSqlNode，因为原始节点可能在解析后被删除
    SelectStmt *subquery_stmt = nullptr;
    if (cond.right_subquery == nullptr) {
      LOG_WARN("subquery right_subquery is null");
      if (filter_stmt) delete filter_stmt;
      return RC::INVALID_ARGUMENT;
    }
    
    // 复制 SelectSqlNode 的内容（因为包含 unique_ptr，需要手动复制）
    // 优先使用 right_subquery_node，因为它保存了原始的未修改的表达式
    const SelectSqlNode *subquery_sql = nullptr;
    if (cond.right_subquery_node != nullptr) {
      // 使用 right_subquery_node 中的原始数据，避免表达式被移动后为空
      subquery_sql = &cond.right_subquery_node->selection;
      LOG_TRACE("using right_subquery_node, expressions size=%d", subquery_sql->expressions.size());
    } else if (cond.right_subquery != nullptr) {
      // 如果没有 right_subquery_node，回退到 right_subquery
      subquery_sql = cond.right_subquery;
      LOG_TRACE("using right_subquery, expressions size=%d", subquery_sql->expressions.size());
    } else {
      LOG_WARN("both right_subquery_node and right_subquery are null");
      if (filter_stmt) delete filter_stmt;
      return RC::INVALID_ARGUMENT;
    }
    
    SelectSqlNode subquery_sql_copy;
    
    // 复制 expressions
    if (subquery_sql->expressions.empty()) {
      LOG_WARN("subquery expressions is empty. right_subquery_node=%p, right_subquery=%p", 
               cond.right_subquery_node, cond.right_subquery);
      if (filter_stmt) delete filter_stmt;
      return RC::INVALID_ARGUMENT;
    }
    
    for (const auto &expr : subquery_sql->expressions) {
      if (expr) {
        subquery_sql_copy.expressions.push_back(expr->copy());
      } else {
        LOG_WARN("subquery expression is null");
      }
    }
    
    if (subquery_sql_copy.expressions.empty()) {
      LOG_WARN("subquery_sql_copy expressions is empty after copying. original size=%d", 
               subquery_sql->expressions.size());
      if (filter_stmt) delete filter_stmt;
      return RC::INVALID_ARGUMENT;
    }
    
    // 复制 relations
    subquery_sql_copy.relations = subquery_sql->relations;
    
    // 复制 conditions（包括嵌套子查询条件）
    // 嵌套子查询会在递归调用 SelectStmt::create 时被正确处理
    // 传递给嵌套子查询的 table_map 包含所有外部查询的表，以支持多层相关子查询
    for (const auto &c : subquery_sql->conditions) {
      subquery_sql_copy.conditions.push_back(c);
    }
    
    // 复制 group_by
    for (const auto &gb : subquery_sql->group_by) {
      if (gb) {
        subquery_sql_copy.group_by.push_back(gb->copy());
      }
    }
    
    // 复制 order_by
    for (const auto &ob : subquery_sql->order_by) {
      if (ob.expression) {
        OrderBySqlNode ob_copy;
        ob_copy.expression = ob.expression->copy();
        ob_copy.asc = ob.asc;
        subquery_sql_copy.order_by.push_back(std::move(ob_copy));
      }
    }
    
    LOG_TRACE("creating subquery SelectStmt with %d expressions, %d relations", 
              subquery_sql_copy.expressions.size(), subquery_sql_copy.relations.size());
    // 传递外部查询的表信息，以支持相关子查询（correlated subquery）
    rc = SelectStmt::create(db, subquery_sql_copy, reinterpret_cast<Stmt *&>(subquery_stmt), &table_map);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to create subquery SelectStmt. rc=%s", strrc(rc));
      if (filter_stmt) delete filter_stmt;
      return rc;
    }
    
    // 验证子查询是否成功创建并包含表达式
    if (subquery_stmt->query_expressions().empty()) {
      LOG_WARN("subquery SelectStmt created but query_expressions is empty");
      delete subquery_stmt;
      if (filter_stmt) delete filter_stmt;
      return RC::INTERNAL;
    }
    LOG_TRACE("subquery SelectStmt created successfully with %d query_expressions", 
              subquery_stmt->query_expressions().size());
    // 将 SelectStmt 的所有权转移给 SubQueryExpr
    unique_ptr<SelectStmt> subquery_stmt_ptr(subquery_stmt);
    unique_ptr<Expression> right = make_unique<SubQueryExpr>(std::move(subquery_stmt_ptr));

    // 创建比较表达式
    unique_ptr<Expression> cmp_expr = make_unique<ComparisonExpr>(cond.comp, std::move(left), std::move(right));
    subquery_condition_exprs.push_back(std::move(cmp_expr));
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->order_by_.swap(order_by_expressions);
  select_stmt->subquery_conditions_.swap(subquery_condition_exprs);
  stmt                      = select_stmt;
  return RC::SUCCESS;
}

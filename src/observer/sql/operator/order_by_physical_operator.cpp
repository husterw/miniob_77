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

#include "sql/operator/order_by_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "sql/expr/composite_tuple.h"

using namespace std;

OrderByPhysicalOperator::OrderByPhysicalOperator(vector<pair<unique_ptr<Expression>, bool>> &&order_by_exprs)
    : order_by_expressions_(std::move(order_by_exprs)), current_index_(0)
{}

RC OrderByPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  // 读取所有tuple
  vector<unique_ptr<CompositeTuple>> tuples;
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get tuple from child operator");
      return RC::INTERNAL;
    }

    // 使用 ValueListTuple 来保存 tuple 数据（复制数据而不是引用）
    ValueListTuple value_list_tuple;
    rc = ValueListTuple::make(*tuple, value_list_tuple);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to make tuple to value list. rc=%s", strrc(rc));
      return rc;
    }

    // 创建 CompositeTuple 来保存 value_list_tuple
    unique_ptr<CompositeTuple> composite_tuple = make_unique<CompositeTuple>();
    composite_tuple->add_tuple(make_unique<ValueListTuple>(std::move(value_list_tuple)));
    tuples.emplace_back(std::move(composite_tuple));
  }

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to read all tuples from child operator. rc=%s", strrc(rc));
    child->close();
    return rc;
  }

  // 排序
  if (!order_by_expressions_.empty()) {
    std::sort(tuples.begin(), tuples.end(), [this](const unique_ptr<CompositeTuple> &t1, const unique_ptr<CompositeTuple> &t2) -> bool {
      int result = 0;
      RC  rc     = compare_tuples(*t1, *t2, result);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to compare tuples. rc=%s", strrc(rc));
        return false;
      }
      return result < 0;
    });
  }

  sorted_tuples_ = std::move(tuples);
  current_index_ = 0;

  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next()
{
  if (current_index_ >= sorted_tuples_.size()) {
    return RC::RECORD_EOF;
  }

  current_index_++;
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{
  sorted_tuples_.clear();
  current_index_ = 0;

  if (!children_.empty()) {
    children_[0]->close();
  }

  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
  if (current_index_ == 0 || current_index_ > sorted_tuples_.size()) {
    return nullptr;
  }

  return sorted_tuples_[current_index_ - 1].get();
}

RC OrderByPhysicalOperator::compare_tuples(const CompositeTuple &t1, const CompositeTuple &t2, int &result) const
{
  result = 0;

  for (const auto &order_by_expr : order_by_expressions_) {
    Expression *expr = order_by_expr.first.get();
    bool        asc  = order_by_expr.second;

    Value v1, v2;
    RC    rc = expr->get_value(t1, v1);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value from tuple 1. rc=%s", strrc(rc));
      return rc;
    }

    rc = expr->get_value(t2, v2);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to get value from tuple 2. rc=%s", strrc(rc));
      return rc;
    }

    int cmp_result = v1.compare(v2);
    if (cmp_result != 0) {
      result = asc ? cmp_result : -cmp_result;
      return RC::SUCCESS;
    }
  }

  // 所有排序字段都相等
  result = 0;
  return RC::SUCCESS;
}

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

#include "sql/operator/physical_operator.h"
#include "sql/expr/composite_tuple.h"

class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(vector<pair<unique_ptr<Expression>, bool>> &&order_by_exprs);

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }
  OpType               get_op_type() const override { return OpType::ORDERBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  RC compare_tuples(const CompositeTuple &t1, const CompositeTuple &t2, int &result) const;

private:
  vector<pair<unique_ptr<Expression>, bool>> order_by_expressions_;  ///< expression and is_asc
  vector<unique_ptr<CompositeTuple>> sorted_tuples_;                 ///< 排序后的所有tuple
  size_t current_index_;                                              ///< 当前返回的tuple索引
};

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
// Created by Wangyunlai on 2022/07/08.
//

#pragma once

#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"

class CompositeTuple;

/**
 * @brief 索引扫描物理算子
 * @ingroup PhysicalOperator
 */
class IndexScanPhysicalOperator : public PhysicalOperator
{
public:
  IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode, const Value *left_value,
      bool left_inclusive, const Value *right_value, bool right_inclusive);

  virtual ~IndexScanPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::INDEX_SCAN; }

  string param() const override;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  void set_predicates(vector<unique_ptr<Expression>> &&exprs);
  
  /// @brief 设置外部查询的 tuple（用于相关子查询）
  void set_outer_tuple(const Tuple *outer_tuple);

private:
  // 与TableScanPhysicalOperator代码相同，可以优化
  RC filter(RowTuple &tuple, bool &result);
  /// @brief 更新 composite_tuple_，使其包含外部查询和当前 tuple 的所有字段
  void update_composite_tuple();

private:
  Trx          *trx_           = nullptr;
  Table        *table_         = nullptr;
  Index        *index_         = nullptr;
  ReadWriteMode mode_          = ReadWriteMode::READ_WRITE;
  IndexScanner *index_scanner_ = nullptr;

  Record   current_record_;
  RowTuple tuple_;

  Value left_value_;
  Value right_value_;
  bool  left_inclusive_  = false;
  bool  right_inclusive_ = false;

  vector<unique_ptr<Expression>> predicates_;
  const Tuple                    *outer_tuple_ = nullptr;  ///< 外部查询的 tuple（用于相关子查询）
  unique_ptr<CompositeTuple>     composite_tuple_;  ///< 组合外部查询和当前 tuple 的 CompositeTuple
};

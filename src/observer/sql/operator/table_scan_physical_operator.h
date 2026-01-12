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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "storage/record/record_scanner.h"
#include "common/types.h"

class Table;
class CompositeTuple;

/**
 * @brief 表扫描物理算子
 * @ingroup PhysicalOperator
 */
class TableScanPhysicalOperator : public PhysicalOperator
{
public:
  TableScanPhysicalOperator(Table *table, ReadWriteMode mode) : table_(table), mode_(mode) {}

  virtual ~TableScanPhysicalOperator() = default;

  string param() const override;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::TABLE_SCAN; }
  OpType               get_op_type() const override { return OpType::SEQSCAN; }
  virtual uint64_t     hash() const override
  {
    uint64_t hash = std::hash<int>()(static_cast<int>(get_op_type()));
    hash ^= std::hash<int>()(table_->table_id());
    return hash;
  }

  virtual bool operator==(const OperatorNode &other) const override
  {
    if (get_op_type() != other.get_op_type())
      return false;
    const auto &other_get = dynamic_cast<const TableScanPhysicalOperator *>(&other);
    if (table_->table_id() != other_get->table_id())
      return false;
    return true;
  }

  double calculate_cost(LogicalProperty *prop, const vector<LogicalProperty *> &child_log_props, CostModel *cm) override
  {
    return (cm->io() + cm->cpu_op()) * prop->get_card();
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  int table_id() const { return table_->table_id(); }

  void set_predicates(vector<unique_ptr<Expression>> &&exprs);
  
  /// @brief 设置外部查询的 tuple（用于相关子查询）
  void set_outer_tuple(const Tuple *outer_tuple);

private:
  RC filter(RowTuple &tuple, bool &result);
  /// @brief 更新 composite_tuple_，使其包含外部查询和当前 tuple 的所有字段
  void update_composite_tuple();

private:
  Table                         *table_ = nullptr;
  Trx                           *trx_   = nullptr;
  ReadWriteMode                  mode_  = ReadWriteMode::READ_WRITE;
  RecordScanner                 *record_scanner_;
  Record                         current_record_;
  RowTuple                       tuple_;
  vector<unique_ptr<Expression>> predicates_;  // TODO chang predicate to table tuple filter
  const Tuple                    *outer_tuple_ = nullptr;  ///< 外部查询的 tuple（用于相关子查询）
  unique_ptr<CompositeTuple>     composite_tuple_;  ///< 组合外部查询和当前 tuple 的 CompositeTuple
};

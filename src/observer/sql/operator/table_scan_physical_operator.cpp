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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/table_scan_physical_operator.h"
#include "event/sql_debug.h"
#include "storage/table/table.h"
#include "sql/expr/composite_tuple.h"
#include "sql/expr/tuple.h"

using namespace std;

RC TableScanPhysicalOperator::open(Trx *trx)
{
  RC rc = table_->get_record_scanner(record_scanner_, trx, mode_);
  if (rc == RC::SUCCESS) {
    tuple_.set_schema(table_, table_->table_meta().field_metas());
  }
  trx_ = trx;
  return rc;
}

RC TableScanPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;

  bool filter_result = false;
  while (OB_SUCC(rc = record_scanner_->next(current_record_))) {
    LOG_TRACE("got a record. rid=%s", current_record_.rid().to_string().c_str());
    
    tuple_.set_record(&current_record_);
    rc = filter(tuple_, filter_result);
    if (rc != RC::SUCCESS) {
      LOG_TRACE("record filtered failed=%s", strrc(rc));
      return rc;
    }

    if (filter_result) {
      sql_debug("get a tuple: %s", tuple_.to_string().c_str());
      break;
    } else {
      sql_debug("a tuple is filtered: %s", tuple_.to_string().c_str());
    }
  }
  return rc;
}

RC TableScanPhysicalOperator::close() {
  RC rc = RC::SUCCESS;
  if (record_scanner_ != nullptr) {
    rc = record_scanner_->close_scan();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close record scanner");
    }
    delete record_scanner_;
    record_scanner_ = nullptr;
  }
  return rc;

}

Tuple *TableScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);
  
  // 如果存在外部查询的 tuple，返回 CompositeTuple 来组合外部查询和当前 tuple
  if (outer_tuple_ != nullptr) {
    update_composite_tuple();
    return composite_tuple_.get();
  }
  
  return &tuple_;
}

string TableScanPhysicalOperator::param() const { return table_->name(); }

void TableScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

void TableScanPhysicalOperator::set_outer_tuple(const Tuple *outer_tuple)
{
  outer_tuple_ = outer_tuple;
  composite_tuple_.reset();  // 重置 CompositeTuple，下次 current_tuple() 时会重新创建
}

void TableScanPhysicalOperator::update_composite_tuple()
{
  if (outer_tuple_ == nullptr) {
    composite_tuple_.reset();
    return;
  }
  
  // 如果 composite_tuple_ 已经存在，先重置
  composite_tuple_ = make_unique<CompositeTuple>();
  
  // 创建外部查询 tuple 的副本
  auto outer_tuple_copy = make_unique<ValueListTuple>();
  int outer_cell_num = outer_tuple_->cell_num();
  vector<TupleCellSpec> outer_specs;
  vector<Value> outer_cells;
  for (int i = 0; i < outer_cell_num; ++i) {
    TupleCellSpec spec;
    RC spec_rc = outer_tuple_->spec_at(i, spec);
    if (OB_FAIL(spec_rc)) {
      LOG_WARN("failed to get spec from outer tuple at index %d. rc=%s", i, strrc(spec_rc));
      composite_tuple_.reset();
      return;
    }
    outer_specs.push_back(spec);
    
    Value cell;
    RC cell_rc = outer_tuple_->cell_at(i, cell);
    if (OB_FAIL(cell_rc)) {
      LOG_WARN("failed to get cell from outer tuple at index %d. rc=%s", i, strrc(cell_rc));
      composite_tuple_.reset();
      return;
    }
    outer_cells.push_back(cell);
  }
  outer_tuple_copy->set_names(outer_specs);
  outer_tuple_copy->set_cells(outer_cells);
  composite_tuple_->add_tuple(std::move(outer_tuple_copy));
  
  // 创建当前 tuple 的副本
  auto inner_tuple_copy = make_unique<ValueListTuple>();
  int inner_cell_num = tuple_.cell_num();
  vector<TupleCellSpec> inner_specs;
  vector<Value> inner_cells;
  for (int i = 0; i < inner_cell_num; ++i) {
    TupleCellSpec spec;
    RC spec_rc = tuple_.spec_at(i, spec);
    if (OB_FAIL(spec_rc)) {
      LOG_WARN("failed to get spec from inner tuple at index %d. rc=%s", i, strrc(spec_rc));
      composite_tuple_.reset();
      return;
    }
    inner_specs.push_back(spec);
    
    Value cell;
    RC cell_rc = tuple_.cell_at(i, cell);
    if (OB_FAIL(cell_rc)) {
      LOG_WARN("failed to get cell from inner tuple at index %d. rc=%s", i, strrc(cell_rc));
      composite_tuple_.reset();
      return;
    }
    inner_cells.push_back(cell);
  }
  inner_tuple_copy->set_names(inner_specs);
  inner_tuple_copy->set_cells(inner_cells);
  composite_tuple_->add_tuple(std::move(inner_tuple_copy));
}

RC TableScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  
  // 如果没有过滤条件，所有行都通过
  if (predicates_.empty()) {
    result = true;
    return RC::SUCCESS;
  }
  
  // 如果存在外部查询的 tuple，创建 CompositeTuple 来组合外部查询和子查询的 tuple
  const Tuple *tuple_to_use = &tuple;
  unique_ptr<CompositeTuple> composite_tuple;
  
  if (outer_tuple_ != nullptr) {
    composite_tuple = make_unique<CompositeTuple>();
    
    // 创建外部查询 tuple 的副本（因为 CompositeTuple 需要拥有所有权）
    // 如果 outer_tuple_ 是 CompositeTuple，将其展开为一个扁平的 ValueListTuple
    // 这样可以在嵌套子查询中保留所有外部查询的字段
    auto outer_tuple_copy = make_unique<ValueListTuple>();
    int outer_cell_num = outer_tuple_->cell_num();
    vector<TupleCellSpec> outer_specs;
    vector<Value> outer_cells;
    for (int i = 0; i < outer_cell_num; ++i) {
      TupleCellSpec spec;
      RC spec_rc = outer_tuple_->spec_at(i, spec);
      if (OB_FAIL(spec_rc)) {
        LOG_WARN("failed to get spec from outer tuple at index %d. rc=%s", i, strrc(spec_rc));
        return spec_rc;
      }
      outer_specs.push_back(spec);
      
      Value cell;
      RC cell_rc = outer_tuple_->cell_at(i, cell);
      if (OB_FAIL(cell_rc)) {
        LOG_WARN("failed to get cell from outer tuple at index %d. rc=%s", i, strrc(cell_rc));
        return cell_rc;
      }
      outer_cells.push_back(cell);
    }
    outer_tuple_copy->set_names(outer_specs);
    outer_tuple_copy->set_cells(outer_cells);
    composite_tuple->add_tuple(std::move(outer_tuple_copy));
    
    // 创建子查询 tuple 的副本
    auto inner_tuple_copy = make_unique<ValueListTuple>();
    int inner_cell_num = tuple.cell_num();
    vector<TupleCellSpec> inner_specs;
    vector<Value> inner_cells;
    for (int i = 0; i < inner_cell_num; ++i) {
      TupleCellSpec spec;
      RC spec_rc = tuple.spec_at(i, spec);
      if (OB_FAIL(spec_rc)) {
        LOG_WARN("failed to get spec from inner tuple at index %d. rc=%s", i, strrc(spec_rc));
        return spec_rc;
      }
      inner_specs.push_back(spec);
      
      Value cell;
      RC cell_rc = tuple.cell_at(i, cell);
      if (OB_FAIL(cell_rc)) {
        LOG_WARN("failed to get cell from inner tuple at index %d. rc=%s", i, strrc(cell_rc));
        return cell_rc;
      }
      inner_cells.push_back(cell);
    }
    inner_tuple_copy->set_names(inner_specs);
    inner_tuple_copy->set_cells(inner_cells);
    
    // 添加子查询的 tuple 到 CompositeTuple
    composite_tuple->add_tuple(std::move(inner_tuple_copy));
    tuple_to_use = composite_tuple.get();
  }
  
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(*tuple_to_use, value);
    if (rc != RC::SUCCESS) {
      // 如果表达式评估返回错误（比如 NOTFOUND），将这一行视为不满足条件（返回 false）
      // 而不是让整个查询失败
      // 这样可以处理子查询返回错误的情况
      LOG_TRACE("expression evaluation failed in filter, treating row as filtered out. rc=%s", strrc(rc));
      result = false;
      return RC::SUCCESS;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}

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

#include "sql/operator/index_scan_physical_operator.h"
#include "sql/expr/composite_tuple.h"
#include "sql/expr/tuple.h"
#include "storage/index/index.h"
#include "storage/trx/trx.h"

IndexScanPhysicalOperator::IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode, const Value *left_value,
    bool left_inclusive, const Value *right_value, bool right_inclusive)
    : table_(table),
      index_(index),
      mode_(mode),
      left_inclusive_(left_inclusive),
      right_inclusive_(right_inclusive)
{
  if (left_value) {
    left_value_ = *left_value;
  }
  if (right_value) {
    right_value_ = *right_value;
  }
}

RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if (nullptr == table_ || nullptr == index_) {
    return RC::INTERNAL;
  }

  IndexScanner *index_scanner = index_->create_scanner(left_value_.data(),
      left_value_.length(),
      left_inclusive_,
      right_value_.data(),
      right_value_.length(),
      right_inclusive_);
  if (nullptr == index_scanner) {
    LOG_WARN("failed to create index scanner");
    return RC::INTERNAL;
  }
  index_scanner_ = index_scanner;

  tuple_.set_schema(table_, table_->table_meta().field_metas());

  trx_ = trx;
  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next()
{
  // TODO: 需要适配 lsm-tree 引擎
  RID rid;
  RC  rc = RC::SUCCESS;

  bool filter_result = false;
  while (RC::SUCCESS == (rc = index_scanner_->next_entry(&rid))) {
    rc = table_->get_record(rid, current_record_);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to get record. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      return rc;
    }

    LOG_TRACE("got a record. rid=%s", rid.to_string().c_str());

    tuple_.set_record(&current_record_);
    rc = filter(tuple_, filter_result);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to filter record. rc=%s", strrc(rc));
      return rc;
    }

    if (!filter_result) {
      LOG_TRACE("record filtered");
      continue;
    }

    rc = trx_->visit_record(table_, current_record_, mode_);
    if (rc == RC::RECORD_INVISIBLE) {
      LOG_TRACE("record invisible");
      continue;
    } else {
      return rc;
    }
  }

  return rc;
}

RC IndexScanPhysicalOperator::close()
{
  index_scanner_->destroy();
  index_scanner_ = nullptr;
  return RC::SUCCESS;
}

Tuple *IndexScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);
  
  // 如果存在外部查询的 tuple，返回 CompositeTuple 来组合外部查询和当前 tuple
  if (outer_tuple_ != nullptr) {
    update_composite_tuple();
    return composite_tuple_.get();
  }
  
  return &tuple_;
}

void IndexScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

void IndexScanPhysicalOperator::set_outer_tuple(const Tuple *outer_tuple)
{
  outer_tuple_ = outer_tuple;
  composite_tuple_.reset();  // 重置 CompositeTuple，下次 current_tuple() 时会重新创建
}

void IndexScanPhysicalOperator::update_composite_tuple()
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

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  
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
      Value v;
      TupleCellSpec spec;
      RC rc = outer_tuple_->cell_at(i, v);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to get cell from outer tuple. index=%d, rc=%s", i, strrc(rc));
        return rc;
      }
      rc = outer_tuple_->spec_at(i, spec);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to get spec from outer tuple. index=%d, rc=%s", i, strrc(rc));
        return rc;
      }
      outer_cells.push_back(v);
      outer_specs.push_back(spec);
    }
    
    outer_tuple_copy->set_cells(outer_cells);
    outer_tuple_copy->set_names(outer_specs);
    composite_tuple->add_tuple(std::move(outer_tuple_copy));
    
    // 创建子查询 tuple 的副本
    auto inner_tuple_copy = make_unique<ValueListTuple>();
    int inner_cell_num = tuple.cell_num();
    vector<TupleCellSpec> inner_specs;
    vector<Value> inner_cells;
    
    for (int i = 0; i < inner_cell_num; ++i) {
      Value v;
      TupleCellSpec spec;
      RC rc = tuple.cell_at(i, v);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to get cell from inner tuple. index=%d, rc=%s", i, strrc(rc));
        return rc;
      }
      rc = tuple.spec_at(i, spec);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to get spec from inner tuple. index=%d, rc=%s", i, strrc(rc));
        return rc;
      }
      inner_cells.push_back(v);
      inner_specs.push_back(spec);
    }
    
    inner_tuple_copy->set_cells(inner_cells);
    inner_tuple_copy->set_names(inner_specs);
    
    // 添加子查询的 tuple 到 CompositeTuple
    composite_tuple->add_tuple(std::move(inner_tuple_copy));
    tuple_to_use = composite_tuple.get();
  }
  
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(*tuple_to_use, value);
    if (rc != RC::SUCCESS) {
      return rc;
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

string IndexScanPhysicalOperator::param() const
{
  return string(index_->index_meta().name()) + " ON " + table_->name();
}

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

#include "sql/operator/insert_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

InsertPhysicalOperator::InsertPhysicalOperator(Table *table, vector<vector<Value>> &&values)
    : table_(table), values_(std::move(values))
{}

RC InsertPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  vector<Record> records;
  vector<Record> inserted_records;

  for (const auto &values_hang : values_) {
    Record record;
    const int row_size = static_cast<int>(values_hang.size());
    
    rc = table_->make_record(row_size, values_hang.data(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record. row_size=%d, rc=%s", row_size, strrc(rc));
      return rc;
    }
    
    records.push_back(std::move(record));
  }
  
  for (auto &record : records) {
    rc = trx->insert_record(table_, record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record by transaction. table=%s, rc=%s", table_->name(), strrc(rc));
      
      for (auto &inserted_record : inserted_records) {
        RC delete_rc = trx->delete_record(table_, inserted_record);
        if (delete_rc != RC::SUCCESS) {
          LOG_ERROR("failed to delete inserted record when rollback. table=%s, rc=%s", table_->name(), strrc(delete_rc));
        }
      }
      
      return rc;
    }
    
    inserted_records.push_back(std::move(record));
  }
  
  return rc;
}

RC InsertPhysicalOperator::next() { return RC::RECORD_EOF; }

RC InsertPhysicalOperator::close() { return RC::SUCCESS; }

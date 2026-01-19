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
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"
#include <vector>
#include <cstring>

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s, field:%s",
        file_name, index_meta.name(), index_meta.field());
    return RC::RECORD_OPENNED;
  }

  // 从 TableMeta 中获取 FieldMeta 指针，确保指针有效
  const FieldMeta *field_meta_ptr = table->table_meta().field(field_meta.name());
  if (field_meta_ptr == nullptr) {
    LOG_WARN("Failed to find field in table meta. field name=%s", field_meta.name());
    return RC::SCHEMA_FIELD_MISSING;
  }

  vector<const FieldMeta *> field_metas;
  field_metas.push_back(field_meta_ptr);
  Index::init(index_meta, field_metas);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, field_meta.type(), field_meta.len());
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name, index_meta.name(), index_meta.field(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully create index, file_name:%s, index:%s, field:%s",
    file_name, index_meta.name(), index_meta.field());
  return RC::SUCCESS;
}

RC BplusTreeIndex::create(Table *table, const char *file_name, const IndexMeta &index_meta, const vector<const FieldMeta *> &field_metas)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s",
        file_name, index_meta.name());
    return RC::RECORD_OPENNED;
  }

  if (field_metas.empty()) {
    LOG_WARN("Failed to create index, no fields specified");
    return RC::INVALID_ARGUMENT;
  }

  // 验证所有 FieldMeta 指针都有效，并且都指向 TableMeta 中的 FieldMeta
  vector<const FieldMeta *> validated_field_metas;
  for (const auto *field_meta : field_metas) {
    if (field_meta == nullptr) {
      LOG_WARN("field_meta is null in field_metas");
      return RC::INVALID_ARGUMENT;
    }
    // 从 TableMeta 中获取 FieldMeta 指针，确保指针有效
    const FieldMeta *validated_field_meta = table->table_meta().field(field_meta->name());
    if (validated_field_meta == nullptr) {
      LOG_WARN("Failed to find field in table meta. field name=%s", field_meta->name());
      return RC::SCHEMA_FIELD_MISSING;
    }
    validated_field_metas.push_back(validated_field_meta);
  }

  Index::init(index_meta, validated_field_metas);

  // 计算复合键的总长度
  int total_key_length = 0;
  for (const auto *field_meta : field_metas) {
    total_key_length += field_meta->len();
  }

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  // 对于多字段索引，使用 CHARS 类型进行字节比较
  // 这样可以正确处理不同字段类型的复合键（如 int + float）
  // 字节比较会按照字段顺序逐个字节比较，对于相同类型的字段可以正确排序
  AttrType composite_type = (field_metas.size() == 1) ? field_metas[0]->type() : AttrType::CHARS;
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, composite_type, total_key_length);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, rc:%s",
        file_name, index_meta.name(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully create multi-field index, file_name:%s, index:%s, fields count:%zu",
    file_name, index_meta.name(), field_metas.size());
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(Table *table, const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta)
{
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been initedd before. file_name:%s, index:%s, field:%s",
        file_name, index_meta.name(), index_meta.field());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, field_meta);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name, index_meta.name(), index_meta.field(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully open index, file_name:%s, index:%s, field:%s",
    file_name, index_meta.name(), index_meta.field());
  return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field:%s", index_meta_.name(), index_meta_.field());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  if (record == nullptr || rid == nullptr) {
    LOG_WARN("Invalid arguments: record or rid is null");
    return RC::INVALID_ARGUMENT;
  }

  // 构建复合键
  vector<char> composite_key;
  if (field_metas_.empty()) {
    // 如果没有 field_metas_，使用 field_meta_（向后兼容）
    composite_key.resize(field_meta_.len());
    memcpy(composite_key.data(), record + field_meta_.offset(), field_meta_.len());
  } else if (field_metas_.size() == 1) {
    // 单字段索引：使用 field_metas_[0]
    const FieldMeta *field_meta = field_metas_[0];
    if (field_meta == nullptr) {
      LOG_WARN("field_meta is null in single field index");
      return RC::INTERNAL;
    }
    composite_key.resize(field_meta->len());
    memcpy(composite_key.data(), record + field_meta->offset(), field_meta->len());
  } else {
    // 多字段索引：将所有字段值拼接在一起
    int total_len = 0;
    for (const auto *field_meta : field_metas_) {
      if (field_meta == nullptr) {
        LOG_WARN("field_meta is null in multi-field index");
        return RC::INTERNAL;
      }
      total_len += field_meta->len();
    }
    composite_key.resize(total_len);
    char *key_ptr = composite_key.data();
    for (const auto *field_meta : field_metas_) {
      if (field_meta == nullptr) {
        LOG_WARN("field_meta is null in multi-field index");
        return RC::INTERNAL;
      }
      memcpy(key_ptr, record + field_meta->offset(), field_meta->len());
      key_ptr += field_meta->len();
    }
  }

  if(index_meta_.unique_type()) {
    // check duplicate key
    list<RID> existing_rids;
    RC rc = index_handler_.get_entry(composite_key.data(), composite_key.size(), existing_rids);
    if (rc != RC::SUCCESS && rc != RC::RECORD_INVALID_KEY) {
      LOG_WARN("Failed to check duplicate key when insert entry to unique index, rc=%d:%s",
               rc, strrc(rc));
      return rc;
    }
    if (!existing_rids.empty()) {
      LOG_WARN("Duplicate key when insert entry to unique index. index:%s",
               index_meta_.name());
      return RC::RECORD_DUPLICATE_KEY;
    }
  }
  return index_handler_.insert_entry(composite_key.data(), rid);
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  if (record == nullptr || rid == nullptr) {
    LOG_WARN("Invalid arguments: record or rid is null");
    return RC::INVALID_ARGUMENT;
  }

  // 构建复合键
  vector<char> composite_key;
  if (field_metas_.empty()) {
    // 如果没有 field_metas_，使用 field_meta_（向后兼容）
    composite_key.resize(field_meta_.len());
    memcpy(composite_key.data(), record + field_meta_.offset(), field_meta_.len());
  } else if (field_metas_.size() == 1) {
    // 单字段索引：使用 field_metas_[0]
    const FieldMeta *field_meta = field_metas_[0];
    if (field_meta == nullptr) {
      LOG_WARN("field_meta is null in single field index");
      return RC::INTERNAL;
    }
    composite_key.resize(field_meta->len());
    memcpy(composite_key.data(), record + field_meta->offset(), field_meta->len());
  } else {
    // 多字段索引：将所有字段值拼接在一起
    int total_len = 0;
    for (const auto *field_meta : field_metas_) {
      if (field_meta == nullptr) {
        LOG_WARN("field_meta is null in multi-field index");
        return RC::INTERNAL;
      }
      total_len += field_meta->len();
    }
    composite_key.resize(total_len);
    char *key_ptr = composite_key.data();
    for (const auto *field_meta : field_metas_) {
      if (field_meta == nullptr) {
        LOG_WARN("field_meta is null in multi-field index");
        return RC::INTERNAL;
      }
      memcpy(key_ptr, record + field_meta->offset(), field_meta->len());
      key_ptr += field_meta->len();
    }
  }
  return index_handler_.delete_entry(composite_key.data(), rid);
}

IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}

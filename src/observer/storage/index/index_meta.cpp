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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_FIELD_NAMES("field_names");
const static Json::StaticString FIELD_UNIQUE("unique");

RC IndexMeta::init(const char *name, const FieldMeta &field, bool Unique)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  fields_.clear();
  fields_.push_back(field.name());
  Unique_ = Unique;
  return RC::SUCCESS;
}

RC IndexMeta::init(const char *name, const vector<FieldMeta> &fields, bool Unique)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  if (fields.empty()) {
    LOG_ERROR("Failed to init index, no fields specified.");
    return RC::INVALID_ARGUMENT;
  }

  name_  = name;
  fields_.clear();
  for (const auto &field : fields) {
    fields_.push_back(field.name());
  }
  Unique_ = Unique;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  if (fields_.size() == 1) {
    // 向后兼容：单个字段时使用field_name
    json_value[FIELD_FIELD_NAME] = fields_[0];
  } else {
    // 多个字段时使用field_names数组
    Json::Value field_names_value;
    for (const auto &field_name : fields_) {
      field_names_value.append(field_name);
    }
    json_value[FIELD_FIELD_NAMES] = field_names_value;
  }
  json_value[FIELD_UNIQUE] = Unique_;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value  = json_value[FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  vector<FieldMeta> fields;
  
  // 支持向后兼容：单个字段使用field_name
  if (json_value.isMember(FIELD_FIELD_NAME.c_str())) {
    const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
    if (!field_value.isString()) {
      LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
          name_value.asCString(), field_value.toStyledString().c_str());
      return RC::INTERNAL;
    }
    const FieldMeta *field = table.field(field_value.asCString());
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(*field);
  } else if (json_value.isMember(FIELD_FIELD_NAMES.c_str())) {
    // 多个字段使用field_names数组
    const Json::Value &field_names_value = json_value[FIELD_FIELD_NAMES];
    if (!field_names_value.isArray()) {
      LOG_ERROR("Field names of index [%s] is not an array. json value=%s",
          name_value.asCString(), field_names_value.toStyledString().c_str());
      return RC::INTERNAL;
    }
    for (const auto &field_name_value : field_names_value) {
      if (!field_name_value.isString()) {
        LOG_ERROR("Field name in index [%s] is not a string", name_value.asCString());
        return RC::INTERNAL;
      }
      const FieldMeta *field = table.field(field_name_value.asCString());
      if (nullptr == field) {
        LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_name_value.asCString());
        return RC::SCHEMA_FIELD_MISSING;
      }
      fields.push_back(*field);
    }
  } else {
    LOG_ERROR("Index [%s] has neither field_name nor field_names", name_value.asCString());
    return RC::INTERNAL;
  }

  return index.init(name_value.asCString(), fields, json_value[FIELD_UNIQUE].asBool());
}

const char *IndexMeta::name() const { return name_.c_str(); }

const char *IndexMeta::field() const { 
  // 向后兼容：返回第一个字段名
  return fields_.empty() ? nullptr : fields_[0].c_str();
}

void IndexMeta::desc(ostream &os) const { 
  os << "index name=" << name_ << ", fields=[";
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (i > 0) os << ", ";
    os << fields_[i];
  }
  os << "], unique=" << Unique_;
}
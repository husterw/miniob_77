#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

// UpdatePhysicalOperator：负责执行SQL的UPDATE操作的物理算子
// 核心功能：扫描目标记录、构建更新后的记录、通过事务完成数据更新


/**
 * @brief 打开算子，初始化执行环境并准备待更新的记录
 * @param trx 事务对象，用于后续的记录更新操作
 * @return RC 执行结果（SUCCESS表示成功，其他值表示失败）
 */
RC UpdatePhysicalOperator::open(Trx *trx)
{
  // 若没有子算子（理论上UPDATE需要扫描表，此处为安全兜底）
  if (children_.empty()) {
    // 没有子算子意味着没有全表扫描逻辑，但为了安全直接返回成功
    return RC::SUCCESS;
  }

  // 获取子算子（通常是表扫描算子，负责读取待更新的记录）
  auto &child = children_[0];
  // 打开子算子，初始化扫描环境
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  // 保存事务对象，后续更新记录需要
  trx_ = trx;

  // 循环读取子算子返回的记录（即待更新的记录）
  while (OB_SUCC(rc = child->next())) {
    // 获取当前扫描到的元组（Tuple是数据的抽象载体）
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current tuple while collecting for update");
      return RC::INTERNAL; // 内部错误：无法获取元组
    }
    // 将Tuple转换为RowTuple（RowTuple是与表记录绑定的具体元组类型）
    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    // 从RowTuple中提取原始记录（Record是存储层的实际数据结构）
    Record   &record    = row_tuple->record();
    // 将待更新的记录存入records_列表，后续批量处理
    records_.emplace_back(std::move(record));
  }
  // 关闭子算子，释放扫描资源
  child->close();

  // 遍历待更新的记录，执行实际的更新操作
  for (Record &old_rec : records_) {
    Record new_rec; // 存储更新后的新记录
    // 根据旧记录构建新记录（将更新的字段值写入新记录）
    rc = build_updated_record(old_rec, new_rec);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to build updated record. rc=%s", strrc(rc));
      return rc;
    }
    // 通过事务更新记录：将表中的old_rec替换为new_rec
    rc = trx_->update_record(table_, old_rec, new_rec);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to update record by transaction. rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS; // UPDATE操作执行成功
}


/**
 * @brief 根据旧记录和更新条件，构建更新后的新记录
 * @param old_record 原始记录（未更新的记录）
 * @param new_record 输出参数：存储更新后的新记录
 * @return RC 执行结果
 */
RC UpdatePhysicalOperator::build_updated_record(const Record &old_record, Record &new_record)
{
  // 步骤1：先复制旧记录的完整数据（保证未更新的字段值不变）
  RC rc = new_record.copy_data(old_record.data(), old_record.len());
  if (OB_FAIL(rc)) {
    return rc;
  }
  // 保留旧记录的行标识符（RID）：标识表中唯一的行
  new_record.set_rid(old_record.rid());

  // 步骤2：处理字段值的类型转换（若更新值的类型与字段类型不匹配）
  Value real_value; // 存储转换后的目标值
  if (value_.attr_type() != field_meta_->type()) {
    // 将更新值value_转换为字段的目标类型
    rc = Value::cast_to(value_, field_meta_->type(), real_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to cast value for update. field=%s target=%d rc=%s",
               field_meta_->name(), (int)field_meta_->type(), strrc(rc));
      return rc;
    }
  } else {
    // 类型匹配，直接使用原始更新值
    real_value = value_;
  }

  // 步骤3：将转换后的字段值写入新记录的对应位置
  if (field_meta_->type() == AttrType::TEXTS) {
    // TEXT 字段走 LOB 写入逻辑
    if (table_ == nullptr || table_->lob_handler() == nullptr) {
      LOG_WARN("lob handler is null when updating TEXT field. table or handler is null");
      return RC::INTERNAL;
    }

    string text = real_value.get_string();
    int64_t offset = 0;
    int64_t length = static_cast<int64_t>(text.size());
    // TEXT 最多保留 4096 字节，多余部分截断
    static const int64_t MAX_TEXT_LENGTH = 4096;
    if (length > MAX_TEXT_LENGTH) {
      length = MAX_TEXT_LENGTH;
    }
    RC rc = table_->lob_handler()->insert_data(offset, length, text.c_str());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to insert lob data for update. field=%s rc=%s", field_meta_->name(), strrc(rc));
      return rc;
    }

    LobRef ref;
    ref.offset   = offset;
    ref.length   = static_cast<int32_t>(length);
    ref.reserved = 0;

    memcpy(new_record.data() + field_meta_->offset(), &ref, sizeof(ref));
    return RC::SUCCESS;
  }

  size_t       copy_len = field_meta_->len(); // 字段定义的长度
  const size_t data_len = real_value.length(); // 更新值的实际长度
  // 针对字符串类型（CHARS）做长度适配（避免越界）
  if (field_meta_->type() == AttrType::CHARS) {
    // CHARS类型：若更新值长度小于字段定义长度，保留空终止符（同INSERT逻辑）
    if (copy_len > data_len) {
      copy_len = data_len + 1;
    }
  }
  // 将更新值写入新记录的对应偏移量位置（field_meta_->offset()是字段在记录中的偏移）
  memcpy(new_record.data() + field_meta_->offset(), real_value.data(), copy_len);
  return RC::SUCCESS;
}


/**
 * @brief 读取下一条记录（UPDATE算子是批量执行的，此处直接返回EOF表示无更多记录）
 * @return RC 固定返回RECORD_EOF
 */
RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }


/**
 * @brief 关闭算子，释放资源（此处暂无额外资源需释放）
 * @return RC 执行结果
 */
RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }
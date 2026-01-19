#pragma once

#include <cstdint>

/**
 * @brief 行内存储 TEXT 字段时用的 LOB 引用结构
 */
struct LobRef {
  int64_t offset;   ///< 在 LOB 文件中的偏移
  int32_t length;   ///< 文本长度（字节数）
  int32_t reserved; ///< 预留字段，便于将来扩展
};
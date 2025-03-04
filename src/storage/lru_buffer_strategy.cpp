#include "storage/lru_buffer_strategy.h"

namespace huadb {

void LRUBufferStrategy::Access(size_t frame_no) {
  // 缓存页面访问
  // LAB 1 BEGIN
  access_order_.remove(frame_no);
  access_order_.emplace_front(frame_no);
};

size_t LRUBufferStrategy::Evict() {
  // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
  // LAB 1 BEGIN
  auto lastBuffer = access_order_.back();
  access_order_.pop_back();
  return lastBuffer;
}

}  // namespace huadb

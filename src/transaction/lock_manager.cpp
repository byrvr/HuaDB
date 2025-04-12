#include "transaction/lock_manager.h"

namespace huadb {

bool LockManager::LockTable(xid_t xid, LockType lock_type, oid_t oid) {
  // 对数据表加锁，成功加锁返回 true，如果数据表已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据表的锁，根据需要升级锁的类型
  // LAB 3 BEGIN

  ResourceLock new_lock{
          lock_type,
          LockGranularity::TABLE,
          xid,
          {0, 0},
  };

  // Iterate through all table locks for this data table
  // Check for incompatibility
  for (auto &entry : resource_locks_[oid]) {
      if (entry.xid_t_ != xid && !Compatible(entry.lock_type_, lock_type) && entry.lock_granularity_ == LockGranularity::TABLE) {
          return false;
      }
  }
  // Handle upgrade
  for (auto &entry : resource_locks_[oid]) {
      if (entry.xid_t_ == xid && entry.lock_granularity_ == LockGranularity::TABLE) {
          entry.lock_type_ = Upgrade(entry.lock_type_, lock_type);
          return true;
      }
  }
  // Acquire the lock
  resource_locks_[oid].emplace_back(new_lock);

  return true;
}

bool LockManager::LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid) {
  // 对数据行加锁，成功加锁返回 true，如果数据行已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据行的锁，根据需要升级锁的类型
  // LAB 3 BEGIN

  ResourceLock new_row_lock{
          lock_type,
          LockGranularity::ROW,
          xid,
          rid,
  };

  // Iterate through all row locks for this data table
  // Check for incompatibility
  for (auto &entry : resource_locks_[oid]) {
      if (entry.rid_.slot_id_ == rid.slot_id_ && entry.rid_.page_id_ == rid.page_id_ &&
          entry.lock_granularity_ == LockGranularity::ROW &&
          entry.xid_t_ != xid &&
          !Compatible(entry.lock_type_, lock_type)) {
          return false;
      }
  }
  // Handle upgrade
  for (auto &entry : resource_locks_[oid]) {
      if (entry.rid_.slot_id_ == rid.slot_id_ && entry.rid_.page_id_ == rid.page_id_ &&
          entry.lock_granularity_ == LockGranularity::ROW &&
          entry.xid_t_ == xid) {
          entry.lock_type_ = Upgrade(entry.lock_type_, lock_type);
          return true;
      }
  }
  // Acquire the lock
  resource_locks_[oid].emplace_back(new_row_lock);

  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 BEGIN

  for (auto &[object_id, lock_list] : resource_locks_) {
      for (auto it = lock_list.begin(); it != lock_list.end();) {
          auto current_lock = *it;
          if (current_lock.xid_t_ == xid) {
              it = lock_list.erase(it);
          } else {
              ++it;
          }
      }
      resource_locks_[object_id] = lock_list;
  }
}

void LockManager::SetDeadLockType(DeadlockType deadlock_type) { deadlock_type_ = deadlock_type; }

bool LockManager::Compatible(LockType type_a, LockType type_b) const {
  // 判断锁是否相容
  // LAB 3 BEGIN

  return lock_compatibility_map_[static_cast<int>(type_a)][static_cast<int>(type_b)];
}

LockType LockManager::Upgrade(LockType self, LockType other) const {
  // 升级锁类型
  // LAB 3 BEGIN
  return lock_upgrade_map_[static_cast<int>(self)][static_cast<int>(other)];
}

}  // namespace huadb

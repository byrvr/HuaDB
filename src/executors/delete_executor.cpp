#include "executors/delete_executor.h"

namespace huadb {

DeleteExecutor::DeleteExecutor(ExecutorContext &context, std::shared_ptr<const DeleteOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
}

void DeleteExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> DeleteExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  uint32_t count = 0;
  while (auto record = children_[0]->Next()) {
    // 通过 context_ 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    auto &lock_mgr = context_.GetLockManager();
    auto transaction_id = context_.GetXid();
    auto object_id = table_->GetOid();
    auto record_id = record->GetRid();

    if (!lock_mgr.LockTable(transaction_id, LockType::IX, object_id)) {
      throw DbException("Failed to acquire IX lock on the table for deletion");
    }

    table_->DeleteRecord(record_id, context_.GetXid(), true);

    if (!lock_mgr.LockRow(transaction_id, LockType::X, object_id, record_id)) {
        throw DbException("Failed to acquire X lock on the row for deletion");
    }

    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb

#include "executors/update_executor.h"

namespace huadb {

UpdateExecutor::UpdateExecutor(ExecutorContext &context, std::shared_ptr<const UpdateOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void UpdateExecutor::Init() {
  children_[0]->Init();
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
}

std::shared_ptr<Record> UpdateExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  uint32_t count = 0;
  while (auto record = children_[0]->Next()) {
    std::vector<Value> values;
    for (const auto &expr : plan_->update_exprs_) {
      values.push_back(expr->Evaluate(record));
    }
    auto new_record = std::make_shared<Record>(std::move(values));
    // 通过 context_ 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN

    auto &lock_mgr = context_.GetLockManager();
    auto transaction_id = context_.GetXid();
    auto object_id = table_->GetOid();

    if (!lock_mgr.LockTable(transaction_id, LockType::IX, object_id)) {
        throw DbException("Failed to acquire IX lock on the table for update");
    }
    
    auto record_id = table_->UpdateRecord(record->GetRid(), context_.GetXid(), context_.GetCid(), new_record, true);

    if (!lock_mgr.LockRow(transaction_id, LockType::X, object_id, record_id)) {
        throw DbException("Failed to acquire X lock on the row for update");
    }

    if (!lock_mgr.LockRow(transaction_id, LockType::X, object_id, record->GetRid())) {
        throw DbException("Failed to acquire X lock on the row for update");
    }

    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb

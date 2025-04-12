#include "executors/insert_executor.h"

namespace huadb {

InsertExecutor::InsertExecutor(ExecutorContext &context, std::shared_ptr<const InsertOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void InsertExecutor::Init() {
  children_[0]->Init();
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
  column_list_ = context_.GetCatalog().GetTableColumnList(plan_->GetTableOid());
}

std::shared_ptr<Record> InsertExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  uint32_t count = 0;
  while (auto record = children_[0]->Next()) {
    std::vector<Value> values(column_list_.Length());
    const auto &insert_columns = plan_->GetInsertColumns().GetColumns();
    for (size_t i = 0; i < insert_columns.size(); i++) {
      auto column_index = column_list_.GetColumnIndex(insert_columns[i].GetName());
      values[column_index] = record->GetValue(i);
    }
    auto table_record = std::make_shared<Record>(std::move(values));
    // 通过 context_ 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    
    auto &lock_mgr = context_.GetLockManager();
    auto transaction_id = context_.GetXid();
    auto object_id = table_->GetOid();

    if (!lock_mgr.LockTable(transaction_id, LockType::IX, object_id)) {
        throw DbException("Failed to acquire IX lock on the table for insertion");
    }
    auto record_id = table_->InsertRecord(std::move(table_record), context_.GetXid(), context_.GetCid(), true);
    if (!lock_mgr.LockRow(transaction_id, LockType::X, object_id, record_id)) {
        throw DbException("Failed to acquire X lock on the row for insertion");
    }
    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb

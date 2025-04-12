#include "executors/seqscan_executor.h"

namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {}

void SeqScanExecutor::Init() {
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // 通过 context_ 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN

  auto transaction_id = context_.GetXid();
  auto client_id = context_.GetCid();
  auto isolation_level = context_.GetIsolationLevel();
  auto &trans_manager = context_.GetTransactionManager();

  // Repeatable Read / Serializable
  if (isolation_level == IsolationLevel::REPEATABLE_READ || isolation_level == IsolationLevel::SERIALIZABLE) {
      active_xids = trans_manager.GetSnapshot(transaction_id);
  }
  // Read Committed
  else if (isolation_level == IsolationLevel::READ_COMMITTED) {
      active_xids = trans_manager.GetActiveTransactions();
  }

  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  auto record = scan_->GetNextRecord(transaction_id, isolation_level, client_id, active_xids);
  auto object_id = table->GetOid();
  auto &lock_mgr = context_.GetLockManager();

  // Table lock IS
  if (!lock_mgr.LockTable(transaction_id, LockType::IS, object_id)) {
      throw DbException("Failed to acquire IS lock on the table");
  }
  return record;
}

}  // namespace huadb

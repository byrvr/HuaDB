#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

    bool IsVisible(IsolationLevel iso_level, xid_t xid, cid_t cid,
               const std::unordered_set<xid_t>& active_xids,
               const std::shared_ptr<Record>& record) {
    // Retrieve record transaction identifiers
    xid_t recordInsertXid = record->GetXmin();
    xid_t recordDeleteXid = record->GetXmax();
    cid_t recordInsertCid = record->GetCid();
    
    bool visible = true;
    
    if (iso_level == IsolationLevel::REPEATABLE_READ || iso_level == IsolationLevel::SERIALIZABLE) {
        // For REPEATABLE_READ and SERIALIZABLE isolation levels:
        // If the record is deleted, its deletion transaction is not active, and the deletion occurred before or at the current transaction, mark it invisible.
        if (record->IsDeleted() &&
            active_xids.find(recordDeleteXid) == active_xids.end() &&
            recordDeleteXid <= xid) {
            visible = false;
        }
        // Also, if the record's insertion transaction is still active or occurred after the current transaction, it is not visible.
        if (active_xids.find(recordInsertXid) != active_xids.end() || recordInsertXid > xid) {
            visible = false;
        }
    } else if (iso_level == IsolationLevel::READ_COMMITTED) {
        // For READ_COMMITTED isolation level:
        // Mark as invisible if the record is deleted and either its deletion transaction is not active or it was deleted by the current transaction.
        if (record->IsDeleted() &&
            (active_xids.find(recordDeleteXid) == active_xids.end() || xid == recordDeleteXid)) {
            visible = false;
        }
        // Additionally, if the insertion transaction is still active (and not the current transaction), the record is not visible.
        if (active_xids.find(recordInsertXid) != active_xids.end() && recordInsertXid != xid) {
            visible = false;
        }
    }
    
    // Prevent the Halloween problem: if the record was inserted by the current transaction with the same command id, it should not be visible.
    if (recordInsertXid == xid && recordInsertCid == cid) {
        visible = false;
    }
    
    return visible;
}


TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // 每次调用读取一条记录
  // 读取时更新 rid_ 变量，避免重复读取
  // 扫描结束时，返回空指针
  // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
  // LAB 1 BEGIN
  if (rid_.page_id_ == NULL_PAGE_ID) {
    return nullptr;
  }
  
  std::shared_ptr<Record> record = nullptr;
  
  while (true) {
    auto current_page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
    TablePage table_page(current_page);

    if (rid_.slot_id_ < table_page.GetRecordCount()) {
        record = table_page.GetRecord(rid_, table_->GetColumnList());
        rid_.slot_id_++;

        // Check record visibility
        if (!IsVisible(isolation_level, xid, cid, active_xids, record)) {
            continue;
        }
        break;
    } else if (table_page.GetNextPageId() != NULL_PAGE_ID) {
        // Switch to the next page and reset the slot index
        rid_.page_id_ = table_page.GetNextPageId();
        rid_.slot_id_ = 0;
        // Continue loop to process the new page
        continue;
    } else {
        rid_.page_id_ = NULL_PAGE_ID;
        rid_.slot_id_ = 0;
        return nullptr;
    }
    }
return record;
}
}  // namespace huadb

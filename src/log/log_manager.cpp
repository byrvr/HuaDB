#include "log/log_manager.h"

#include "common/exceptions.h"
#include "log/log_records/log_records.h"
#include "table/table_page.h"

namespace huadb {

LogManager::LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn)
    : disk_(disk), transaction_manager_(transaction_manager), next_lsn_(next_lsn), flushed_lsn_(next_lsn - 1) {}

void LogManager::SetBufferPool(std::shared_ptr<BufferPool> buffer_pool) { buffer_pool_ = std::move(buffer_pool); }

void LogManager::SetCatalog(std::shared_ptr<Catalog> catalog) { catalog_ = std::move(catalog); }

lsn_t LogManager::GetNextLSN() const { return next_lsn_; }

void LogManager::Clear() {
  std::unique_lock lock(log_buffer_mutex_);
  log_buffer_.clear();
}

void LogManager::Flush() { Flush(NULL_LSN); }

void LogManager::SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn) {
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
}

lsn_t LogManager::AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset,
                                  db_size_t size, char *new_record) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendInsertLog)");
  }
  auto log = std::make_shared<InsertLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id, offset, size, new_record);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendDeleteLog)");
  }
  auto log = std::make_shared<DeleteLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id) {
  if (xid != DDL_XID && att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendNewPageLog)");
  }
  xid_t log_xid;
  if (xid == DDL_XID) {
    log_xid = NULL_XID;
  } else {
    log_xid = att_.at(xid);
  }
  auto log = std::make_shared<NewPageLog>(NULL_LSN, xid, log_xid, oid, prev_page_id, page_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);

  if (xid != DDL_XID) {
    att_[xid] = lsn;
  }
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  if (prev_page_id != NULL_PAGE_ID && dpt_.find({oid, prev_page_id}) == dpt_.end()) {
    dpt_[{oid, prev_page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendBeginLog(xid_t xid) {
  if (att_.find(xid) != att_.end()) {
    throw DbException(std::to_string(xid) + " already exists in att");
  }
  auto log = std::make_shared<BeginLog>(NULL_LSN, xid, NULL_LSN);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  return lsn;
}

lsn_t LogManager::AppendCommitLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendCommitLog)");
  }
  auto log = std::make_shared<CommitLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::AppendRollbackLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendRollbackLog)");
  }
  auto log = std::make_shared<RollbackLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::Checkpoint(bool async) {
  auto begin_checkpoint_log = std::make_shared<BeginCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN);
  lsn_t begin_lsn = next_lsn_.fetch_add(begin_checkpoint_log->GetSize(), std::memory_order_relaxed);
  begin_checkpoint_log->SetLSN(begin_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(begin_checkpoint_log));
  }

  auto end_checkpoint_log = std::make_shared<EndCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN, att_, dpt_);
  lsn_t end_lsn = next_lsn_.fetch_add(end_checkpoint_log->GetSize(), std::memory_order_relaxed);
  end_checkpoint_log->SetLSN(end_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(end_checkpoint_log));
  }
  Flush(end_lsn);
  std::ofstream out(MASTER_RECORD_NAME);
  out << begin_lsn;
  return end_lsn;
}

void LogManager::FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn) {
  Flush(page_lsn);
  dpt_.erase({table_oid, page_id});
}

void LogManager::Rollback(xid_t xid) {
  // 在 att_ 中查找事务 xid 的最后一条日志的 lsn
  // 依次获取 lsn 的 prev_lsn_，直到 NULL_LSN
  // 根据 lsn 和 flushed_lsn_ 的大小关系，判断日志在 buffer 中还是在磁盘中
  // 若日志在 buffer 中，通过 log_buffer_ 获取日志
  // 若日志在磁盘中，通过 disk_ 读取日志，count 参数可设置为 MAX_LOG_SIZE
  // 通过 LogRecord::DeserializeFrom 函数解析日志
  // 调用日志的 Undo 函数
  // LAB 2 BEGIN

  // Retrieve the initial log sequence number for this transaction
  lsn_t current_sequence = att_.find(xid)->second;

  // Process all log entries for this transaction until reaching the beginning
  for (; current_sequence != NULL_LSN; ) {
    // Determine if the log record is in memory or on disk
    if (current_sequence > flushed_lsn_) {
      // Search in memory buffer for the log entry
      bool found = false;
      for (auto buffer_iter = log_buffer_.begin(); buffer_iter != log_buffer_.end(); ++buffer_iter) {
        std::shared_ptr<LogRecord> current_record = *buffer_iter;

        // Found the matching record in the buffer
        if (current_record->GetLSN() == current_sequence) {
          // Get the previous log entry in the chain before undoing
          lsn_t previous_sequence = current_record->GetPrevLSN();

          // Execute undo operation for this record
          current_record->Undo(*buffer_pool_, *catalog_, *this, previous_sequence);

          // Move to the previous record in the chain
          current_sequence = previous_sequence;
          found = true;
          break;
        }
      }

      // Sanity check - should always find the record
      if (!found) {
        throw std::runtime_error("Log record not found in buffer");
      }
    }
    else {
      // Retrieve record from persistent storage
      std::vector<char> log_data(MAX_LOG_SIZE);

      // Read the log entry from disk
      disk_.ReadLog(current_sequence, MAX_LOG_SIZE, log_data.data());

      // Deserialize the log record
      auto log_entry = LogRecord::DeserializeFrom(current_sequence, log_data.data());

      // Get the previous log entry in the chain
      lsn_t previous_sequence = log_entry->GetPrevLSN();

      // Execute undo operation for this record
      log_entry->Undo(*buffer_pool_, *catalog_, *this, previous_sequence);

      // Move to the previous record in the chain
      current_sequence = previous_sequence;
    }
  }
}

void LogManager::Recover() {
  Analyze();
  Redo();
  Undo();
}

void LogManager::IncrementRedoCount() { redo_count_++; }

uint32_t LogManager::GetRedoCount() const { return redo_count_; }

void LogManager::Flush(lsn_t lsn) {
  size_t max_log_size = 0;
  lsn_t max_lsn = NULL_LSN;
  {
    std::unique_lock lock(log_buffer_mutex_);
    for (auto iterator = log_buffer_.cbegin(); iterator != log_buffer_.cend();) {
      const auto &log_record = *iterator;
      // 如果 lsn 为 NULL_LSN，表示 log_buffer_ 中所有日志都需要刷盘
      if (lsn != NULL_LSN && log_record->GetLSN() > lsn) {
        iterator++;
        continue;
      }
      auto log_size = log_record->GetSize();
      auto log = std::make_unique<char[]>(log_size);
      log_record->SerializeTo(log.get());
      disk_.WriteLog(log_record->GetLSN(), log_size, log.get());
      if (max_lsn == NULL_LSN || log_record->GetLSN() > max_lsn) {
        max_lsn = log_record->GetLSN();
        max_log_size = log_size;
      }
      iterator = log_buffer_.erase(iterator);
    }
  }
  // 如果 max_lsn 为 NULL_LSN，表示没有日志刷盘
  // 如果 flushed_lsn_ 为 NULL_LSN，表示还没有日志刷过盘
  if (max_lsn != NULL_LSN && (flushed_lsn_ == NULL_LSN || max_lsn > flushed_lsn_)) {
    flushed_lsn_ = max_lsn;
    lsn_t next_lsn = FIRST_LSN;
    if (disk_.FileExists(NEXT_LSN_NAME)) {
      std::ifstream in(NEXT_LSN_NAME);
      in >> next_lsn;
    }
    if (flushed_lsn_ + max_log_size > next_lsn) {
      std::ofstream out(NEXT_LSN_NAME);
      out << (flushed_lsn_ + max_log_size);
    }
  }
}

void LogManager::Analyze() {
  // 恢复 Master Record 等元信息
  // 恢复故障时正在使用的数据库
  if (disk_.FileExists(NEXT_LSN_NAME)) {
    std::ifstream in(NEXT_LSN_NAME);
    lsn_t next_lsn;
    in >> next_lsn;
    next_lsn_ = next_lsn;
  } else {
    next_lsn_ = FIRST_LSN;
  }
  flushed_lsn_ = next_lsn_ - 1;
  lsn_t checkpoint_lsn = 0;

  if (disk_.FileExists(MASTER_RECORD_NAME)) {
    std::ifstream in(MASTER_RECORD_NAME);
    in >> checkpoint_lsn;
  }
  // 根据 Checkpoint 日志恢复脏页表、活跃事务表等元信息
  // 必要时调用 transaction_manager_.SetNextXid 来恢复事务 id
  // LAB 2 BEGIN

  // Initialize sequence number tracking to start from checkpoint
  lsn_t current_position = checkpoint_lsn;
  // Set recovery starting position
  smallest_sequence_num_ = checkpoint_lsn;

  // Allocate buffer for reading log records
  std::vector<char> buffer_memory(MAX_LOG_SIZE);
  char* log_buffer = buffer_memory.data();

  // PHASE 1: Scan for END_CHECKPOINT record to retrieve transaction and dirty page state
  while (current_position < next_lsn_) {
    // Read log record from persistent storage
    disk_.ReadLog(current_position, MAX_LOG_SIZE, log_buffer);

    // Deserialize the log record
    std::shared_ptr<LogRecord> log_entry = LogRecord::DeserializeFrom(current_position, log_buffer);

    // Check if this is the end checkpoint record we're looking for
    if (log_entry->GetType() == LogType::END_CHECKPOINT) {
      // Cast to checkpoint record type to access tables
      auto checkpoint_record = std::dynamic_pointer_cast<EndCheckpointLog>(log_entry);

      // Restore active transaction table and dirty page table from checkpoint
      att_ = checkpoint_record->GetATT();
      dpt_ = checkpoint_record->GetDPT();

      // Exit the first scan loop once we find the checkpoint record
      break;
    }

    current_position += log_entry->GetSize();
  }

  // PHASE 2: Process all logs after checkpoint to rebuild state
  current_position = checkpoint_lsn;

  // Scan forward through all logs after checkpoint
  while (current_position < next_lsn_) {
    // Read and deserialize log record
    disk_.ReadLog(current_position, MAX_LOG_SIZE, log_buffer);
    std::shared_ptr<LogRecord> log_entry = LogRecord::DeserializeFrom(current_position, log_buffer);

    // Get transaction ID from log record
    xid_t transaction_id = log_entry->GetXid();

    // Process based on log record type
    LogType record_type = log_entry->GetType();

    // Handle transaction records (INSERT, DELETE, NEW_PAGE)
    bool is_modification_record = (record_type == LogType::INSERT ||
                                  record_type == LogType::DELETE ||
                                  record_type == LogType::NEW_PAGE);

    // Update active transaction table for modification records
    if (is_modification_record) {
      att_[transaction_id] = current_position;
    }

    // Handle transaction commit
    if (record_type == LogType::COMMIT) {
      // Remove from active transactions if present
      if (att_.find(transaction_id) != att_.end()) {
        att_.erase(transaction_id);
      }
    }

    // Update transaction ID counter if needed
    if (transaction_id > transaction_manager_.GetNextXid()) {
      transaction_manager_.SetNextXid(transaction_id);
    }

    // Extract page information from record
    auto coordinates = ExtractRecordCoordinates(log_entry);
    oid_t object_id = coordinates.first;
    pageid_t page_location = coordinates.second;

    // Update dirty page table for modification records
    if (is_modification_record) {
      // Add to dirty page table if not already present
      if (dpt_.find({object_id, page_location}) == dpt_.end()) {
        dpt_[{object_id, page_location}] = current_position;
      }
    }

    // Move to next log record
    current_position += log_entry->GetSize();
  }
}

void LogManager::Redo() {
  // 正序读取日志，调用日志记录的 Redo 函数
  // LAB 2 BEGIN

  // Start from the recovery beginning position
  lsn_t current_lsn = smallest_sequence_num_;

  // Find the earliest log entry that needs to be redone
  for (const auto& dirty_page_entry : dpt_) {
    lsn_t recovery_lsn = dirty_page_entry.second;
    if (recovery_lsn < current_lsn) {
      current_lsn = recovery_lsn;
    }
  }

  // Use vector for safer memory management
  std::vector<char> log_storage(MAX_LOG_SIZE);
  char* log_buffer = log_storage.data();

  // Process all logs from earliest needed LSN to the latest
  while (current_lsn < next_lsn_) {
    // Read and deserialize the log record
    disk_.ReadLog(current_lsn, MAX_LOG_SIZE, log_buffer);
    auto log_entry = LogRecord::DeserializeFrom(current_lsn, log_buffer);

    // Extract page information
    auto record_location = ExtractRecordCoordinates(log_entry);
    oid_t object_id = record_location.first;
    pageid_t page_location = record_location.second;

    // Check if this is a modification record
    bool is_data_modification = (log_entry->GetType() == LogType::INSERT ||
                                log_entry->GetType() == LogType::DELETE ||
                                log_entry->GetType() == LogType::NEW_PAGE);

    if (is_data_modification) {
      // Check if page is in dirty page table
      if (dpt_.find({object_id, page_location}) != dpt_.end()) {
        // Get recovery LSN for this page
        lsn_t recovery_lsn = dpt_[{object_id, page_location}];

        // Only redo if this log entry should be applied
        if (current_lsn >= recovery_lsn) {
          if (log_entry->GetType() == LogType::NEW_PAGE) {
            // For new pages, always apply the redo operation
            log_entry->Redo(*buffer_pool_, *catalog_, *this);
          } else {
            // For existing pages, check the page LSN first
            oid_t database_id = catalog_->GetDatabaseOid(object_id);
            auto page_ptr = buffer_pool_->GetPage(database_id, object_id, page_location);
            TablePage table_page(page_ptr);
            lsn_t page_sequence_num = table_page.GetPageLSN();

            // Only redo if the page hasn't been refreshed after this log entry
            if (current_lsn > page_sequence_num) {
              log_entry->Redo(*buffer_pool_, *catalog_, *this);
            }
          }
        }
      }
    }

    // Move to the next log entry
    current_lsn += log_entry->GetSize();
  }
}

void LogManager::Undo() {
  // 根据活跃事务表，将所有活跃事务回滚
  // LAB 2 BEGIN

  // Iterate through all entries in the active transaction table
  auto transaction_iter = att_.begin();
  while (transaction_iter != att_.end()) {
    // Extract transaction ID from the current entry
    xid_t transaction_id = transaction_iter->first;

    // Perform rollback operation for this transaction
    Rollback(transaction_id);

    // Move to next transaction in the table
    ++transaction_iter;
  }

}

std::pair<oid_t, pageid_t> LogManager::ExtractRecordCoordinates(const std::shared_ptr<LogRecord> &log_entry) {
  // Initialize return values
  oid_t entity_identifier = 0;
  pageid_t page_location = 0;

  // Extract information based on log entry type
  const LogType entry_type = log_entry->GetType();

  // Handle INSERT records
  if (entry_type == LogType::INSERT) {
    auto insert_entry = std::dynamic_pointer_cast<InsertLog>(log_entry);
    if (insert_entry) {
      page_location = insert_entry->GetPageId();
      entity_identifier = insert_entry->GetOid();
    }
  }
  // Handle DELETE records
  else if (entry_type == LogType::DELETE) {
    auto removal_entry = std::dynamic_pointer_cast<DeleteLog>(log_entry);
    if (removal_entry) {
      page_location = removal_entry->GetPageId();
      entity_identifier = removal_entry->GetOid();
    }
  }
  // Handle NEW_PAGE records
  else if (entry_type == LogType::NEW_PAGE) {
    auto page_creation_entry = std::dynamic_pointer_cast<NewPageLog>(log_entry);
    if (page_creation_entry) {
      page_location = page_creation_entry->GetPageId();
      entity_identifier = page_creation_entry->GetOid();
    }
  }
  // Other log types remain with default values (implicit)

  // Return the coordinate pair
  return {entity_identifier, page_location};
}

}  // namespace huadb

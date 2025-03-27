# Databases Lab2 Report: Transaction Processing and Fault Recovery

###### 罗兰 Ruslan | 2021080066

## Introduction

This report documents the implementation of transaction processing and fault recovery mechanisms in a database system. The project focuses on ensuring ACID properties by implementing proper logging mechanisms and the ARIES (Algorithm for Recovery and Isolation Exploiting Semantics) recovery algorithm. This implementation guarantees that data remains consistent even in the event of system failures during transaction processing.

## 1. Transaction Rollback Implementation

### 1.1 Log Appending Mechanism

When modifying pages in the database, it's essential to record these changes in logs for potential rollback. I implemented the logging mechanism for three primary operations:

- **Insert Record**: Records details about the inserted data, including table OID, database OID, page ID, slot ID, offset, record length, and serialized data.
- **Delete Record**: Records information about the deleted record, including table OID, database OID, page ID, and slot ID.
- **New Page Creation**: Records information about newly created pages, including table OID, database OID, previous page ID, and new page ID.

These logs provide sufficient information to undo operations when rolling back transactions.

### 1.2 Undo Log Retrieval

The central component of the rollback mechanism is the `LogManager::Rollback` function. This function:

1. Retrieves the last log sequence number (LSN) for the transaction from the active transaction table (ATT)
2. Iteratively processes log records in reverse chronological order using the `prev_lsn_` field
3. Determines whether the log record is in memory or on disk based on comparison with `flushed_lsn_`
4. Retrieves the log record from the appropriate location (memory buffer or disk)
5. Calls the `Undo` function for each record

Here's a code snippet showing the core logic for traversing transaction logs:

```cpp
// Starting from the most recent log for this transaction
lsn_t current_sequence = att_.find(xid)->second;

// Process all logs in reverse order
for (; current_sequence != NULL_LSN; ) {
    // Determine if log is in memory or on disk
    if (current_sequence > flushed_lsn_) {
        // Retrieve from memory buffer
        // ...
    } else {
        // Retrieve from disk
        std::vector<char> log_data(MAX_LOG_SIZE);
        disk_.ReadLog(current_sequence, MAX_LOG_SIZE, log_data.data());
        auto log_entry = LogRecord::DeserializeFrom(current_sequence, log_data.data());

        // Get previous log in chain
        lsn_t previous_sequence = log_entry->GetPrevLSN();

        // Undo this operation
        log_entry->Undo(*buffer_pool_, *catalog_, *this, previous_sequence);

        // Move to previous log
        current_sequence = previous_sequence;
    }
}
```

### 1.3 Undo Operations Implementation

For each log type, I implemented specific undo operations:

- **InsertLog::Undo**: Removes records that were inserted during the transaction by marking them as deleted.
- **DeleteLog::Undo**: Restores records that were deleted during the transaction by clearing their deletion flags.
- **NewPageLog::Undo**: Fixes page links when a newly created page needs to be logically removed during rollback.

The `TablePage::UndoDeleteRecord` function clears the delete flag for records that were deleted during a transaction:

```cpp
void TablePage::UndoDeleteRecord(slotid_t slot_id) {
    auto record_offset = slots_[slot_id].offset_;
    auto *record_ptr = page_data_ + record_offset;
    record_ptr[0] = 0;  // Clear delete flag
}
```

## 2. Redo Implementation

### 2.1 Redo Log Retrieval

For redo operations, I implemented the `LogManager::Redo` function to sequentially process logs:

1. Starting from the earliest relevant LSN (determined by minimum value in dirty page table)
2. Reading and deserializing each log record sequentially
3. Determining if the log needs to be applied based on page LSN comparisons
4. Calling the appropriate `Redo` function for each log record

### 2.2 Redo Operations Implementation

For each log type, I implemented corresponding redo operations:

- **InsertLog::Redo**: Re-applies insert operations using the serialized record data.
- **DeleteLog::Redo**: Re-applies delete operations to mark records as deleted.
- **NewPageLog::Redo**: Re-creates pages and updates page links.

The `TablePage::RedoInsertRecord` function is particularly important, as it restores inserted records with their exact original position and content:

```cpp
void TablePage::RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size) {
    // Update page pointers
    *upper_ -= record_size;
    *lower_ += sizeof(Slot);

    // Update slot information
    Slot slot_entry = {
        page_offset,
        record_size
    };
    slots_[slot_id] = slot_entry;

    // Copy record data
    char *destination_ptr = page_data_ + page_offset;
    memcpy(destination_ptr, raw_record, record_size);

    page_->SetDirty();
}
```

## 3. ARIES Recovery Algorithm Implementation

The ARIES recovery algorithm consists of three phases: Analysis, Redo, and Undo. This design ensures efficient and correct recovery from system failures.

### 3.1 Analysis Phase Implementation

In the `LogManager::Analyze` function, I implemented the analysis phase with the following steps:

1. Start from the checkpoint LSN
2. First, locate the END_CHECKPOINT record to retrieve the initial state of active transaction and dirty page tables
3. Then process all subsequent logs to update these tables based on new operations
4. Maintain transaction ID counter for consistent transaction numbering

Analysis phase code snippet:

```cpp
// Process all logs after checkpoint to rebuild state
current_position = checkpoint_lsn;

while (current_position < next_lsn_) {
    // Read and deserialize log record
    disk_.ReadLog(current_position, MAX_LOG_SIZE, log_buffer);
    std::shared_ptr<LogRecord> log_entry = LogRecord::DeserializeFrom(current_position, log_buffer);

    // Get transaction ID from log record
    xid_t transaction_id = log_entry->GetXid();
    LogType record_type = log_entry->GetType();

    // Handle transaction records
    bool is_modification_record = (record_type == LogType::INSERT ||
                                  record_type == LogType::DELETE ||
                                  record_type == LogType::NEW_PAGE);

    // Update active transaction table
    if (is_modification_record) {
        att_[transaction_id] = current_position;
    }

    // Handle transaction commit
    if (record_type == LogType::COMMIT) {
        if (att_.find(transaction_id) != att_.end()) {
            att_.erase(transaction_id);
        }
    }

    // Update dirty page table for modification records
    if (is_modification_record) {
        auto coordinates = ExtractRecordCoordinates(log_entry);
        oid_t object_id = coordinates.first;
        pageid_t page_location = coordinates.second;

        if (dpt_.find({object_id, page_location}) == dpt_.end()) {
            dpt_[{object_id, page_location}] = current_position;
        }
    }

    current_position += log_entry->GetSize();
}
```

### 3.2 Optimized Redo Phase

I optimized the redo phase implementation to avoid unnecessary redo operations:

1. Start from the minimum recovery LSN found in the dirty page table
2. For each log record, check if the affected page is in the dirty page table
3. Only perform redo if the log's LSN is greater than or equal to the recovery LSN for that page
4. For existing pages, additionally verify the page LSN to avoid redundant operations

This optimization significantly improves recovery performance by limiting operations to only what's necessary.

### 3.3 Undo Phase Implementation

The undo phase rolls back all active transactions identified during analysis:

1. Iterate through all entries in the active transaction table
2. Call the `Rollback` function for each transaction
3. This leverages the transaction rollback mechanism implemented in Task 1

```cpp
void LogManager::Undo() {
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
```

### 3.4 Helper Functions

To support the ARIES implementation, I created several helper functions:

- `ExtractRecordCoordinates`: Extracts object ID and page ID from different log record types, unifying page coordinate access
- Type-specific deserialization functions to properly reconstruct log records from raw data
- LSN tracking mechanisms to determine recovery start points

## 4. Implementation Challenges and Solutions

### 4.1 LSN Management

Unlike the theoretical approach where LSNs are simple sequential integers, the implementation uses file positions as LSNs. This required careful handling of LSN comparisons and management to ensure proper ordering during recovery.

### 4.2 Page LSN Tracking

Tracking page LSNs was crucial for determining whether a redo operation should be applied. I implemented proper page LSN updates with each modification and comparisons during recovery to avoid redundant operations.

### 4.3 Log Record Buffer Management

Logs could be either in memory or on disk, requiring two distinct retrieval mechanisms. I implemented a unified approach that checks the `flushed_lsn_` value to determine the correct retrieval method.

## 5. Testing and Verification

The implementation successfully passes all required test cases:

- **10-rollback.test**: Verifies transaction rollback for insert, delete, update, and new page operations
- **20-recover.test**: Tests redo operations for the same set of operations
- **30-aries.test**: Validates the complete ARIES recovery algorithm

For each test, I verified correct behavior by examining:

1. Data consistency after recovery
2. Proper LSN handling
3. Correct log record application

## Conclusion

This implementation provides a robust transaction processing and fault recovery system based on the ARIES algorithm. The physical logging approach combined with efficient redo and undo operations ensures that the database maintains ACID properties even in the presence of system failures.

The modular design allows for future extensions, such as implementing compensation logs for failures during undo (as suggested in the advanced tasks). The current implementation successfully balances recovery correctness with performance considerations, particularly in the optimized redo phase that avoids unnecessary operations.

Future work could focus on implementing the advanced features mentioned in the project requirements, particularly the non-blocking checkpoint mechanism and failure recovery during undo operations.

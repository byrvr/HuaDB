# Databases Lab2 Report: Transaction Processing and Fault Recovery

###### 罗兰 Ruslan | 2021080066

## Basic Functionality Implementation and Challenges

### 1. Transaction Rollback Implementation

#### 1.1 Log Appending Mechanism

When modifying pages in the database, it's essential to record these changes in logs for potential rollback. I implemented the logging mechanism for three primary operations:

- **Insert Record**: Records details about the inserted data, including table OID, database OID, page ID, slot ID, offset, record length, and serialized data.
- **Delete Record**: Records information about the deleted record, including table OID, database OID, page ID, and slot ID.
- **New Page Creation**: Records information about newly created pages, including table OID, database OID, previous page ID, and new page ID.

These logs provide sufficient information to undo operations when rolling back transactions.

#### 1.2 Undo Log Retrieval

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

#### 1.3 Undo Operations Implementation

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

### 2. Redo Implementation

#### 2.1 Implementation Approach

For redo operations, I implemented the `LogManager::Redo` function to sequentially process logs:

1. Starting from the earliest relevant LSN (determined by minimum value in dirty page table)
2. Reading and deserializing each log record sequentially
3. Determining if the log needs to be applied based on page LSN comparisons
4. Calling the appropriate `Redo` function for each log record

A key challenge was determining when redo operations should be applied. Since the system uses the WAL (Write-Ahead Logging) protocol, it's important to only redo operations whose effects may not be persisted in the database files. I solved this by comparing log LSNs with page LSNs and only applying changes when necessary.

#### 2.2 Redo Operations Implementation

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

### 3. ARIES Recovery Algorithm Implementation

The ARIES recovery algorithm consists of three phases: Analysis, Redo, and Undo. This design ensures efficient and correct recovery from system failures.

#### 3.1 Analysis Phase Implementation

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

#### 3.2 Optimized Redo Phase

I optimized the redo phase implementation to avoid unnecessary redo operations:

1. Start from the minimum recovery LSN found in the dirty page table
2. For each log record, check if the affected page is in the dirty page table
3. Only perform redo if the log's LSN is greater than or equal to the recovery LSN for that page
4. For existing pages, additionally verify the page LSN to avoid redundant operations

This optimization significantly improves recovery performance by limiting operations to only what's necessary.

#### 3.3 Undo Phase Implementation

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

## Implementation Challenges and Solutions

### 1. LSN Management

Unlike the theoretical approach where LSNs are simple sequential integers, the implementation uses file positions as LSNs. This required careful handling of LSN comparisons and management to ensure proper ordering during recovery.

The LSN implementation presents a unique challenge because it's tied to the physical location in the log file rather than being a simple sequence number. This design choice simplifies log retrieval but complicates tracking log relationships. To address this, I created the `ExtractRecordCoordinates` helper function which standardizes page identification across different log types.

### 2. Checkpoint Handling

Finding and processing the checkpoint correctly was a key challenge. The implementation needed to first locate the END_CHECKPOINT record from the checkpoint_lsn and then restore the saved state before processing subsequent logs. This two-phase approach ensures that the recovery process starts from a known consistent state.

### 3. Transaction State Management

Keeping track of which transactions were active at the time of failure was challenging. The solution involved careful maintenance of the Active Transaction Table (ATT), particularly when processing COMMIT logs to remove completed transactions.

### 4. Memory Buffer vs. Disk Log Handling

The framework manages logs both in memory and on disk, requiring two different retrieval mechanisms. I implemented a unified approach that checks against the `flushed_lsn_` value to determine where to retrieve logs from. This approach keeps the code clean while handling both scenarios correctly.

### 5. Framework-Specific Challenges

One counter-intuitive aspect of the framework is that it uses physical logs for both redo and undo operations, which differs from the theoretical ARIES approach discussed in the course. Adapting to this design required understanding that record deletion is handled by marking rather than physical removal, and there are no index operations to consider in the current implementation.

## Testing and Verification

The implementation successfully passes all required test cases:

- **10-rollback.test**: Verifies transaction rollback for insert, delete, update, and new page operations
- **20-recover.test**: Tests redo operations for the same set of operations
- **30-aries.test**: Validates the complete ARIES recovery algorithm

For each test, I verified correct behavior by examining:

1. Data consistency after recovery
2. Proper LSN handling
3. Correct log record application

My implementation strategy involved incrementally implementing and testing each type of log operation. This made it easier to isolate and fix issues specific to each operation type before moving on to the next phase of implementation.

## Advanced Functionality Design (Not Implemented)

While I focused on completing the basic functionality with high quality, I've researched the design approaches for the advanced features:

### 1. Failure Recovery During Undo

To handle failures during the undo process, compensation log records (CLRs) would need to be implemented. These special log records:

- Would contain information to compensate for an undo operation
- Would have a specific "next undo LSN" pointer to skip already undone operations
- Would not themselves be undone during recovery

### 2. Non-blocking Checkpoint Mechanism

For a non-blocking checkpoint implementation, the `LogManager::Checkpoint` function would need to:

- Create a snapshot of the current state asynchronously
- Use a separate thread for writing checkpoint data to disk
- Employ proper synchronization mechanisms to ensure consistency

## Time Spent on the Project

- Understanding the framework and project requirements: ~5 hours
- Implementing Task 1 (Transaction Rollback): ~8 hours
- Implementing Task 2 (Redo): ~6 hours
- Implementing Task 3 (ARIES Recovery): ~10 hours
- Testing and debugging: ~8 hours
- Report writing: ~3 hours
- Total time: ~40 hours

## Honor Code

1. I have completed this assignment independently and have not copied code from any other student or external source. Where I have discussed implementation strategies with others or referenced existing resources, I have explicitly noted this in my report.
2. I have not used GitHub Copilot, ChatGPT, or any other AI tools for automatic code completion in this project.
3. I have not and will not share my code in any public repository or with other students in ways that might facilitate academic dishonesty.
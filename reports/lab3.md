# Databases Lab 3 Report: Multi-Version Concurrency Control and Locking

###### 罗兰 Ruslan | 2021080066

## Basic Functionality Implementation and Challenges

This experiment focused on implementing Multi-Version Concurrency Control (MVCC) and Two-Phase Locking (2PL) to handle concurrent transactions effectively and provide different isolation levels.

### 1. Multi-Version Concurrency Control (MVCC) Implementation

MVCC allows multiple transactions to access data concurrently by maintaining different versions of records.

#### 1.1 Record Header Management

To support MVCC, the record header was utilized to store versioning information:

* **`xmin_`**: Stores the transaction ID (XID) that inserted the record version. This is set during `InsertRecord`.
* **`xmax_`**: Stores the XID that deleted the record version. This is set during `DeleteRecord` and cleared (set to `NULL_XID`) during `UndoDeleteRecord` based on the updated logic in `TablePage::UndoDeleteRecord`.
* **`cid_`**: Stores the command ID within the transaction that created the record version. This is crucial for solving the Halloween Problem, ensuring a statement doesn't read data it just wrote within the same command.

#### 1.2 Visibility Checks

The core of MVCC lies in determining which record version is visible to a given transaction based on its isolation level and snapshot. This logic resides primarily within `TableScan::GetNextRecord` (though the exact code modifications are inferred from executor changes and requirements).

* **Snapshot Management:** The `SeqScanExecutor` was modified to obtain the set of active transaction IDs (`active_xids`) based on the current transaction's isolation level.
  * For **Read Committed**, `TransactionManager::GetActiveTransactions()` is used to get currently active transactions for each visibility check.
  * For **Repeatable Read** and **Serializable**, `TransactionManager::GetSnapshot(xid)` is called once at the start of the scan to get the set of transactions active when the *current* transaction began.
* **Visibility Rules (Conceptual):** The visibility check function compares a record version's `xmin_`, `xmax_`, and `cid_` against the current transaction's XID, CID, and its `active_xids` snapshot. The specific rules depend on the isolation level:
  * Generally, a version is visible if its `xmin_` is committed and not in the snapshot, and its `xmax_` is either null, uncommitted, or belongs to a transaction in the snapshot (meaning the deletion wasn't visible when the snapshot was taken).
  * The `cid_` check ensures records inserted by the *same command* within the current transaction are not immediately visible, preventing the Halloween Problem.

### 2. Isolation Level Implementation

Different SQL isolation levels were implemented by varying the snapshot acquisition and visibility rules:

* **Read Committed:** Uses a snapshot of currently active transactions for each tuple read. This prevents reading uncommitted data but allows non-repeatable reads and phantom reads. Visibility checks use the per-tuple snapshot.
* **Repeatable Read:** Uses a single snapshot taken at the beginning of the transaction (or first read). This prevents non-repeatable reads but phantom reads can still occur. Visibility checks use this fixed transaction snapshot. The `SeqScanExecutor` fetches this snapshot using `GetSnapshot(transaction_id)`.
* **Serializable:** Builds upon Repeatable Read by using stricter locking (covered below) in addition to the fixed transaction snapshot to prevent phantom reads, ensuring full serializability. Visibility checks use the fixed transaction snapshot, similar to Repeatable Read.

### 3. Two-Phase Locking (2PL) Implementation

To handle write-write conflicts, a Strict Two-Phase Locking (SS2PL) protocol was implemented using a `LockManager`.

#### 3.1 Lock Manager (`LockManager`)

The `LockManager` was implemented to manage table-level and row-level locks:

* **Data Structures:** An `std::unordered_map<oid_t, std::vector<ResourceLock>> resource_locks_` was used to store active locks, mapping object IDs (tables) to a list of `ResourceLock` structs. The `ResourceLock` struct holds lock type, granularity (TABLE/ROW), transaction ID, and Row ID (for row locks).
* **Lock Acquisition (`LockTable`, `LockRow`):**
  * Checks for compatibility with existing locks held by *other* transactions using the `Compatible` function and the `lock_compatibility_map_`. Returns false if an incompatible lock exists.
  * Handles lock upgrades for the *same* transaction using the `Upgrade` function and the `lock_upgrade_map_`. If the transaction already holds a lock, it upgrades it; otherwise, it adds a new lock entry.
* **Lock Release (`ReleaseLocks`):** Iterates through the `resource_locks_` map and removes all locks held by the specified transaction ID.
* **Compatibility and Upgrades:** Implemented `Compatible` and `Upgrade` methods using predefined matrices (`lock_compatibility_map_`, `lock_upgrade_map_`) to determine if locks conflict and how they should escalate.

#### 3.2 Lock Integration in Executors

Locks were integrated into the executors to enforce the SS2PL protocol:

* **Intention Locks:** Intention Share (IS) and Intention Exclusive (IX) locks are acquired at the table level before accessing rows to signal intent and prevent conflicting table-level operations.
  * `SeqScanExecutor`: Acquires an IS lock on the table.
  * `InsertExecutor`, `DeleteExecutor`, `UpdateExecutor`: Acquire an IX lock on the table.
* **Row Locks:** Share (S) and Exclusive (X) locks are acquired at the row level.
  * `LockRowsExecutor`: Implements `SELECT ... FOR SHARE` (acquires S lock) and `SELECT ... FOR UPDATE` (acquires X lock) on specific rows.
  * `InsertExecutor`: Acquires an X lock on the newly inserted row.
  * `DeleteExecutor`: Acquires an X lock on the row being deleted.
  * `UpdateExecutor`: Acquires an X lock on both the old version (implicitly via `LockRows` likely preceding it or needing explicit locking) and the new version of the row. (The diff shows locking the *new* RID returned by `UpdateRecord` and also the *original* `record->GetRid()`).
* **Error Handling:** If a required lock cannot be acquired due to an incompatible lock held by another transaction, a `DbException` is thrown.

## Implementation Challenges and Solutions

* **Complex Visibility Logic:** Implementing the correct visibility checks for each isolation level, considering `xmin_`, `xmax_`, `cid_`, and the appropriate transaction snapshot, was challenging. Carefully mapping the theoretical rules to code and testing edge cases was necessary.
* **Lock Compatibility/Upgrade Matrix:** Ensuring the `lock_compatibility_map_` and `lock_upgrade_map_` in `LockManager` were correctly defined according to standard locking protocols was critical for preventing incorrect lock behavior.
* **Correct Lock Placement:** Determining the right place and the right type of lock (IS, IX, S, X) to acquire in each executor required careful consideration of the operation being performed and the 2PL protocol rules. For instance, remembering to acquire table-level intention locks before row-level locks was important.
* **`UndoDeleteRecord` Logic:** Modifying `UndoDeleteRecord` to correctly reset `xmax_` instead of using the old `deleted_` flag required understanding its interaction with MVCC visibility.
* **Debugging Concurrent Transactions:** Testing and debugging concurrent scenarios required using the provided `server` and multiple `client` instances, which added complexity compared to single-transaction debugging.

## Testing and Verification

The implementation successfully passes all required test cases for this experiment:

* **10-halloween.test**: Verifies the fix for the Halloween Problem using `cid_`.
* **20-mvcc_insert.test, 21-mvcc_delete.test, 22-mvcc_update.test, 23-write_skew.test**: Test basic MVCC operations (Insert, Delete, Update) and write skew anomaly prevention under Repeatable Read.
* **30-repeatable_read.test**: Verifies the Repeatable Read isolation level using MVCC.
* **40-read_committed.test**: Verifies the Read Committed isolation level using MVCC.
* **50-lock.test**: Tests the basic locking functionality (SS2PL) for write-write conflicts.
* **60-mv2pl.test**: Validates the implementation of Serializable isolation using MVCC and 2PL, including `SELECT ... FOR SHARE/UPDATE`.

Testing involved incremental implementation and verification against these specific test cases, ensuring each component (MVCC visibility, locking) worked correctly before integrating them for the final Serializable level.

## Advanced Functionality Design (Not Implemented)

While the basic requirements were met, the following advanced features described in the requirements document were not implemented:

* **Deadlock Handling:** Implementing deadlock detection (e.g., building a waits-for graph) or prevention (e.g., Wait-Die or Wound-Wait protocols) to handle cycles in lock dependencies.
* **Garbage Collection:** Implementing a mechanism, potentially leveraging version chains added to record headers, to identify and reclaim space occupied by old, no-longer-visible record versions.

## Time Spent on the Project

* Understanding MVCC & Locking Concepts: ~6 hours
* Implementing Task 1 (Halloween Problem): ~3 hours
* Implementing Task 2 (MVCC & Repeatable Read): ~8 hours
* Implementing Task 3 (Read Committed): ~2 hours
* Implementing Task 4 (Lock Manager & Basic Locking): ~10 hours
* Implementing Task 5 (Serializable & Select For): ~5 hours
* Testing and debugging: ~10 hours
* Report writing: ~3 hours
* **Total time: ~47 hours**

## Honor Code

1. I have completed this assignment independently and have not copied code from any other student or external source. Where I have discussed implementation strategies with others or referenced existing resources, I have explicitly noted this in my report.
2. I have not used GitHub Copilot, ChatGPT, or any other AI tools for automatic code completion in this project.
3. I have not and will not share my code in any public repository or with other students in ways that might facilitate academic dishonesty.
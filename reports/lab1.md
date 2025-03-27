# Databases Lab1 Report: Database Storage Engine Implementation
###### Ruslan | 2021080066
## Introduction
This report details the implementation of fundamental components in a database storage engine, focusing on buffer management strategies, record operations, and transaction visibility controls. The lab required implementing key functionalities including LRU buffer management, table record operations, and a table scan mechanism with proper transaction isolation.

## Implementation Approach and Challenges

### LRU Buffer Strategy
The Least Recently Used (LRU) buffer strategy was implemented as a replacement policy for managing the database buffer pool. The implementation uses a standard C++ `std::list` to track the access order of buffer frames:

```cpp
class LRUBufferStrategy : public BufferStrategy {
 public:
  void Access(size_t frame_no) override;
  size_t Evict() override;

 private:
  std::list<size_t> access_order_;
};
```

When a page is accessed, the implementation:
1. Removes the frame number from its current position in the list (if present)
2. Places the frame at the front of the list, marking it as most recently used

```cpp
void LRUBufferStrategy::Access(size_t frame_no) {
  access_order_.remove(frame_no);
  access_order_.emplace_front(frame_no);
}
```

For eviction, the strategy selects the least recently used page (at the back of the list):

```cpp
size_t LRUBufferStrategy::Evict() {
  auto lastBuffer = access_order_.back();
  access_order_.pop_back();
  return lastBuffer;
}
```

This implementation ensures O(1) complexity for most recently accessed elements while maintaining the chronological order of access.

### Table Record Operations

#### Record Insertion
The record insertion process involves several key steps:
1. Finding a page with sufficient free space
2. Creating a new page if necessary
3. Inserting the record and updating metadata
4. Logging the operation if required

A significant challenge was handling the case when the table is empty (no pages yet). The solution involves initializing the first page and setting up the appropriate page pointers:

```cpp
if (first_page_id_ == NULL_PAGE_ID) {
  first_page_id_ = 0;
  auto firstPage = buffer_pool_.NewPage(db_oid_, oid_, first_page_id_);
  TablePage firstTablePage(firstPage);
  firstTablePage.Init();
  // Further operations...
}
```

Another challenge was managing the linked-list structure of table pages, especially when traversing pages to find one with sufficient space, and creating new pages when necessary.

#### Record Deletion
The record deletion process was implemented as a soft delete, where the record remains in storage but is marked as deleted. This approach simplifies transaction management and enables potential undoing of operations:

```cpp
void TablePage::DeleteRecord(slotid_t slot_id, xid_t xid) {
  auto slotOffset = slots_[slot_id].offset_;
  auto* recPtr = page_data_ + slotOffset;
  
  // Set the first byte of the record to 1 to mark it as deleted
  recPtr[0] = 1;
}
```

### Table Page Management
The table page implementation handles the low-level storage layout with slot arrays and record data. Key aspects include:

1. Managing the slot array and free space within pages
2. Implementing lower and upper pointers to track page utilization
3. Proper serialization and deserialization of records

One complexity was calculating the correct offsets for record placement and ensuring that page metadata remained consistent:

```cpp
slotid_t TablePage::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid) {
  record->SetXmin(xid);
  record->SetCid(cid);
  
  auto slotId = (*lower_ - PAGE_HEADER_SIZE) / sizeof(Slot);
  *upper_ -= record->GetSize();
  *lower_ += sizeof(Slot);
  
  Slot newSlot{*upper_, record->GetSize()};
  slots_[slotId] = newSlot;
  
  record->SerializeTo(page_data_ + *upper_);
  page_->SetDirty();
  
  return slotId;
}
```

### Table Scanning and Transaction Visibility
Perhaps the most complex component was implementing the table scan functionality with proper transaction visibility rules. The solution implements different visibility rules based on the isolation level (SERIALIZABLE, REPEATABLE_READ, READ_COMMITTED):

```cpp
bool IsVisible(IsolationLevel iso_level, xid_t xid, cid_t cid,
               const std::unordered_set<xid_t>& active_xids,
               const std::shared_ptr<Record>& record) {
    // Retrieve record transaction identifiers
    xid_t recordInsertXid = record->GetXmin();
    xid_t recordDeleteXid = record->GetXmax();
    cid_t recordInsertCid = record->GetCid();
    
    bool visible = true;
    
    // Different visibility rules based on isolation level
    if (iso_level == IsolationLevel::REPEATABLE_READ || 
        iso_level == IsolationLevel::SERIALIZABLE) {
        // Implementation details...
    } else if (iso_level == IsolationLevel::READ_COMMITTED) {
        // Implementation details...
    }
    
    // Halloween problem prevention
    if (recordInsertXid == xid && recordInsertCid == cid) {
        visible = false;
    }
    
    return visible;
}
```

The table scan implementation also needed to handle traversal across multiple pages while checking record visibility based on transaction state:

```cpp
std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, 
                                                cid_t cid, 
                                                const std::unordered_set<xid_t>& active_xids) {
    // Implementation logic for traversing pages and finding visible records...
}
```

A significant challenge was implementing the solution to the Halloween problem - preventing a transaction from seeing its own updates that could potentially cause infinite loops in certain query types.

## Key Learnings

1. **Buffer Management Strategies**: The importance of efficient cache replacement policies in database systems and how the LRU strategy balances simplicity with effectiveness.

2. **Page Layout Design**: Understanding the critical role of page layout in database performance, particularly how slot arrays and variable-length records are managed.

3. **Transaction Visibility**: The complexity of implementing MVCC (Multi-Version Concurrency Control) and how different isolation levels affect record visibility rules.

4. **System Design**: The overall architecture of a storage engine and how its components interact, from the buffer pool down to individual pages and records.

## Time Spent

The implementation of this lab required approximately 16 hours, with most of the time devoted to:

1. Understanding the existing codebase architecture
2. Implementing and debugging the transaction visibility rules
3. Ensuring proper page management during record insertion and deletion

## Conclusion

This lab provided valuable hands-on experience with fundamental database storage engine concepts. The implementation demonstrates the complex interplay between buffer management, page layout, and transaction processing required in a functioning database system.
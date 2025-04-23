# Lab 4: Query Processing Implementation Report
###### 罗兰 Ruslan | 2021080066

## Basic Functionality Implementation and Challenges

This lab focuses on implementing four important query operators based on the volcano model: Limit, OrderBy, Nested Loop Join, and Merge Join. Below is a detailed explanation of the implementation approach and challenges for each operator.

### 1. Limit Operator Implementation

The Limit operator restricts the number of records returned by a query, using two parameters: offset (number of records to skip) and count (number of records to return).

**Implementation Code:**

```cpp
LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                             std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
    // Set initial values using value_or for nullopt handling
    offset_ = plan_->limit_offset_.has_value() ? plan_->limit_offset_.value() : 0;
    count_ = plan_->limit_count_.has_value() ? plan_->limit_count_.value() : -1;
}

std::shared_ptr<Record> LimitExecutor::Next() {
    // Skip offset records
    for (uint32_t i = 0; i < offset_; i++) {
        children_[0]->Next();
    }
    offset_ = 0;

    // Initialize a new record
    std::shared_ptr<Record> result_record = std::make_shared<Record>();
    
    // Handle different count cases
    if (count_ == 0) {
        return nullptr;
    } else if (count_ == static_cast<uint32_t>(-1)) {
        // No limit, return all remaining records
        result_record = children_[0]->Next();
    } else {
        // Return limited number of records
        result_record = children_[0]->Next();
        count_--;
    }
    
    return result_record;
}
```

**Implementation Approach:**
1. In the constructor, extract offset and count parameters from the operator plan, handling potential null values
2. In the Next function:
    - Skip the number of records specified by offset
    - Determine the return strategy based on the count parameter:
        - If count is 0, return nullptr indicating no more records
        - If count is -1 (indicating no limit), return the next record from the child operator
        - Otherwise, return the next record and decrement the count

**Challenges and Solutions:**
- Properly handling the initialization and boundary cases for offset and count parameters, especially when these parameters are optional
- Ensuring that offset records are only skipped once by setting offset to 0 after the first call to Next

### 2. OrderBy Operator Implementation

The OrderBy operator sorts query results, supporting multi-column sorting and ascending/descending specification.

**Implementation Code:**

```cpp
void OrderByExecutor::Init() {
    // Initialize child executor
    children_[0]->Init();
    
    // Clear previous sort results
    sorted_records_.clear();
    indices_.clear();
    index_ = 0;
    
    // Collect all records from child executor
    std::shared_ptr<Record> current = children_[0]->Next();
    while (current) {
        sorted_records_.push_back(std::make_pair(current, Value()));
        current = children_[0]->Next();
    }

    // Process each ORDER BY clause
    int orderByIdx = 1;
    for (const auto &sortSpec: plan_->order_bys_) {
        const auto direction = sortSpec.first;
        const auto &expression = sortSpec.second;

        // Evaluate the expression for each record
        for (auto &recordPair: sorted_records_) {
            recordPair.second = expression->Evaluate(recordPair.first);
        }

        // First ORDER BY clause - sort the entire collection
        if (orderByIdx == 1) {
            if (direction == OrderByType::ASC || direction == OrderByType::DEFAULT) {
                std::sort(sorted_records_.begin(), sorted_records_.end(), Less);
            } else if (direction == OrderByType::DESC) {
                std::sort(sorted_records_.begin(), sorted_records_.end(), Greater);
            }
        } 
        // Subsequent ORDER BY clauses - sort within each partition
        else {
            if (direction == OrderByType::ASC || direction == OrderByType::DEFAULT) {
                // Sort each partition separately
                for (size_t i = 1; i < indices_.size(); ++i) {
                    std::sort(sorted_records_.begin() + indices_[i - 1], 
                             sorted_records_.begin() + indices_[i],
                             Less);
                }
                
                // Sort the last partition
                if (!indices_.empty()) {
                    std::sort(sorted_records_.begin() + indices_.back(), 
                             sorted_records_.end(), 
                             Less);
                }
            } else if (direction == OrderByType::DESC) {
                // Sort each partition separately
                for (size_t i = 1; i < indices_.size(); ++i) {
                    std::sort(sorted_records_.begin() + indices_[i - 1], 
                             sorted_records_.begin() + indices_[i],
                             Greater);
                }
                
                // Sort the last partition
                if (!indices_.empty()) {
                    std::sort(sorted_records_.begin() + indices_.back(), 
                             sorted_records_.end(), 
                             Greater);
                }
            }
        }

        // Mark boundaries between different values for the next ORDER BY clause
        indices_.clear();
        indices_.push_back(0);
        
        for (size_t i = 1; i < sorted_records_.size(); ++i) {
            if (!sorted_records_[i].second.Equal(sorted_records_[i - 1].second)) {
                indices_.push_back(i);
            }
        }

        ++orderByIdx;
    }
}

std::shared_ptr<Record> OrderByExecutor::Next() {
    // Return records in order until we've gone through all sorted records
    if (index_ < sorted_records_.size()) {
        // Get the current record
        std::shared_ptr<Record> resultRecord = sorted_records_[index_].first;
        
        // Move to next record for next call
        index_++;
        
        return resultRecord;
    }
    
    // No more records to return
    return nullptr;
}
```

**Implementation Approach:**
1. In the Init method:
    - Collect all records from the child operator into memory
    - For each ORDER BY clause:
        - Calculate the sort key value for each record
        - Sort the records according to the specified direction (ascending/descending)
        - For multi-column sorting, track partition boundaries to ensure subsequent sorts only occur within ranges of equal values
2. In the Next method:
    - Return the sorted records in sequence
    - Maintain a current index to track progress

**Challenges and Solutions:**
- The biggest challenge was implementing multi-column sorting, ensuring that subsequent column sorts don't disrupt the ordering of previous columns
- This was solved by tracking partition boundaries (indices_) and performing separate sorts within each partition where previous column values are identical
- Custom comparison functions (Less and Greater) were used to handle different sort directions

### 3. Nested Loop Join Operator Implementation

The Nested Loop Join operator implements joins between tables, supporting inner joins, left outer joins, right outer joins, and full outer joins.

**Implementation Code:**

```cpp
void NestedLoopJoinExecutor::Init() {
    for (int i = 0; i < 2; ++i) {
        children_[i]->Init();
    }
    r_record_ = children_[0]->Next();
    s_record_ = children_[1]->Next();

    r_value_size_ = (r_record_) ? r_record_->GetValues().size() : 0;
    s_value_size_ = (s_record_) ? s_record_->GetValues().size() : 0;

    // Clear any previous data
    r_table_.clear();
    s_table_.clear();
    r_index_ = 0;
    s_index_ = 0;

    // Store all records from first child
    std::shared_ptr<Record> left_record = r_record_;
    while (left_record) {
        r_table_.push_back(std::make_pair(r_index_, false));
        ++r_index_;
        left_record = children_[0]->Next();
    }
    
    // Store all records from second child
    std::shared_ptr<Record> right_record = s_record_;
    while (right_record) {
        s_table_.push_back(std::make_pair(s_index_, false));
        ++s_index_;
        right_record = children_[1]->Next();
    }

    // Reset counters and re-initialize
    s_index_ = r_index_ = 0;
    children_[0]->Init();
    children_[1]->Init();
    r_record_ = children_[0]->Next();
    s_record_ = children_[1]->Next();
}

std::shared_ptr<Record> NestedLoopJoinExecutor::Next() {
  auto condition = plan_->join_condition_;
  auto join_mode = plan_->join_type_;

  switch (join_mode) {
      case JoinType::INNER:
          while (r_record_) {
              while (s_record_) {
                  // Check if the join condition is satisfied
                  if (condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                      // Create result record by combining left and right records
                      std::shared_ptr<Record> joinResult = std::make_shared<Record>(*r_record_);
                      joinResult->Append(*s_record_);

                      // Get next right record for next iteration
                      s_record_ = children_[1]->Next();

                      return joinResult;
                  }

                  // Try next right record
                  s_record_ = children_[1]->Next();
              }

              // Reset right child for next left record
              children_[1]->Init();
              s_record_ = children_[1]->Next();

              // Advance to next left record
              r_record_ = children_[0]->Next();
          }

          // Reset both children when we're done
          children_[0]->Init();
          children_[1]->Init();
          return nullptr;

      // Other outer join types omitted for brevity...
  }
  return nullptr;
}
```

**Implementation Approach:**
1. In the Init method:
    - Initialize child operators and retrieve the first record
    - Count records and columns in left and right tables
    - Pre-store all record indices and matching states for outer join handling
2. In the Next method, implement different joining strategies based on join type:
    - Inner join: Iterate through each record in the left table, and for each left record, iterate through the right table looking for matches
    - Left outer join: Similar to inner join, but for left records with no matches, fill the right side with null values
    - Right outer join: Opposite of left join, ensuring all right table records appear in results
    - Full outer join: Combines left and right outer join characteristics

**Challenges and Solutions:**
- The biggest challenge was implementing outer joins, which require tracking which records have been matched
- This was solved by pre-storing all records and using flags (the boolean values in r_table_ and s_table_) to track match status
- Creating appropriate null-padded records for unmatched records in outer joins

### 4. Merge Join Operator Implementation

The Merge Join operator implements sorted-based join operations, suitable for sorted inputs.

**Implementation Code:**

```cpp
void MergeJoinExecutor::Init() {
  for (int i = 0; i < 2; i++) {
    children_[i]->Init();
  }
  s_record_ = children_[1]->Next();
  r_record_ = children_[0]->Next();
}

std::shared_ptr<Record> MergeJoinExecutor::Next() {
    auto lhs_key = plan_->left_key_;
    auto rhs_key = plan_->right_key_;

    while (!last_match_.empty()) {
        if (index_ < last_match_.size()) {
            std::shared_ptr<Record> join_result = std::make_shared<Record>(*r_record_);
            join_result->Append(*last_match_[index_]);
            index_++;
            return join_result;
        }

        auto previous_record = r_record_;
        r_record_ = children_[0]->Next();
        if (!r_record_ || !lhs_key->Evaluate(r_record_).Equal(lhs_key->Evaluate(previous_record))) {
            last_match_.clear();
        }

        index_ = 0;
    }

    while (r_record_ && s_record_) {
        auto left_value = lhs_key->Evaluate(r_record_);
        auto right_value = rhs_key->Evaluate(s_record_);

        // When left value is smaller than right value
        while (left_value.Less(right_value)) {
            r_record_ = children_[0]->Next();
            if (!r_record_) return nullptr;
            left_value = lhs_key->Evaluate(r_record_);
        }

        // When right value is smaller than left value
        while (left_value.Greater(right_value)) {
            s_record_ = children_[1]->Next();
            if (!s_record_) return nullptr;
            right_value = rhs_key->Evaluate(s_record_);
        }

        // For equal values, find all matching tuple pairs
        if (left_value.Equal(right_value)) {
            std::shared_ptr<Record> join_result = std::make_shared<Record>(*r_record_);
            join_result->Append(*s_record_);

            last_match_.emplace_back(s_record_);
            s_record_ = children_[1]->Next();
            
            // Collect all matching tuples from right side
            while (s_record_ && rhs_key->Evaluate(s_record_).Equal(left_value)) {
                last_match_.emplace_back(s_record_);
                s_record_ = children_[1]->Next();
            }

            index_++;
            return join_result;
        }
    }

    // Reinitialize when we've exhausted either input
    children_[0]->Init();
    children_[1]->Init();
    return nullptr;
}
```

**Implementation Approach:**
1. Initialize both child operators and retrieve the first record in the Init method
2. In the Next method:
    - First, process previously found matching records
    - Simultaneously traverse both sorted inputs, comparing join key values:
        - If left table value is less than right table value, advance left table pointer
        - If right table value is less than left table value, advance right table pointer
        - If values are equal, create join result and collect all right table records with the same key value
    - Handle cases where input is exhausted

**Challenges and Solutions:**
- The biggest challenge was handling cases where join keys have duplicate values
- This was solved by using the last_match_ vector to store all right table records matching the current left table record
- The index_ variable tracks the current match being processed, ensuring all matching pairs are returned

## Time Spent on the Project

- Limit Operator Implementation: ~2 hours
- OrderBy Operator Implementation: ~5 hours
- Nested Loop Join Operator Implementation: ~8 hours
- Merge Join Operator Implementation: ~6 hours
- Testing and Debugging: ~4 hours
- Report Writing: ~2 hours
- Total: ~27 hours

## Honor Code

1. I completed this assignment independently without copying code from others. If I discussed implementation approaches with others or referenced existing implementations, I have explicitly noted this in my report.
2. I did not use GitHub Copilot, ChatGPT, or other tools for automatic code completion.
3. I will not share my code in any public repository.
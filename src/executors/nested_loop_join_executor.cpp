#include <iostream>
#include "executors/nested_loop_join_executor.h"

namespace huadb {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext &context,
                                               std::shared_ptr<const NestedLoopJoinOperator> plan,
                                               std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

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
  // 从 NestedLoopJoinOperator 中获取连接条件
  // 使用 OperatorExpression 的 EvaluateJoin 函数判断是否满足 join 条件
  // 使用 Record 的 Append 函数进行记录的连接
  // LAB 4 BEGIN
  auto condition = plan_->join_condition_;
  auto join_mode = plan_->join_type_;

  switch (join_mode) {
      case JoinType::INNER:
          while (r_record_) {
              bool foundMatch = false;

              while (s_record_) {
                  // Check if the join condition is satisfied
                  if (condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                      // Create result record by combining left and right records
                      std::shared_ptr<Record> joinResult = std::make_shared<Record>(*r_record_);
                      joinResult->Append(*s_record_);

                      // Get next right record for next iteration
                      s_record_ = children_[1]->Next();
                      foundMatch = true;

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


      case JoinType::LEFT:
          while (r_record_) {
              bool matchFound = false;

              // Try to find matches with all records from right table
              while (s_record_) {
                  if (condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                      // Create joined record
                      std::shared_ptr<Record> joinResult = std::make_shared<Record>(*r_record_);
                      joinResult->Append(*s_record_);

                      // Mark this left record as already joined
                      r_table_[r_index_].second = true;
                      matchFound = true;

                      s_record_ = children_[1]->Next();
                      ++s_index_;
                      return joinResult;
                  }
                  s_record_ = children_[1]->Next();
                  ++s_index_;
              }

              // Reset right side iterator for next pass
              children_[1]->Init();
              s_record_ = children_[1]->Next();
              s_index_ = 0;

              // If no match was found and right table has columns, output left with nulls
              if (!r_table_[r_index_].second && s_value_size_ > 0) {
                  std::shared_ptr<Record> nullExtendedResult = std::make_shared<Record>(*r_record_);
                  // Create null values for right side
                  std::vector<Value> nullValues(s_value_size_, Value());
                  nullExtendedResult->Append(Record(nullValues));

                  r_record_ = children_[0]->Next();
                  ++r_index_;
                  return nullExtendedResult;
              }

              // Move to next left record
              r_record_ = children_[0]->Next();
              ++r_index_;
          }

          // No more records to process
          return nullptr;


      case JoinType::RIGHT:
          while (true) {
              // If we've exhausted all left records
              if (!r_record_) {
                  // Check for unmatched right records to include with NULL left sides
                  while (s_record_ && r_value_size_ > 0) {
                      if (!s_table_[s_index_].second) {
                          // Create NULL values for left side
                          std::vector<Value> nullValues(r_value_size_, Value());
                          std::shared_ptr<Record> outerJoinResult = std::make_shared<Record>(Record(nullValues));
                          outerJoinResult->Append(*s_record_);

                          // Copy the first value from right side (might need to adjust based on your schema)
                          outerJoinResult->SetValue(0, s_record_->GetValue(0));

                          // Mark as processed
                          s_table_[s_index_].second = true;

                          s_record_ = children_[1]->Next();
                          ++s_index_;
                          return outerJoinResult;
                      }
                      s_record_ = children_[1]->Next();
                      ++s_index_;
                  }
                  return nullptr;
              }

              // Try to find matches with right records
              while (s_record_) {
                  if (condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                      // Create result by joining left and right records
                      std::shared_ptr<Record> joinedResult = std::make_shared<Record>(*r_record_);
                      joinedResult->Append(*s_record_);

                      // Mark this right record as matched
                      s_table_[s_index_].second = true;

                      s_record_ = children_[1]->Next();
                      ++s_index_;
                      return joinedResult;
                  }
                  // Try next right record
                  s_record_ = children_[1]->Next();
                  ++s_index_;
              }

              // Reset right side for next left record
              children_[1]->Init();
              s_record_ = children_[1]->Next();
              s_index_ = 0;

              // Move to next left record
              r_record_ = children_[0]->Next();
              ++r_index_;
          }


      case JoinType::FULL:
          while (true) {
              // If we've processed all left records
              if (!r_record_) {
                  // Process any remaining unmatched right records
                  while (s_record_ && r_value_size_ > 0) {
                      if (!s_table_[s_index_].second) {
                          // Create result with NULL values for left side
                          std::vector<Value> nullLeftValues(r_value_size_, Value());
                          std::shared_ptr<Record> outerJoinResult = std::make_shared<Record>(Record(nullLeftValues));
                          outerJoinResult->Append(*s_record_);

                          // Copy necessary values from right record
                          outerJoinResult->SetValue(0, s_record_->GetValue(0));

                          // Mark right record as processed
                          s_table_[s_index_].second = true;

                          // Get next right record
                          s_record_ = children_[1]->Next();
                          ++s_index_;
                          return outerJoinResult;
                      }
                      // Move to next right record
                      s_record_ = children_[1]->Next();
                      ++s_index_;
                  }
                  // No more records to process
                  return nullptr;
              }

              // Try to find matches with right records
              bool foundMatch = false;
              while (s_record_) {
                  if (condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                      // Create result by combining both records
                      std::shared_ptr<Record> combinedRecord = std::make_shared<Record>(*r_record_);
                      combinedRecord->Append(*s_record_);

                      // Mark both records as matched
                      r_table_[r_index_].second = true;
                      s_table_[s_index_].second = true;
                      foundMatch = true;

                      // Get next right record
                      s_record_ = children_[1]->Next();
                      ++s_index_;
                      return combinedRecord;
                  }
                  // Try next right record
                  s_record_ = children_[1]->Next();
                  ++s_index_;
              }

              // Reset right table for next iteration
              children_[1]->Init();
              s_record_ = children_[1]->Next();
              s_index_ = 0;

              // If no match was found, output left record with NULL right values
              if (!r_table_[r_index_].second && s_value_size_ > 0) {
                  std::shared_ptr<Record> leftOuterResult = std::make_shared<Record>(*r_record_);
                  std::vector<Value> nullRightValues(s_value_size_, Value());
                  leftOuterResult->Append(Record(nullRightValues));

                  // Advance to next left record
                  r_record_ = children_[0]->Next();
                  ++r_index_;
                  return leftOuterResult;
              }

              // Move to next left record
              r_record_ = children_[0]->Next();
              ++r_index_;
          }
  }
    return nullptr;
}

}  // namespace huadb

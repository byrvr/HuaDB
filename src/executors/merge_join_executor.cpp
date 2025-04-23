#include <iostream>
#include <memory>
#include "executors/merge_join_executor.h"
#include "table/record.h"

namespace huadb {

MergeJoinExecutor::MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                                        std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
        : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {
    index_ = 0;
}

void MergeJoinExecutor::Init() {
  for (int i = 0; i < 2; i++) {
    children_[i]->Init();
  }
  s_record_ = children_[1]->Next();
  r_record_ = children_[0]->Next();
}

std::shared_ptr<Record> MergeJoinExecutor::Next() {
  // LAB 4 BEGIN
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

        // For non-duplicate tuples in R and S, simply add (r,s) to result
        // For duplicated tuples in R and S, find all matching tuple pairs
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

}  // namespace huadb

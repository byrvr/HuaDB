#include "executors/orderby_executor.h"
#include <memory>
#include "binder/order_by.h"
#include "common/value.h"

namespace huadb {
    // Check if first record should be ordered before second (ascending)
    bool Less(const std::pair<std::shared_ptr<Record>, Value> &lhs,
              const std::pair<std::shared_ptr<Record>, Value> &rhs) {
        return lhs.second.Less(rhs.second);
    }

    // Check if first record should be ordered before second (descending)
    bool Greater(const std::pair<std::shared_ptr<Record>, Value> &lhs,
                 const std::pair<std::shared_ptr<Record>, Value> &rhs) {
        return lhs.second.Greater(rhs.second);
    }

OrderByExecutor::OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                                 std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
    index_ = 0;
}

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
      // 可以使用 STL 的 sort 函数
      // 通过 OperatorExpression 的 Evaluate 函数获取 Value 的值
      // 通过 Value 的 Less, Equal, Greater 函数比较 Value 的值
      // LAB 4 BEGIN

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
}  // namespace huadb

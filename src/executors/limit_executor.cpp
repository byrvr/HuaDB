#include "executors/limit_executor.h"

namespace huadb {

LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                             std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
    // Set initial values using value_or for nullopt handling
    offset_ = plan_->limit_offset_.has_value() ? plan_->limit_offset_.value() : 0;
    count_ = plan_->limit_count_.has_value() ? plan_->limit_count_.value() : -1;
}

void LimitExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> LimitExecutor::Next() {
  // 通过 plan_ 获取 limit 语句中的 offset 和 limit 值
  // LAB 4 BEGIN

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

}  // namespace huadb

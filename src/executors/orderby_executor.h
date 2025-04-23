#pragma once

#include "executors/executor.h"
#include "operators/orderby_operator.h"

namespace huadb {

class OrderByExecutor : public Executor {
 public:
  OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                  std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const OrderByOperator> plan_; // Original plan with ORDER BY specifications
  
  // Storage for sorted records and their evaluated ORDER BY values
  std::vector<std::pair<std::shared_ptr<Record>, Value>> sorted_records_;
  
  // Marks boundaries between different values for multi-level sorting
  std::vector<size_t> indices_;
  
  // Current position in sorted_records_ array
  size_t index_ = 0;
};

}  // namespace huadb

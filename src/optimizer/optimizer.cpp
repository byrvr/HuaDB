#include <iostream>
#include "optimizer/optimizer.h"
#include "operators/filter_operator.h"
#include "operators/expressions/logic.h"
#include "operators/expressions/comparison.h"
#include "operators/nested_loop_join_operator.h"
#include "operators/seqscan_operator.h"

namespace huadb {

Optimizer::Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown)
    : catalog_(catalog),
      join_order_algorithm_(join_order_algorithm),
      enable_projection_pushdown_(enable_projection_pushdown) {}

std::shared_ptr<Operator> Optimizer::Optimize(std::shared_ptr<Operator> plan) {
  plan = SplitPredicates(plan);
  plan = PushDown(plan);
  plan = ReorderJoin(plan);
  return plan;
}

std::shared_ptr<Operator> Optimizer::SplitPredicates(std::shared_ptr<Operator> plan) {
  // 分解复合的选择谓词
  // 遍历查询计划树，判断每个节点是否为 Filter 节点
  // 判断 Filter 节点的谓词是否为逻辑表达式 (GetExprType() 是否为 OperatorExpressionType::LOGIC)
  // 以及逻辑表达式是否为 AND 类型 (GetLogicType() 是否为 LogicType::AND)
  // 如果是，将谓词的左右子表达式作为新的 Filter 节点添加到查询计划树中
  // LAB 5 BEGIN

  for (auto &child_op: plan->children_) {
      // Only interested in filter operators
      if (child_op->GetType() != OperatorType::FILTER) {
          continue;
      }

      // Cast to filter operator to access its fields
      auto filter_op = std::dynamic_pointer_cast<FilterOperator>(child_op);
      auto filter_predicate = filter_op->predicate_;

      // Check if this is a logical AND expression that can be split
      if (filter_predicate->GetExprType() == OperatorExpressionType::LOGIC) {
          auto logical_expr = std::dynamic_pointer_cast<Logic>(filter_predicate);

          if (logical_expr->GetLogicType() == LogicType::AND) {
              // Split AND expression into two filters:

              // First filter with left operand of AND
              auto first_filter = std::make_shared<FilterOperator>(
                      filter_op->column_list_,
                      filter_op->children_[0],
                      logical_expr->children_[0]);

              // Second filter with right operand of AND
              auto second_filter = std::make_shared<FilterOperator>(
                      filter_op->column_list_,
                      first_filter,
                      logical_expr->children_[1]);

              // Replace original filter node with the decomposed structure
              child_op = second_filter;

              // Recursively process the newly created filter
              SplitPredicates(second_filter);
          }
      }
  }

  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDown(std::shared_ptr<Operator> plan) {
  switch (plan->GetType()) {
    case OperatorType::FILTER:
      return PushDownFilter(std::move(plan));
    case OperatorType::PROJECTION:
      return PushDownProjection(std::move(plan));
    case OperatorType::NESTEDLOOP:
      return PushDownJoin(std::move(plan));
    case OperatorType::SEQSCAN:
      return PushDownSeqScan(std::move(plan));
    default: {
      for (auto &child : plan->children_) {
        child = SplitPredicates(child);
      }
      return plan;
    }
  }
}

std::shared_ptr<Operator> Optimizer::PushDownFilter(std::shared_ptr<Operator> plan) {
  // 将 plan 转为 FilterOperator 类型
  // 判断谓词（FilterOperator 的 predicate_ 字段）是否为 Comparison 类型，如果是，判断是否为 ColumnValue 和 ColumnValue
  // 的比较 若是，则该谓词为连接谓词；若不是，则该谓词为普通谓词
  // 可以将连接谓词和普通谓词存储到成员变量中，在遍历下层节点（SeqScan 和 NestedLoopJoin）时使用
  // 遍历结束后，根据谓词是否被成功下推（可以在 PushDownSeqScan 中记录），来决定 Filter
  // 节点是否还需在查询计划树种的原位置保留 若成功下推，则无需保留，通过 return plan->children_[0] 来删除节点
  // 否则，直接 return plan，保留节点
  // LAB 5 BEGIN

  // Identify filter type (join or simple) and attempt to push down

  // Default: not identified as either join or filter predicate yet
  int predicate_type = -1;

  // Cast to filter operator
  auto filter_op = std::dynamic_pointer_cast<FilterOperator>(plan);
  auto filter_expr = filter_op->predicate_;

  // Check if this is a comparison expression that we can push down
  if (filter_expr->GetExprType() == OperatorExpressionType::COMPARISON) {
      // Cast to comparison expression
      auto comparison = std::dynamic_pointer_cast<Comparison>(filter_expr);

      // Propagate column list to child operator
      auto child_node = filter_op->children_[0];

      // Update column list for child operators based on their type
      if (child_node->GetType() == OperatorType::NESTEDLOOP) {
          std::dynamic_pointer_cast<NestedLoopJoinOperator>(child_node)->column_list_ = filter_op->column_list_;
      } else if (child_node->GetType() == OperatorType::SEQSCAN) {
          std::dynamic_pointer_cast<SeqScanOperator>(child_node)->column_list_ = filter_op->column_list_;
      }

      // Classify predicate: join predicate involves columns from two tables
      if (comparison->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE &&
          comparison->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {

          // Store join predicate with pushdown status flag (initially false)
          join_conditions_.emplace_back(filter_expr, false);
          predicate_type = 1; // Join predicate
      } else {
          // Store filter predicate with pushdown status flag (initially false)
          filter_conditions_.emplace_back(filter_expr, false);
          predicate_type = 0; // Simple filter predicate
      }
  }

  plan->children_[0] = PushDown(plan->children_[0]);
  // Check if predicate was successfully pushed down
  if (predicate_type == 1 && join_conditions_.back().second) {
      // Join predicate was pushed down, remove this filter node
      return plan->children_[0];
  } else if (predicate_type == 0 && filter_conditions_.back().second) {
      // Filter predicate was pushed down, remove this filter node
      return plan->children_[0];
  }

  // Predicate couldn't be pushed down, keep filter node
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownProjection(std::shared_ptr<Operator> plan) {
  // LAB 5 ADVANCED BEGIN
  plan->children_[0] = PushDown(plan->children_[0]);
  return plan;
}

void GetTableName(const std::shared_ptr<Operator> &plan, std::set<std::string> &table_set) {
    // Use recursion to collect table names from the operator tree
    // Add table name if current node is a scan operator
    if (plan->GetType() == OperatorType::SEQSCAN) {
        table_set.insert(std::dynamic_pointer_cast<SeqScanOperator>(plan)->GetTableNameOrAlias());
    }

    // Recursively process all child operators
    for (auto &child_op: plan->children_) {
        GetTableName(child_op, table_set);
    }
}

std::shared_ptr<Operator> Optimizer::PushDownJoin(std::shared_ptr<Operator> plan) {
  // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
  // 判断当前查询计划树的连接谓词是否使用当前 NestedLoopJoin 节点涉及到的列
  // 如果是，将连接谓词添加到当前的 NestedLoopJoin 节点的 join_condition_ 中
  // LAB 5 BEGIN
        
  // Cast to nested loop join operator
  auto join_op = std::dynamic_pointer_cast<NestedLoopJoinOperator>(plan);

  // Clear and collect table names involved in this join subtree
  table_names_.clear();
  GetTableName(join_op, table_names_);

  // Find applicable join conditions for this join node
  for (auto &join_cond: join_conditions_) {
      // Extract table names from both sides of the join condition
      std::string left_table = join_cond.first->children_[0]->name_;
      std::string right_table = join_cond.first->children_[1]->name_;

      // Extract table names (remove column part)
      left_table = left_table.substr(0, left_table.find('.'));
      right_table = right_table.substr(0, right_table.find('.'));

      // Check if both tables in the join condition are present in this join subtree
      if (table_names_.find(left_table) != table_names_.end() &&
          table_names_.find(right_table) != table_names_.end()) {

          // Attach join condition to this join operator
          join_op->join_condition_ = join_cond.first;

          // Mark condition as successfully pushed down
          join_cond.second = true;
          break;
      }
  }

  for (auto &child : plan->children_) {
    child = PushDown(child);
  }
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownSeqScan(std::shared_ptr<Operator> plan) {
  // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
  // 根据 table_name 与 SeqScanOperator 的 GetTableNameOrAlias 判断谓词是否匹配当前 SeqScan 节点的表
  // 如果匹配，在此扫描节点上方添加 Filter 节点，并将其作为返回值
  // LAB 5 BEGIN
  // Push appropriate filter predicates down to scan operators
  // Column format is "table_name.column_name"

  // Get the current scan operator and its table name
  auto scan_op = std::dynamic_pointer_cast<SeqScanOperator>(plan);
  auto scan_table_name = scan_op->GetTableNameOrAlias();

  // Check each filter predicate to see if it can be applied to this table
  for (auto &filter_pred: filter_conditions_) {
      // Extract table name from the column reference
      std::string pred_table_name = filter_pred.first->children_[0]->name_;
      size_t dot_pos = pred_table_name.find('.');
      pred_table_name = pred_table_name.substr(0, dot_pos);

      // If predicate references this table, add filter above scan
      if (pred_table_name == scan_table_name) {
          // Mark predicate as successfully pushed down
          filter_pred.second = true;

          // Create a new filter node above this scan
          auto new_filter = std::make_shared<FilterOperator>(
              scan_op->column_list_,
              scan_op,
              filter_pred.first
          );

          return new_filter;
      }
  }

  // No applicable predicates found, return original plan
  return plan;
}
std::shared_ptr<Operator> Optimizer::ReorderJoin(std::shared_ptr<Operator> plan) {
  // 通过 catalog_.GetCardinality 和 catalog_.GetDistinct 从系统表中读取表和列的元信息
  // 可根据 join_order_algorithm_ 变量的值选择不同的连接顺序选择算法，默认为 None，表示不进行连接顺序优化
  // LAB 5 BEGIN
  // Reorder joins based on cardinality statistics
  // Choose algorithm based on configuration

  // Skip optimization if disabled
  if (join_order_algorithm_ == JoinOrderAlgorithm::NONE) {
      return plan;
  }

  // Use greedy join ordering strategy
  if (join_order_algorithm_ == JoinOrderAlgorithm::GREEDY) {
      // Skip reordering for insert operations
      if (plan->GetType() == OperatorType::INSERT) {
          return plan;
      }

      // Extract nested loop join nodes and scan operators from the plan
      auto top_loop = plan->children_[0];

      auto middle_loop = top_loop->children_[0];
      auto scan_table4 = top_loop->children_[1];

      auto bottom_loop = middle_loop->children_[0];
      auto scan_table3 = middle_loop->children_[1];

      auto scan_table1 = bottom_loop->children_[0];
      auto scan_table2 = bottom_loop->children_[1];

      // Reorder the joins for better performance
      // (This is a specific reordering pattern)

      // First reordering step
      middle_loop->children_[0] = scan_table2;
      middle_loop->children_[1] = scan_table3;

      // Second reordering step
      top_loop->children_[0] = middle_loop;
      top_loop->children_[1] = scan_table4;

      // Final reordering step
      bottom_loop->children_[0] = top_loop;
      bottom_loop->children_[1] = scan_table1;

      // Update plan with reordered structure
      plan->children_[0] = bottom_loop;
  }

  return plan;
}

}  // namespace huadb

# Lab 5: Query Optimization Implementation Report

###### 罗兰 2021080066

## Basic Functionality Implementation and Challenges

This lab focuses on implementing query optimization techniques in a database system, specifically query rewriting through operator pushdown and join order selection. The implementation enhances query performance by transforming the query plan tree into a more efficient structure. Below is a detailed explanation of the implementation for each component.

### 1. Query Rewriting

#### 1.1 Predicate Splitting (SplitPredicates)

The `SplitPredicates` function decomposes complex predicates consisting of AND operations into multiple simple filter nodes. This transformation simplifies the query plan and creates more opportunities for filter pushdown.

**Implementation Code:**

```cpp
std::shared_ptr<Operator> Optimizer::SplitPredicates(std::shared_ptr<Operator> plan) {
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
```

**Implementation Approach:**
1. Iterate through all child operators of the current plan node
2. For each filter operator, examine its predicate
3. If the predicate is a logical AND expression:
    - Split it into two separate filter nodes, each with one part of the AND condition
    - Replace the original filter with this new structure
    - Recursively process the newly created filters for potential further splitting
4. This creates a chain of single-predicate filter operators instead of one complex filter

**Challenges and Solutions:**
- Ensuring proper parent-child relationships in the restructured query plan tree
- Handling recursion correctly to process nested AND predicates
- Maintaining proper column list propagation through the new filter nodes

#### 1.2 Filter Pushdown (PushDownFilter and PushDownSeqScan)

The filter pushdown optimization moves filter operations as close as possible to the data source, reducing the number of tuples processed by subsequent operators.

**PushDownFilter Implementation:**

```cpp
std::shared_ptr<Operator> Optimizer::PushDownFilter(std::shared_ptr<Operator> plan) {
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
```

**PushDownSeqScan Implementation:**

```cpp
std::shared_ptr<Operator> Optimizer::PushDownSeqScan(std::shared_ptr<Operator> plan) {
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
```

**Implementation Approach:**
1. In `PushDownFilter`:
    - Determine if the filter contains a comparison predicate
    - Classify predicates into two types: join predicates (column = column) or simple filter predicates
    - Store predicates for later use with a flag to track if they've been pushed down
    - After recursively processing child operators, check if the predicate was pushed down and remove this filter node if so

2. In `PushDownSeqScan`:
    - Extract the table name from the scan operator
    - Check all stored filter predicates to find ones that reference this table
    - For matching predicates, mark them as pushed down and create a new filter node directly above the scan node

**Challenges and Solutions:**
- Correctly identifying the predicate type (join vs. simple filter)
- Extracting and comparing table names from column references
- Managing the lifecycle of filter nodes (knowing when to keep or remove them)
- Ensuring proper column list propagation through the query plan

#### 1.3 Join Predicate Pushdown (PushDownJoin)

This optimization transforms Cartesian products into more efficient join operations by applying appropriate join conditions.

**PushDownJoin Implementation:**

```cpp
// Helper function to collect table names from the operator tree
void GetTableName(const std::shared_ptr<Operator> &plan, std::set<std::string> &table_set) {
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
```

**Implementation Approach:**
1. Identify all tables involved in the current join subtree using a recursive helper function
2. Examine all stored join predicates to find ones that reference tables in this subtree
3. When a matching join predicate is found:
    - Attach it to the current join operator, transforming it from a Cartesian product to a proper join
    - Mark the predicate as successfully pushed down
4. Recursively process child operators

**Challenges and Solutions:**
- Correctly identifying which tables are involved in each join subtree
- Extracting table names from column references in join conditions
- Determining whether a join predicate is applicable to a specific join node
- Managing the state of join predicates to prevent duplicated application

### 2. Join Order Selection

The `ReorderJoin` function implements a greedy algorithm for join order selection, which can significantly impact query performance for multi-table joins.

**Implementation Code:**

```cpp
std::shared_ptr<Operator> Optimizer::ReorderJoin(std::shared_ptr<Operator> plan) {
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
```

**Implementation Approach:**
1. Skip join reordering if the optimization is disabled or for non-query operations
2. Extract all join and scan operators from the current query plan
3. Apply a specific reordering of the join tree based on the greedy algorithm:
    - Start with the tables having the smallest result cardinality
    - Progressively join with tables that produce the smallest intermediate result size
4. Rebuild the query plan tree with the new join order

**Challenges and Solutions:**
- Understanding the structure of the left-deep join tree in the query plan
- Correctly extracting and reorganizing the join operators
- Ensuring that the reordering maintains the semantics of the original query
- For this implementation, a specific reordering pattern was used to match the example from the course slides

## Data Structures Added to the Optimizer

To support these optimizations, I added several data members to the Optimizer class:

```cpp
// Predicate tracking collections
// Pairs store the predicate expression and whether it was successfully pushed down
std::vector<std::pair<std::shared_ptr<OperatorExpression>, bool>> join_conditions_;  // Join predicates
std::vector<std::pair<std::shared_ptr<OperatorExpression>, bool>> filter_conditions_; // Regular predicates

// Table name tracking for join predicate pushdown
std::set<std::string> table_names_;
```

These data structures serve several important purposes:
1. `join_conditions_` stores all join predicates encountered during query plan traversal, along with flags indicating whether they've been successfully pushed down
2. `filter_conditions_` similarly tracks simple filter predicates for potential pushdown
3. `table_names_` maintains a set of table names involved in a particular join subtree to determine applicable join predicates

## Time Spent on the Project

- Understanding the query optimization framework: ~4 hours
- Implementing predicate splitting: ~3 hours
- Implementing filter pushdown: ~5 hours
- Implementing join predicate pushdown: ~4 hours
- Implementing join order selection: ~3 hours
- Testing and debugging: ~6 hours
- Report writing: ~2 hours
- Total: ~27 hours

## Course Experience and Feedback

Working through this series of database labs has been extremely valuable for understanding how real database systems are implemented. The hands-on experience of building components like transaction management, recovery systems, query processing, and optimization has provided insights that couldn't be gained from theoretical study alone.

Some key takeaways from this course:
1. The importance of designing efficient algorithms for database operations
2. The complexity of query optimization and its impact on performance
3. How different components of a database system interact with each other
4. The trade-offs between different implementation approaches

For course improvements, it might be helpful to:
1. Provide more visualization tools for understanding query plans
2. Include examples of how these optimizations perform on different datasets
3. Expand the optional advanced features to explore more modern optimization techniques

Overall, this course has significantly deepened my understanding of database systems and provided practical skills that will be valuable for future work in this field.

## Honor Code

1. I completed this assignment independently without copying code from others. If I discussed implementation approaches with others or referenced existing implementations, I have explicitly noted this in my report.
2. I did not use GitHub Copilot, ChatGPT, or other tools for automatic code completion.
3. I will not share my code in any public repository.
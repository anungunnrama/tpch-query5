#ifndef SQL_ENGINE_HPP
#define SQL_ENGINE_HPP

#include <vector>
#include <map>
#include <string>
#include <functional>
#include <algorithm>
#include <numeric>

// ============================================================================
// SQL ENGINE - Reusable SQL Clause Implementations
// ============================================================================

namespace SQLEngine {

// Type alias for a table row
using Row = std::map<std::string, std::string>;
using Table = std::vector<Row>;

// Type alias for predicate functions
using Predicate = std::function<bool(const Row&)>;
using JoinPredicate = std::function<bool(const Row&, const Row&)>;

// ============================================================================
// SELECT Clause
// ============================================================================

/**
 * SELECT specific columns from a table
 * SQL: SELECT col1, col2, col3 FROM table
 */
Table SELECT(const Table& table, const std::vector<std::string>& columns) {
    Table result;
    for (const auto& row : table) {
        Row new_row;
        for (const auto& col : columns) {
            if (row.find(col) != row.end()) {
                new_row[col] = row.at(col);
            }
        }
        result.push_back(new_row);
    }
    return result;
}

/**
 * SELECT * (all columns)
 * SQL: SELECT * FROM table
 */
Table SELECT_ALL(const Table& table) {
    return table;  // Return as-is
}


// ============================================================================
// WHERE Clause
// ============================================================================

/**
 * WHERE clause - filter rows based on predicate
 * SQL: SELECT * FROM table WHERE condition
 */
Table WHERE(const Table& table, Predicate predicate) {
    Table result;
    for (const auto& row : table) {
        if (predicate(row)) {
            result.push_back(row);
        }
    }
    return result;
}

/**
 * WHERE with multiple conditions (AND)
 * SQL: WHERE cond1 AND cond2 AND cond3
 */
Table WHERE_AND(const Table& table, const std::vector<Predicate>& predicates) {
    Table result;
    for (const auto& row : table) {
        bool all_match = true;
        for (const auto& pred : predicates) {
            if (!pred(row)) {
                all_match = false;
                break;
            }
        }
        if (all_match) {
            result.push_back(row);
        }
    }
    return result;
}

/**
 * WHERE with multiple conditions (OR)
 * SQL: WHERE cond1 OR cond2 OR cond3
 */
Table WHERE_OR(const Table& table, const std::vector<Predicate>& predicates) {
    Table result;
    for (const auto& row : table) {
        bool any_match = false;
        for (const auto& pred : predicates) {
            if (pred(row)) {
                any_match = true;
                break;
            }
        }
        if (any_match) {
            result.push_back(row);
        }
    }
    return result;
}

// ============================================================================
// JOIN Clauses
// ============================================================================

/**
 * INNER JOIN - join two tables based on predicate
 * SQL: SELECT * FROM table1 INNER JOIN table2 ON condition
 */
Table INNER_JOIN(const Table& left_table, const Table& right_table, 
                 JoinPredicate join_condition) {
    Table result;
    for (const auto& left_row : left_table) {
        for (const auto& right_row : right_table) {
            if (join_condition(left_row, right_row)) {
                // Merge rows
                Row merged_row = left_row;
                for (const auto& [key, value] : right_row) {
                    merged_row[key] = value;
                }
                result.push_back(merged_row);
            }
        }
    }
    return result;
}

/**
 * LEFT JOIN - left outer join
 * SQL: SELECT * FROM table1 LEFT JOIN table2 ON condition
 */
Table LEFT_JOIN(const Table& left_table, const Table& right_table,
                JoinPredicate join_condition) {
    Table result;
    for (const auto& left_row : left_table) {
        bool found_match = false;
        for (const auto& right_row : right_table) {
            if (join_condition(left_row, right_row)) {
                Row merged_row = left_row;
                for (const auto& [key, value] : right_row) {
                    merged_row[key] = value;
                }
                result.push_back(merged_row);
                found_match = true;
            }
        }
        // If no match found, add left row with NULLs
        if (!found_match) {
            result.push_back(left_row);
        }
    }
    return result;
}

/**
 * CROSS JOIN - Cartesian product
 * SQL: SELECT * FROM table1 CROSS JOIN table2
 */
Table CROSS_JOIN(const Table& left_table, const Table& right_table) {
    Table result;
    for (const auto& left_row : left_table) {
        for (const auto& right_row : right_table) {
            Row merged_row = left_row;
            for (const auto& [key, value] : right_row) {
                merged_row[key] = value;
            }
            result.push_back(merged_row);
        }
    }
    return result;
}

// ============================================================================
// GROUP BY Clause
// ============================================================================

/**
 * GROUP BY - group rows by column values
 * SQL: SELECT col, AGG(col2) FROM table GROUP BY col
 * Returns: Map of group_key -> group_rows
 */
std::map<std::string, Table> GROUP_BY(const Table& table, const std::string& group_column) {
    std::map<std::string, Table> groups;
    for (const auto& row : table) {
        if (row.find(group_column) != row.end()) {
            std::string key = row.at(group_column);
            groups[key].push_back(row);
        }
    }
    return groups;
}

/**
 * GROUP BY with multiple columns
 * SQL: GROUP BY col1, col2, col3
 */
std::map<std::string, Table> GROUP_BY_MULTI(const Table& table, 
                                             const std::vector<std::string>& group_columns) {
    std::map<std::string, Table> groups;
    for (const auto& row : table) {
        // Create composite key
        std::string composite_key;
        for (const auto& col : group_columns) {
            if (row.find(col) != row.end()) {
                composite_key += row.at(col) + "|";
            }
        }
        groups[composite_key].push_back(row);
    }
    return groups;
}

// ============================================================================
// Aggregate Functions
// ============================================================================

/**
 * SUM aggregate function
 * SQL: SUM(column)
 */
double SUM(const Table& group, const std::string& column) {
    double sum = 0.0;
    for (const auto& row : group) {
        if (row.find(column) != row.end()) {
            sum += std::stod(row.at(column));
        }
    }
    return sum;
}

/**
 * COUNT aggregate function
 * SQL: COUNT(*)
 */
size_t COUNT(const Table& group) {
    return group.size();
}

/**
 * COUNT with column
 * SQL: COUNT(column)
 */
size_t COUNT_COLUMN(const Table& group, const std::string& column) {
    size_t count = 0;
    for (const auto& row : group) {
        if (row.find(column) != row.end() && !row.at(column).empty()) {
            count++;
        }
    }
    return count;
}

/**
 * AVG aggregate function
 * SQL: AVG(column)
 */
double AVG(const Table& group, const std::string& column) {
    if (group.empty()) return 0.0;
    return SUM(group, column) / group.size();
}

/**
 * MAX aggregate function
 * SQL: MAX(column)
 */
double MAX(const Table& group, const std::string& column) {
    if (group.empty()) return 0.0;
    double max_val = std::stod(group[0].at(column));
    for (const auto& row : group) {
        if (row.find(column) != row.end()) {
            double val = std::stod(row.at(column));
            if (val > max_val) max_val = val;
        }
    }
    return max_val;
}

/**
 * MIN aggregate function
 * SQL: MIN(column)
 */
double MIN(const Table& group, const std::string& column) {
    if (group.empty()) return 0.0;
    double min_val = std::stod(group[0].at(column));
    for (const auto& row : group) {
        if (row.find(column) != row.end()) {
            double val = std::stod(row.at(column));
            if (val < min_val) min_val = val;
        }
    }
    return min_val;
}

/**
 * Apply aggregation after GROUP BY
 * SQL: SELECT group_col, SUM(agg_col) FROM table GROUP BY group_col
 */
Table AGGREGATE(const std::map<std::string, Table>& groups,
                const std::string& group_column_name,
                const std::map<std::string, std::function<double(const Table&)>>& agg_functions) {
    Table result;
    for (const auto& [group_key, group_rows] : groups) {
        Row result_row;
        result_row[group_column_name] = group_key;
        
        // Apply each aggregate function
        for (const auto& [agg_name, agg_func] : agg_functions) {
            result_row[agg_name] = std::to_string(agg_func(group_rows));
        }
        
        result.push_back(result_row);
    }
    return result;
}

// ============================================================================
// ORDER BY Clause
// ============================================================================

/**
 * ORDER BY - sort table by column
 * SQL: ORDER BY column ASC
 */
Table ORDER_BY_ASC(Table table, const std::string& column) {
    std::sort(table.begin(), table.end(),
              [&column](const Row& a, const Row& b) {
                  return a.at(column) < b.at(column);
              });
    return table;
}

/**
 * ORDER BY - sort table by column descending
 * SQL: ORDER BY column DESC
 */
Table ORDER_BY_DESC(Table table, const std::string& column) {
    std::sort(table.begin(), table.end(),
              [&column](const Row& a, const Row& b) {
                  return a.at(column) > b.at(column);
              });
    return table;
}

/**
 * ORDER BY - sort by numeric column ascending
 */
Table ORDER_BY_NUMERIC_ASC(Table table, const std::string& column) {
    std::sort(table.begin(), table.end(),
              [&column](const Row& a, const Row& b) {
                  return std::stod(a.at(column)) < std::stod(b.at(column));
              });
    return table;
}

/**
 * ORDER BY - sort by numeric column descending
 */
Table ORDER_BY_NUMERIC_DESC(Table table, const std::string& column) {
    std::sort(table.begin(), table.end(),
              [&column](const Row& a, const Row& b) {
                  return std::stod(a.at(column)) > std::stod(b.at(column));
              });
    return table;
}

/**
 * ORDER BY multiple columns
 * SQL: ORDER BY col1 DESC, col2 ASC
 */
Table ORDER_BY_MULTI(Table table, 
                     const std::vector<std::pair<std::string, bool>>& order_specs) {
    // order_specs: vector of (column_name, is_ascending)
    std::sort(table.begin(), table.end(),
              [&order_specs](const Row& a, const Row& b) {
                  for (const auto& [col, is_asc] : order_specs) {
                      if (a.at(col) != b.at(col)) {
                          return is_asc ? (a.at(col) < b.at(col)) : (a.at(col) > b.at(col));
                      }
                  }
                  return false;
              });
    return table;
}

// ============================================================================
// LIMIT Clause
// ============================================================================

/**
 * LIMIT - return first N rows
 * SQL: LIMIT n
 */
Table LIMIT(const Table& table, size_t n) {
    if (table.size() <= n) return table;
    return Table(table.begin(), table.begin() + n);
}

/**
 * OFFSET - skip first N rows
 * SQL: OFFSET n
 */
Table OFFSET(const Table& table, size_t n) {
    if (table.size() <= n) return Table();
    return Table(table.begin() + n, table.end());
}

/**
 * LIMIT with OFFSET
 * SQL: LIMIT n OFFSET m
 */
Table LIMIT_OFFSET(const Table& table, size_t limit, size_t offset) {
    if (table.size() <= offset) return Table();
    size_t end = std::min(offset + limit, table.size());
    return Table(table.begin() + offset, table.begin() + end);
}

// ============================================================================
// DISTINCT Clause
// ============================================================================

/**
 * DISTINCT - remove duplicate rows
 * SQL: SELECT DISTINCT * FROM table
 */
Table DISTINCT(const Table& table) {
    Table result;
    std::set<std::string> seen;
    
    for (const auto& row : table) {
        // Create a string representation of the row
        std::string row_str;
        for (const auto& [key, value] : row) {
            row_str += key + ":" + value + "|";
        }
        
        if (seen.find(row_str) == seen.end()) {
            seen.insert(row_str);
            result.push_back(row);
        }
    }
    return result;
}

/**
 * DISTINCT on specific columns
 * SQL: SELECT DISTINCT col1, col2 FROM table
 */
Table DISTINCT_COLUMNS(const Table& table, const std::vector<std::string>& columns) {
    Table result;
    std::set<std::string> seen;
    
    for (const auto& row : table) {
        std::string key;
        for (const auto& col : columns) {
            if (row.find(col) != row.end()) {
                key += row.at(col) + "|";
            }
        }
        
        if (seen.find(key) == seen.end()) {
            seen.insert(key);
            result.push_back(row);
        }
    }
    return result;
}

// ============================================================================
// UNION Clause
// ============================================================================

/**
 * UNION - combine two tables (removes duplicates)
 * SQL: SELECT * FROM table1 UNION SELECT * FROM table2
 */
Table UNION(const Table& table1, const Table& table2) {
    Table combined = table1;
    combined.insert(combined.end(), table2.begin(), table2.end());
    return DISTINCT(combined);
}

/**
 * UNION ALL - combine two tables (keeps duplicates)
 * SQL: SELECT * FROM table1 UNION ALL SELECT * FROM table2
 */
Table UNION_ALL(const Table& table1, const Table& table2) {
    Table result = table1;
    result.insert(result.end(), table2.begin(), table2.end());
    return result;
}

// ============================================================================
// Helper Predicate Builders
// ============================================================================

/**
 * Build equality predicate: column = value
 * SQL: WHERE column = 'value'
 */
Predicate EQUALS(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) == value;
    };
}

/**
 * Build comparison predicate: column > value
 * SQL: WHERE column > value
 */
Predicate GREATER_THAN(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) > value;
    };
}

/**
 * Build comparison predicate: column >= value
 * SQL: WHERE column >= value
 */
Predicate GREATER_EQUAL(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) >= value;
    };
}

/**
 * Build comparison predicate: column < value
 * SQL: WHERE column < value
 */
Predicate LESS_THAN(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) < value;
    };
}

/**
 * Build comparison predicate: column <= value
 * SQL: WHERE column <= value
 */
Predicate LESS_EQUAL(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) <= value;
    };
}

/**
 * Build IN predicate: column IN (val1, val2, ...)
 * SQL: WHERE column IN ('val1', 'val2')
 */
Predicate IN(const std::string& column, const std::vector<std::string>& values) {
    return [column, values](const Row& row) {
        if (row.find(column) == row.end()) return false;
        return std::find(values.begin(), values.end(), row.at(column)) != values.end();
    };
}

/**
 * Build join predicate: table1.col1 = table2.col2
 */
JoinPredicate JOIN_ON(const std::string& left_column, const std::string& right_column) {
    return [left_column, right_column](const Row& left, const Row& right) {
        return left.find(left_column) != left.end() && 
               right.find(right_column) != right.end() &&
               left.at(left_column) == right.at(right_column);
    };
}

} // namespace SQLEngine

#endif // SQL_ENGINE_HPP
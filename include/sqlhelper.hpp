#ifndef SQL_ENGINE_MINIMAL_HPP
#define SQL_ENGINE_MINIMAL_HPP

#include <vector>
#include <map>
#include <string>
#include <functional>
#include <algorithm>
#include <set>
#include <thread>


namespace SQLEngine {

// Type alias for a table row
using Row = std::map<std::string, std::string>;
using Table = std::vector<Row>;

// Type alias for predicate functions
using Predicate = std::function<bool(const Row&)>;
using JoinPredicate = std::function<bool(const Row&, const Row&)>;


Table WHERE(const Table& table, Predicate predicate) {
    Table result;
    for (const auto& row : table) {
        if (predicate(row)) {
            result.push_back(row);
        }
    }
    return result;
}

// JOIN Clause (Required for all the table joins)

Table INNER_JOIN(const Table& left_table, const Table& right_table,
                               const std::string& left_column, const std::string& right_column,
                               int num_threads) {
    
    // Build hash index on right table (shared across threads)
    std::map<std::string, std::vector<size_t>> right_index;
    for (size_t i = 0; i < right_table.size(); i++) {
        if (right_table[i].find(right_column) != right_table[i].end()) {
            right_index[right_table[i].at(right_column)].push_back(i);
        }
    }
    
    // Divide left table among threads
    size_t chunk_size = (left_table.size() + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;
    std::vector<Table> thread_results(num_threads);
    
    auto worker = [&](int thread_id, size_t start_idx, size_t end_idx) {
        Table local_result;
        for (size_t i = start_idx; i < end_idx && i < left_table.size(); i++) {
            const auto& left_row = left_table[i];
            if (left_row.find(left_column) == left_row.end()) continue;
            
            const std::string& key = left_row.at(left_column);
            auto it = right_index.find(key);
            if (it != right_index.end()) {
                for (size_t right_idx : it->second) {
                    Row merged_row = left_row;
                    for (const auto& [k, v] : right_table[right_idx]) {
                        merged_row[k] = v;
                    }
                    local_result.push_back(merged_row);
                }
            }
        }
        thread_results[thread_id] = std::move(local_result);
    };
    
    // Launch threads & wait for threads to merge
    for (int i = 0; i < num_threads; i++) {
        size_t start_idx = i * chunk_size;
        size_t end_idx = start_idx + chunk_size;
        threads.emplace_back(worker, i, start_idx, end_idx);
    }
    
    for (auto& t : threads) t.join();
    
    
    // Merge results
    Table result;
    for (const auto& thread_result : thread_results) {
        result.insert(result.end(), thread_result.begin(), thread_result.end());
    }
    
    return result;
}

// GROUP BY Clause (Required for: GROUP BY n_name)
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

// Aggregate Fx: SUM(column)
double SUM(const Table& group, const std::string& column) {
    double sum = 0.0;
    for (const auto& row : group) {
        if (row.find(column) != row.end()) {
            sum += std::stod(row.at(column));
        }
    }
    return sum;
}

// ORDER BY Clause (Required for: ORDER BY revenue DESC)
Table ORDER_BY_DESC(Table table, const std::string& column) {
    std::sort(table.begin(), table.end(),
              [&column](const Row& a, const Row& b) {
                  return std::stod(a.at(column)) > std::stod(b.at(column));
              });
    return table;
}

// Helper Predicate Builders

Predicate EQUALS(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) == value;
    };
}

Predicate GREATER_EQUAL(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) >= value;
    };
}

Predicate LESS_THAN(const std::string& column, const std::string& value) {
    return [column, value](const Row& row) {
        return row.find(column) != row.end() && row.at(column) < value;
    };
}

// Build join predicate: table1.col1 = table2.col2
JoinPredicate JOIN_ON(const std::string& left_column, const std::string& right_column) {
    return [left_column, right_column](const Row& left, const Row& right) {
        return left.find(left_column) != left.end() && 
               right.find(right_column) != right.end() &&
               left.at(left_column) == right.at(right_column);
    };
}

}

#endif
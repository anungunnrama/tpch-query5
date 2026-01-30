#include "../include/query5.hpp"
#include "../include/utilities.hpp"
#include "../include/sqlhelper.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    std::unordered_map<std::string, std::string> options;
    for (int i = 1; i < argc; )
    {
        std::string key(argv[i]);

        if (key.size() < 3 || key.substr(0, 2) != "--") return false;

        key = key.substr(2);

        if (key.empty()) return false;
        if (i + 1 >= argc) return false;

        std::string value(argv[i + 1]);
        // value starts with -- , ie another option
        if (value.size() >= 2 && value.substr(0, 2) == "--") return false;
        //no duplicate keys
        if (!options.emplace(key, value).second) return false;

        i += 2;
    }

    if (options.count("r_name") == 0 || options.count("start_date") == 0 || options.count("end_date") == 0 || options.count("threads") == 0 || options.count("table_path") == 0 || options.count("result_path") == 0) return false;
    else
        {
        r_name = std::move(options["r_name"]);
        start_date = std::move(options["start_date"]);
        end_date = std::move(options["end_date"]);
        table_path = std::move(options["table_path"]);
        result_path = std::move(options["result_path"]);
        }

    try { num_threads = std::stoi(options["threads"]); }
    catch (...) { return false; }

    if (num_threads <= 0) return false;

    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    
    std::string path_prefix = table_path;
    if (!path_prefix.empty() && path_prefix.back() != '/') {
        path_prefix += "/";
    }
    // columns for each table
    std::vector<std::string> customer_cols = {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", 
                                               "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"};
    std::vector<std::string> orders_cols = {"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE",
                                            "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", 
                                            "O_SHIPPRIORITY", "O_COMMENT"};
    std::vector<std::string> lineitem_cols = {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER",
                                              "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
                                              "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", 
                                              "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT",
                                              "L_SHIPMODE", "L_COMMENT"};
    std::vector<std::string> supplier_cols = {"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY",
                                              "S_PHONE", "S_ACCTBAL", "S_COMMENT"};
    std::vector<std::string> nation_cols = {"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"};
    std::vector<std::string> region_cols = {"R_REGIONKEY", "R_NAME", "R_COMMENT"};
    
    // Read each table
    if (!readTable(path_prefix + "customer.tbl", customer_cols, customer_data)) return false;
    if (!readTable(path_prefix + "orders.tbl", orders_cols, orders_data)) return false;
    if (!readTable(path_prefix + "lineitem.tbl", lineitem_cols, lineitem_data)) return false;
    if (!readTable(path_prefix + "supplier.tbl", supplier_cols, supplier_data)) return false;
    if (!readTable(path_prefix + "nation.tbl", nation_cols, nation_data)) return false;
    if (!readTable(path_prefix + "region.tbl", region_cols, region_data)) return false;
    
    return true;
}


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    using namespace SQLEngine;
    
    Table filtered_region = WHERE(region_data, EQUALS("R_NAME", r_name));
    
    if (filtered_region.empty()) {
        std::cerr << "ERROR: No matching region found!" << std::endl;
        return false;
    }
    

    // JOIN nation with region (n_regionkey = r_regionkey)
    Table nation_region = INNER_JOIN(nation_data, filtered_region, "N_REGIONKEY", "R_REGIONKEY",num_threads );
    
    // JOIN customer with nation (c_nationkey = n_nationkey)
    Table customer_nation = INNER_JOIN(customer_data, nation_region, "C_NATIONKEY", "N_NATIONKEY", num_threads);
    
    // WHERE o_orderdate >= start_date AND o_orderdate < end_date
    Table filtered_orders = WHERE(orders_data, 
        [&start_date, &end_date](const Row& row) {
            const std::string& date = row.at("O_ORDERDATE");
            return date >= start_date && date < end_date;
        });
    

    // JOIN customer_nation with orders (c_custkey = o_custkey)
    // Use parallel join since orders table is large
    Table customer_orders = INNER_JOIN(customer_nation, filtered_orders,
                                                     "C_CUSTKEY", "O_CUSTKEY",
                                                     num_threads);
    

    // JOIN supplier with nation (s_nationkey = n_nationkey)
    Table supplier_nation = INNER_JOIN(supplier_data, nation_region,"S_NATIONKEY", "N_NATIONKEY",num_threads);
    

    // JOIN lineitem with customer_orders (l_orderkey = o_orderkey)
    // This is the most expensive join - use parallel processing
    Table lineitem_orders = INNER_JOIN(lineitem_data, customer_orders, "L_ORDERKEY", "O_ORDERKEY", num_threads);
    

    // This is a composite join condition, so we first join on l_suppkey = s_suppkey
    // then filter where c_nationkey = s_nationkey
    Table temp_join = INNER_JOIN(lineitem_orders, supplier_nation, "L_SUPPKEY", "S_SUPPKEY", num_threads);
    Table full_join = WHERE(temp_join, [](const Row& row) {
        return row.at("C_NATIONKEY") == row.at("S_NATIONKEY");
    });
    
    

    // Compute revenue: l_extendedprice * (1 - l_discount)
    Table with_revenue;
    for (const auto& row : full_join) {
        Row new_row;
        new_row["N_NAME"] = row.at("N_NAME");
        
        double price = std::stod(row.at("L_EXTENDEDPRICE"));
        double discount = std::stod(row.at("L_DISCOUNT"));
        double revenue = price * (1.0 - discount);
        new_row["REVENUE"] = std::to_string(revenue);
        
        with_revenue.push_back(new_row);
    }
    std::cout << "        Result: " << with_revenue.size() << " rows with revenue\n" << std::endl;
    

    // GROUP BY n_name
    auto grouped = GROUP_BY(with_revenue, "N_NAME");
    

    // SUM(revenue) for each group
    Table aggregated;
    for (const auto& [nation_name, group_rows] : grouped) {
        Row result_row;
        result_row["N_NAME"] = nation_name;
        result_row["REVENUE"] = std::to_string(SUM(group_rows, "REVENUE"));
        aggregated.push_back(result_row);
    }
    

    // ORDER BY revenue DESC
    Table sorted = ORDER_BY_DESC(aggregated, "REVENUE");
    
    for (const auto& row : sorted) {
        results[row.at("N_NAME")] = std::stod(row.at("REVENUE"));
    }
    
    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to open output file: " << result_path << std::endl;
        return false;
    }
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    outfile << "N_NAME|REVENUE" << std::endl;
    for (const auto& pair : sorted_results) {
        outfile << pair.first << "|" << std::fixed << pair.second << std::endl;
    }
    
    outfile.close();
    return true;
}
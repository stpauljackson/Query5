#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <string>
#include <algorithm>

std::mutex results_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char *argv[], std::string &r_name, std::string &start_date, std::string &end_date, int &num_threads, std::string &table_path, std::string &result_path)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc)
            r_name = argv[++i];
        else if (arg == "--start_date" && i + 1 < argc)
            start_date = argv[++i];
        else if (arg == "--end_date" && i + 1 < argc)
            end_date = argv[++i];
        else if (arg == "--threads" && i + 1 < argc)
            num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path" && i + 1 < argc)
            table_path = argv[++i];
        else if (arg == "--result_path" && i + 1 < argc)
            result_path = argv[++i];
        else
        {
            std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() || num_threads <= 0 || table_path.empty() || result_path.empty())
    {
        std::cerr << "Missing required arguments." << std::endl;
        return false;
    }
    return true;
}


bool readDataFile(const std::string &filepath, std::vector<std::map<std::string, std::string>> &data, const std::vector<std::string> &columns, char delimiter = '|')
{
    std::ifstream file(filepath);
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line))
    {
        if (line.empty())
            continue;
        std::stringstream ss(line);
        std::string item;
        std::map<std::string, std::string> row;
        size_t colIndex = 0;

        while (std::getline(ss, item, delimiter))
        {
            if (colIndex < columns.size())
            {
                row[columns[colIndex]] = item;
            }
            ++colIndex;
        }
        data.push_back(row);
    }
    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string &table_path,
                  std::vector<std::map<std::string, std::string>> &customer_data,
                  std::vector<std::map<std::string, std::string>> &orders_data,
                  std::vector<std::map<std::string, std::string>> &lineitem_data,
                  std::vector<std::map<std::string, std::string>> &supplier_data,
                  std::vector<std::map<std::string, std::string>> &nation_data,
                  std::vector<std::map<std::string, std::string>> &region_data)
{
    // Define columns for each table (must match your .tbl files)
    std::vector<std::string> region_cols = {"r_regionkey", "r_name", "r_comment"};
    std::vector<std::string> nation_cols = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};
    std::vector<std::string> supplier_cols = {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"};
    std::vector<std::string> customer_cols = {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};
    std::vector<std::string> orders_cols = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"};
    std::vector<std::string> lineitem_cols = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

    bool ok = true;
    ok &= readDataFile(table_path + "/region.tbl", region_data, region_cols);
    ok &= readDataFile(table_path + "/nation.tbl", nation_data, nation_cols);
    ok &= readDataFile(table_path + "/supplier.tbl", supplier_data, supplier_cols);
    ok &= readDataFile(table_path + "/customer.tbl", customer_data, customer_cols);
    ok &= readDataFile(table_path + "/orders.tbl", orders_data, orders_cols);
    ok &= readDataFile(table_path + "/lineitem.tbl", lineitem_data, lineitem_cols);

    return ok;
}


void processPartition(int start_idx, int end_idx,
                      const std::string &r_name,
                      const std::string &start_date,
                      const std::string &end_date,
                      const std::vector<std::map<std::string, std::string>> &customer_data,
                      const std::vector<std::map<std::string, std::string>> &orders_data,
                      const std::vector<std::map<std::string, std::string>> &lineitem_data,
                      const std::vector<std::map<std::string, std::string>> &supplier_data,
                      const std::vector<std::map<std::string, std::string>> &nation_data,
                      const std::vector<std::map<std::string, std::string>> &region_data,
                      std::map<std::string, double> &partial_results)
{
    // TODO: Implement the core logic for Query 5 here for the assigned partition of orders_data

    // The general approach:
    // - Filter region by r_name
    // - Join region->nation->supplier->customer->orders->lineitem using keys and conditions
    // - Filter orders by orderdate between start_date and end_date
    // - Calculate revenue per nation name
    // - Store partial sums in partial_results

    // This is just a stub — replace with your actual implementation
}




std::mutex result_mutex;

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results
) {
    std::unordered_map<std::string, std::string> nation_map;
    std::unordered_map<std::string, std::string> region_map;
    std::unordered_set<std::string> region_keys;
    std::unordered_map<std::string, std::string> supp_nation_map;
    std::unordered_map<std::string, std::string> cust_nation_map;

    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name) {
            region_keys.insert(region.at("r_regionkey"));
        }
    }
    for (const auto& nation : nation_data) {
        if (region_keys.count(nation.at("n_regionkey"))) {
            nation_map[nation.at("n_nationkey")] = nation.at("n_name");
        }
    }

    for (const auto& supplier : supplier_data) {
        if (nation_map.count(supplier.at("s_nationkey"))) {
            supp_nation_map[supplier.at("s_suppkey")] = nation_map[supplier.at("s_nationkey")];
        }
    }
    for (const auto& customer : customer_data) {
        if (nation_map.count(customer.at("c_nationkey"))) {
            cust_nation_map[customer.at("c_custkey")] = nation_map[customer.at("c_nationkey")];
        }
    }

    std::unordered_map<std::string, std::string> order_cust_map;
    for (const auto& order : orders_data) {
        if (cust_nation_map.count(order.at("o_custkey")) &&
            order.at("o_orderdate") >= start_date &&
            order.at("o_orderdate") < end_date) {
            order_cust_map[order.at("o_orderkey")] = cust_nation_map[order.at("o_custkey")];
        }
    }

    auto worker = [&](int start, int end, std::map<std::string, double>& local_results) {
        for (int i = start; i < end; ++i) {
            const auto& item = lineitem_data[i];
            auto order_it = order_cust_map.find(item.at("l_orderkey"));
            auto supp_it = supp_nation_map.find(item.at("l_suppkey"));
            if (order_it != order_cust_map.end() && supp_it != supp_nation_map.end()) {
                if (order_it->second == supp_it->second) {
                    double extended_price = std::stod(item.at("l_extendedprice"));
                    double discount = std::stod(item.at("l_discount"));
                    local_results[order_it->second] += extended_price * (1 - discount);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    std::vector<std::map<std::string, double>> local_results(num_threads);
    int chunk_size = lineitem_data.size() / num_threads;
    for (int t = 0; t < num_threads; ++t) {
        int start = t * chunk_size;
        int end = (t == num_threads - 1) ? lineitem_data.size() : (t + 1) * chunk_size;
        threads.emplace_back(worker, start, end, std::ref(local_results[t]));
    }

    for (auto& t : threads) t.join();

    for (const auto& local : local_results) {
        for (const auto& pair : local) {
            results[pair.first] += pair.second;
        }
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string &result_path, const std::map<std::string, double> &results)
{
    std::ofstream out(result_path);
    if (!out.is_open())
    {
        std::cerr << "Failed to open output file: " << result_path << std::endl;
        return false;
    }
    
    for (const auto &[nation, revenue] : results)
    {
        std::cout << "Nation: " << nation << ", Revenue: " << revenue << std::endl;
        out << nation << " | " << revenue << std::endl;
    }

    out.close();
    return true;
}

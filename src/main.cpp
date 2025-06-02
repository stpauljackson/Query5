#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <chrono>
int main(int argc, char *argv[])
{
    auto total_start = std::chrono::high_resolution_clock::now();
    std::cout << "Starting..." << std::endl;
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;
    auto start = std::chrono::high_resolution_clock::now();

    auto t1 = std::chrono::high_resolution_clock::now();
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path))
    {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "parseArgs took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " ms\n";

    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    t1 = std::chrono::high_resolution_clock::now();
    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data))
    {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }
    t2 = std::chrono::high_resolution_clock::now();

    std::cout << "readTPCHData took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " ms\n";
    std::map<std::string, double> results;

    t1 = std::chrono::high_resolution_clock::now();
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results))
    {
        std::cerr << "Failed to execute TPCH Query 5" << std::endl;
        return 1;
    }
    t2 = std::chrono::high_resolution_clock::now();

    std::cout << "executeQuery5 took "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count()
              << " ms\n";

    if (!outputResults(result_path, results))
    {
        std::cerr << "Failed to output results" << std::endl;
        return 1;
    }
    auto total_end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execution time with " << num_threads << " threads: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(total_end - total_start).count()
              << " ms" << std::endl;
    return 0;
}

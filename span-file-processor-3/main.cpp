#include "span-parser.h"
#include "logger.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <regex>
#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>
#include <chrono>


Logger logger("app.log");

extern int phyCounter;
extern int futCounter;
extern int optCounter;


int main(int argc, char* argv[]) {

    auto startTime = std::chrono::high_resolution_clock::now();
    try {  
        logger.log("Starting application");

        if (argc != 3) {
            logger.log("Less command line arguments", LogLevel::ERRORS);
            return 1;
        }

        const std::string configPath = argv[1];
        const std::string spanFilePath = argv[2];
        SQLHENV hEnv = nullptr;
        SQLHDBC hDbc = nullptr;

        std::wstring connStr;
        if (!readConnectionString(configPath, connStr)) {
            logger.log("Failed to read db-config.ini", LogLevel::ERRORS);
            return 1;
        }
        logger.log("db-Connstr formed: " + std::string(connStr.begin(), connStr.end()), LogLevel::INFO);

        std::ifstream file(spanFilePath);
        if (!file.is_open()) {
            logger.log("Failed to open SPAN file.", LogLevel::ERRORS);
            return 1;
        }
        logger.log("SPAN file opened successfully", LogLevel::INFO);

        ConfigMap config = loadTagConfigIni("tags-config.ini");
        if (config.empty()) {
            logger.log("Failed to load tag config file.", LogLevel::ERRORS);
            return 1;
        }
        logger.log("tags-config file loaded successfully", LogLevel::INFO);
        //   logConfigMap(config);

        if (!connectToMSSQL(hEnv, hDbc, connStr)) {
            logger.log("DB connection failed.", LogLevel::ERRORS);
            return 1;
        }
        logger.log("Connected to database successfully", LogLevel::INFO);


        // Load full file into string
        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();

        // Match all <phyPf>, <futPf>, <oofPf> blocks
        std::string tag1 = config["blocks"]["PHYPF_BLOCK_TAG"];
        std::string tag2 = config["blocks"]["FUTPF_BLOCK_TAG"];
        std::string tag3 = config["blocks"]["OOPF_BLOCK_TAG"];

        std::string pattern = "<(" + tag1 + "|" + tag2 + "|" + tag3 + ")>([\\s\\S]*?)</\\1>";
        std::regex blockRgx(pattern);
        auto begin = std::sregex_iterator(content.begin(), content.end(), blockRgx);
        auto end = std::sregex_iterator();

        std::vector<SpanRecord> records;

        for (auto it = begin; it != end; ++it) {
            std::string block = (*it).str();
            parseSpanXmlBlock(block, records, config);
        }
        logger.log("Parsing of file records into vector of SpanRecord is done.", LogLevel::INFO);


        bool flag = insertSpanRecords(hDbc, records);
        if (!flag)
            logger.log("Failed to insert span records", LogLevel::ERRORS);
        else
            logger.log("Span records inserted successfully", LogLevel::INFO);

        logger.log("phyCounter=" + std::to_string(phyCounter) + ", futCounter=" + std::to_string(futCounter) + ", optCounter=" + std::to_string(optCounter), LogLevel::INFO);


        std::cout << "Span records inserted successfully\n";
        SQLDisconnect(hDbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
        SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        double seconds = duration / 1000.0;
        logger.log("Total time taken to process: " + std::to_string(seconds) + " sec", LogLevel::INFO);


        return 0;
    }
    catch (const std::exception& ex) {
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        double seconds = duration / 1000.0;

        logger.log("Unhandled exception: " + std::string(ex.what()), LogLevel::ERRORS);
        logger.log("Time taken before failure: " + std::to_string(seconds) + " sec", LogLevel::INFO);
        return 1;
    }
    catch (...) {
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        double seconds = duration / 1000.0;

        logger.log("Unknown fatal error occurred.", LogLevel::ERRORS);
        logger.log("Time taken before failure: " + std::to_string(seconds) + " sec", LogLevel::INFO);
        return 1;
    }

   

   
}

#include "span-parser.h"
#include "logger.h"
#include <fstream>
#include <iostream>
#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>

Logger logger("app.log");

int main(int argc, char* argv[]) {
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
    logger.log("span file opened", LogLevel::INFO);

    if (!connectToMSSQL(hEnv, hDbc, connStr)) {
        logger.log("DB connection failed.", LogLevel::ERRORS);
        return 1;
    }
    logger.log("Connected to database", LogLevel::INFO);

    std::string line;
    std::string block;
    std::vector<SpanRecord> records;
    bool insideBlock = false;

    while (std::getline(file, line)) {
        line.erase(line.begin(), std::find_if(line.begin(), line.end(), [](int ch) {
            return !std::isspace(ch);
            }));

        if (!insideBlock && (line.find("<phyPf>") != std::string::npos ||
            line.find("<futPf>") != std::string::npos ||
            line.find("<oofPf>") != std::string::npos)) {
            insideBlock = true;
            block = line + "\n";
            continue;
        }

        if (insideBlock) {
            block += line + "\n";
            if (line.find("</phyPf>") != std::string::npos ||
                line.find("</futPf>") != std::string::npos ||
                line.find("</oofPf>") != std::string::npos) {

                parseSpanXmlBlock(block, records);
                block.clear();
                insideBlock = false;
            }
        }
    }
    logger.log("parsing of file records into vector of SpanRecord is done.", LogLevel::INFO);


    bool flag = insertSpanRecords(hDbc, records);
    if (!flag)
        logger.log("Failed to insert span Records", LogLevel::INFO);
    else
        logger.log("Span Records inserted successfully", LogLevel::INFO);

    std::cout << "Span Records inserted successfully";
    SQLDisconnect(hDbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hDbc);
    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return 0;
}

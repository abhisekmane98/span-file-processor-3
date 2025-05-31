// Updated span-parser.cpp
#include "span-parser.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <iostream>
#include <sstream>
#include <fstream>
#include <regex>
#include <map>
#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>

extern Logger logger;  // use the shared logger
int phyCounter = 0;
int futCounter = 0;
int optCounter = 0;



ConfigMap loadTagConfigIni(const std::string& path) {
    std::ifstream file(path);
    ConfigMap config;

    if (!file.is_open())
        return config;

    std::string line, section;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#')
            continue;

        if (line[0] == '[' && line.back() == ']') {
            section = line.substr(1, line.length() - 2);
            continue;
        }

        size_t eq = line.find('=');
        if (eq == std::string::npos)
            continue;

        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        config[section][key] = val;
    }

    return config;
}

void logConfigMap(const ConfigMap& config) {
    logger.log("===== Parsed Tag Configuration =====");

    for (const auto& sectionPair : config) {
        logger.log("[" + sectionPair.first + "]");
        for (const auto& kv : sectionPair.second) {
            logger.log(kv.first + " = " + kv.second);
        }
    }
}

bool readConnectionString(const std::string& filePath, std::wstring& connStr) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        logger.log("Failed to open db-config.ini.", LogLevel::ERRORS);
        return false;
    }

    std::string line;
    std::map<std::string, std::string> config;

    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '[' || line[0] == '#')
            continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos)
            continue;
        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);
        config[key] = value;
    }

    std::ostringstream oss;
    oss << "DRIVER={ODBC Driver 17 for SQL Server};";
    oss << "SERVER=" << config["SERVER"] << ";";
    oss << "DATABASE=" << config["DATABASE"] << ";";
    oss << "UID=" << config["UID"] << ";";
    oss << "PWD=" << config["PWD"] << ";";

    std::string connStrUtf8 = oss.str();
    connStr = std::wstring(connStrUtf8.begin(), connStrUtf8.end());
    return true;
}

std::string extractTag(const std::string& block, const std::string& tag) {
    std::string open = "<" + tag + ">";
    std::string close = "</" + tag + ">";
    auto start = block.find(open);
    auto end = block.find(close);
    if (start != std::string::npos && end != std::string::npos && end > start) {
        start += open.length();
        return block.substr(start, end - start);
    }
    return "";
}

RiskArray extractRiskArray(const std::string& block, const ConfigMap& config) {
    RiskArray riskArray;

    std::string raTag = config.at("risk_array").at("RISK_ARRAY_TAG");
    size_t raStart = block.find("<" + raTag + ">");
    size_t raEnd = block.find("</" + raTag + ">");
    if (raStart != std::string::npos && raEnd != std::string::npos && raEnd > raStart) {
        std::string raContent = block.substr(raStart, raEnd - raStart + raTag.length() + 3);

        std::string rTag = config.at("risk_array").at("RISK_ARRAY_R");
        std::string dTag = config.at("risk_array").at("RISK_ARRAY_D");
        std::string aTag = config.at("risk_array").at("RISK_ARRAY_A");

        riskArray.r = safeStoi(extractTag(raContent, rTag));
        riskArray.d = safeStod(extractTag(raContent, dTag));

        size_t pos = 0;
        while ((pos = raContent.find("<" + aTag + ">", pos)) != std::string::npos) {
            size_t end = raContent.find("</" + aTag + ">", pos);
            if (end == std::string::npos)
                break;
            std::string val = raContent.substr(pos + aTag.length() + 2, end - pos - aTag.length() - 2);
            riskArray.a.push_back(safeStod(val));
            pos = end + aTag.length() + 3;
        }
    }

    return riskArray;
}

void parseSpanXmlBlock(const std::string& block, std::vector<SpanRecord>& recs, const ConfigMap& config) {
    SpanRecord rec;

    std::string segment;
    if (block.find(config.at("blocks").at("PHYPF_BLOCK_TAG")) != std::string::npos)
        segment = config.at("blocks").at("PHYPF_BLOCK_TAG");
    else if (block.find(config.at("blocks").at("FUTPF_BLOCK_TAG")) != std::string::npos)
        segment = config.at("blocks").at("FUTPF_BLOCK_TAG");
    else if (block.find(config.at("blocks").at("OOPF_BLOCK_TAG")) != std::string::npos)
        segment = config.at("blocks").at("OOPF_BLOCK_TAG");

    rec.segment = segment;

    // Root-level fields
    rec.pfId = safeStoi(extractTag(block, config.at("root_fields").at("PF_ID")));
    rec.pfCode = extractTag(block, config.at("root_fields").at("PF_CODE"));
    rec.currency = extractTag(block, config.at("root_fields").at("CURRENCY"));
    rec.cvf = safeStod(extractTag(block, config.at("root_fields").at("CVF")));
    rec.valueMeth = extractTag(block, config.at("root_fields").at("VALUE_METHOD"));
    rec.priceMeth = extractTag(block, config.at("root_fields").at("PRICE_METHOD"));
    rec.setlMeth = extractTag(block, config.at("root_fields").at("SETTLE_METHOD"));

   
    if (segment == config.at("blocks").at("PHYPF_BLOCK_TAG")) {
        std::regex phyRgx("<" + config.at("contracts").at("PHYPF_TAG") + ">([\\s\\S]*?)</" + config.at("contracts").at("PHYPF_TAG") + ">");
        std::smatch m;
        std::regex_search(block, m, phyRgx);
        std::string phyBlock = m[0];

        rec.contractId = safeStoi(extractTag(phyBlock, config.at("contract_fields").at("CONTRACT_ID")));
        rec.expiry = extractTag(phyBlock, config.at("contract_fields").at("EXPIRY"));
        rec.volatility = safeStod(extractTag(phyBlock, config.at("contract_fields").at("VOLATILITY")));
        rec.priceScan = safeStod(extractTag(phyBlock, config.at("contract_fields").at("PRICE_SCAN")));
        rec.volScan = safeStod(extractTag(phyBlock, config.at("contract_fields").at("VOL_SCAN")));
        rec.riskArray = extractRiskArray(phyBlock, config);

        phyCounter++;
        recs.push_back(rec);
    }
    else if (segment == config.at("blocks").at("FUTPF_BLOCK_TAG")) {
        std::regex futRgx("<" + config.at("contracts").at("FUTPF_TAG") + ">([\\s\\S]*?)</" + config.at("contracts").at("FUTPF_TAG") + ">");
        auto futBegin = std::sregex_iterator(block.begin(), block.end(), futRgx);
        auto futEnd = std::sregex_iterator();

        for (auto it = futBegin; it != futEnd; ++it) {
            std::string fut = (*it)[0];
            rec.contractId = safeStoi(extractTag(fut, config.at("contract_fields").at("CONTRACT_ID")));
            rec.expiry = extractTag(fut, config.at("contract_fields").at("EXPIRY"));
            rec.volatility = safeStod(extractTag(fut, config.at("contract_fields").at("VOLATILITY")));
            rec.settleDate = extractTag(fut, config.at("contract_fields").at("SETTLE_DATE"));

            std::string intrBlock = extractTag(fut, config.at("interest_rate").at("INTRA_RATE_PARENT"));
            rec.intraRate = safeStod(extractTag(intrBlock, config.at("interest_rate").at("INTRA_RATE_VALUE")));

            rec.priceScan = safeStod(extractTag(fut, config.at("contract_fields").at("PRICE_SCAN")));
            rec.volScan = safeStod(extractTag(fut, config.at("contract_fields").at("VOL_SCAN")));
            rec.riskArray = extractRiskArray(fut, config);

            futCounter++;
            recs.push_back(rec);
        }
    }
    else if (segment == config.at("blocks").at("OOPF_BLOCK_TAG")) {    
        rec.svf = safeStod(extractTag(block, config.at("root_fields").at("SVF")));

        std::regex seriesRgx("<" + config.at("contracts").at("OOPF_SERIES_TAG") + ">([\\s\\S]*?)</" + config.at("contracts").at("OOPF_SERIES_TAG") + ">");
        auto seriesBegin = std::sregex_iterator(block.begin(), block.end(), seriesRgx);
        auto seriesEnd = std::sregex_iterator();

        for (auto sit = seriesBegin; sit != seriesEnd; ++sit) {

            std::string series = (*sit)[0];
            rec.contractId = safeStoi(extractTag(series, config.at("contract_fields").at("CONTRACT_ID")));
            rec.expiry = extractTag(series, config.at("contract_fields").at("EXPIRY"));
            rec.settleDate = extractTag(series, config.at("contract_fields").at("SETTLE_DATE"));
            rec.volatility = safeStod(extractTag(series, config.at("contract_fields").at("VOLATILITY")));

            std::string intrBlock = extractTag(series, config.at("interest_rate").at("INTRA_RATE_PARENT"));
            rec.intraRate = safeStod(extractTag(series, config.at("interest_rate").at("INTRA_RATE_VALUE")));

            rec.priceScan = safeStod(extractTag(series, config.at("contract_fields").at("PRICE_SCAN")));
            rec.volScan = safeStod(extractTag(series, config.at("contract_fields").at("VOL_SCAN")));

            std::regex optRgx("<" + config.at("contracts").at("OOPF_OPT_TAG") + ">([\\s\\S]*?)</" + config.at("contracts").at("OOPF_OPT_TAG") + ">");
            auto optBegin = std::sregex_iterator(series.begin(), series.end(), optRgx);
            auto optEnd = std::sregex_iterator();

            for (auto oit = optBegin; oit != optEnd; ++oit) {
      
                std::string opt = (*oit)[0];
                rec.optContractId = safeStoi(extractTag(opt, config.at("option_fields").at("OPT_CONTRACT_ID")));
                rec.optionType = extractTag(opt, config.at("option_fields").at("OPTION_TYPE"));
                rec.strikePrice = safeStod(extractTag(opt, config.at("option_fields").at("STRIKE_PRICE")));
                rec.optionValue = safeStod(extractTag(opt, config.at("option_fields").at("OPTION_VALUE")));
                rec.riskArray = extractRiskArray(opt, config);

                optCounter++;
                recs.push_back(rec);
            }
        }
    }

}

std::wstring joinRiskArray(RiskArray riskArray) {
    std::wstringstream ss;
    ss << riskArray.r;

    for (size_t i = 0; i < riskArray.a.size(); ++i) {
        ss << riskArray.a[i];
        ss << L",";
    }
    ss << riskArray.d;

    return ss.str();
}

bool connectToMSSQL(SQLHENV& hEnv, SQLHDBC& hDbc, const std::wstring& connStr) {
    SQLRETURN ret;
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("hEnv SQLAllocHandle failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_ENV, hEnv, "SQLAllocHandle");
        return false;
    }

    ret = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("hEnv SQLSetEnvAttr failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_ENV, hEnv, "SQLSetEnvAttr");
        return false;
    }

    ret = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("hDbc SQLAllocHandle failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_DBC, hDbc, "SQLAllocHandle");
        return false;
    }

    SQLWCHAR outstr[1024];
    SQLSMALLINT outstrlen;
    ret = SQLDriverConnectW(hDbc, NULL, (SQLWCHAR*)connStr.c_str(),
        SQL_NTS, outstr, sizeof(outstr) / sizeof(SQLWCHAR), &outstrlen, SQL_DRIVER_COMPLETE);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("SQLDriverConnectW failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_DBC, hDbc, "SQLDriverConnectW");
        return false;
    }
    return true;
}

bool insertSpanRecords(SQLHDBC hDbc, const std::vector<SpanRecord>& records) {
    SQLHSTMT hStmt;
    SQLRETURN ret;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("hStmt SQLAllocHandle failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_STMT, hStmt, "SQLAllocHandle");
        return false;
    }

    const wchar_t* insertSQL =
        L"INSERT INTO SpanRecords6 (Segment, PfId, PfCode, Currency, CVF, SVF, ValueMeth, PriceMeth, SetlMeth,ContractId, Expiry, Volatility, SettleDate, IntraRate, PriceScan, VolScan, OptContractId, OptionType, StrikePrice, OptionValue, RiskArray) "
        L"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    ret = SQLPrepareW(hStmt, (SQLWCHAR*)insertSQL, SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        logger.log("hStmt SQLAllocHandle failed.", LogLevel::ERRORS);
        handleError(SQL_HANDLE_STMT, hStmt, "SQLAllocHandle");
        return false;
    }

    for (const auto& rec : records) {

        // printSpanRecords(rec);
        std::wstring segment(rec.segment.begin(), rec.segment.end());
        std::wstring pfCode(rec.pfCode.begin(), rec.pfCode.end());
        std::wstring currency(rec.currency.begin(), rec.currency.end());
        std::wstring valueMeth(rec.valueMeth.begin(), rec.valueMeth.end());
        std::wstring priceMeth(rec.priceMeth.begin(), rec.priceMeth.end());
        std::wstring setlMeth(rec.setlMeth.begin(), rec.setlMeth.end());
        std::wstring expiry(rec.expiry.begin(), rec.expiry.end());
        std::wstring settleDate(rec.settleDate.begin(), rec.settleDate.end());
        std::wstring optionType(rec.optionType.begin(), rec.optionType.end());
        std::wstring riskArray = joinRiskArray(rec.riskArray);

        SQLRETURN k[21];

        k[0] = SQLBindParameter(hStmt, 1, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 10, 0, (SQLPOINTER)segment.c_str(), 0, NULL);
        k[1] = SQLBindParameter(hStmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, (SQLPOINTER)&rec.pfId, 0, NULL);
        k[2] = SQLBindParameter(hStmt, 3, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 50, 0, (SQLPOINTER)pfCode.c_str(), 0, NULL);
        k[3] = SQLBindParameter(hStmt, 4, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 10, 0, (SQLPOINTER)currency.c_str(), 0, NULL);
        k[4] = SQLBindParameter(hStmt, 5, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.cvf, 0, NULL);
        k[5] = SQLBindParameter(hStmt, 6, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.svf, 0, NULL);
        k[6] = SQLBindParameter(hStmt, 7, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 20, 0, (SQLPOINTER)valueMeth.c_str(), 0, NULL);
        k[7] = SQLBindParameter(hStmt, 8, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 20, 0, (SQLPOINTER)priceMeth.c_str(), 0, NULL);
        k[8] = SQLBindParameter(hStmt, 9, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 20, 0, (SQLPOINTER)setlMeth.c_str(), 0, NULL);
        k[9] = SQLBindParameter(hStmt, 10, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, (SQLPOINTER)&rec.contractId, 0, NULL);
        k[10] = SQLBindParameter(hStmt, 11, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 10, 0, (SQLPOINTER)expiry.c_str(), 0, NULL);
        k[11] = SQLBindParameter(hStmt, 12, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.volatility, 0, NULL);
        k[12] = SQLBindParameter(hStmt, 13, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 10, 0, (SQLPOINTER)settleDate.c_str(), 0, NULL);
        k[13] = SQLBindParameter(hStmt, 14, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.intraRate, 0, NULL);
        k[14] = SQLBindParameter(hStmt, 15, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.priceScan, 0, NULL);
        k[15] = SQLBindParameter(hStmt, 16, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.volScan, 0, NULL);
        k[16] = SQLBindParameter(hStmt, 17, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, (SQLPOINTER)&rec.optContractId, 0, NULL);
        k[17] = SQLBindParameter(hStmt, 18, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 1, 0, (SQLPOINTER)optionType.c_str(), 0, NULL);
        k[18] = SQLBindParameter(hStmt, 19, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.strikePrice, 0, NULL);
        k[19] = SQLBindParameter(hStmt, 20, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_FLOAT, 0, 0, (SQLPOINTER)&rec.optionValue, 0, NULL);
        k[20] = SQLBindParameter(hStmt, 21, SQL_PARAM_INPUT, SQL_C_WCHAR, SQL_WVARCHAR, 0, 0, (SQLPOINTER)riskArray.c_str(), 0, NULL);

        // Check all return values
        for (int i = 0; i < 21; i++) {
            if (k[i] != SQL_SUCCESS && k[i] != SQL_SUCCESS_WITH_INFO) {
                handleError(SQL_HANDLE_STMT, hStmt, "SQLBindParameter", i + 1);
                return false;
            }
        }

        ret = SQLExecute(hStmt);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            logger.log("hStmt SQLExecute failed.", LogLevel::ERRORS);
            handleError(SQL_HANDLE_STMT, hStmt, "SQLExecute");
            return false;
        }
        SQLFreeStmt(hStmt, SQL_RESET_PARAMS);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    return true;
}

void printSpanRecords(const SpanRecord& rec) {

    std::cout << "Record " << 1 << ":\n";
    std::cout << "  Segment: " << rec.segment << "\n";
    std::cout << "  PfId: " << rec.pfId << "\n";
    std::cout << "  PfCode: " << rec.pfCode << "\n";
    std::cout << "  Currency: " << rec.currency << "\n";
    std::cout << "  CVF: " << rec.cvf << "\n";
    std::cout << "  SVF: " << rec.svf << "\n";
    std::cout << "  ValueMeth: " << rec.valueMeth << "\n";
    std::cout << "  PriceMeth: " << rec.priceMeth << "\n";
    std::cout << "  SetlPieceMeth: " << rec.setlMeth << "\n";
    std::cout << "  ContractId: " << rec.contractId << "\n";
    std::cout << "  Expiry: " << rec.expiry << "\n";
    std::cout << "  Volatility: " << rec.volatility << "\n";
    std::cout << "  SettleDate: " << rec.settleDate << "\n";
    std::cout << "  IntraRate: " << rec.intraRate << "\n";
    std::cout << "  PriceScan: " << rec.priceScan << "\n";
    std::cout << "  VolScan: " << rec.volScan << "\n";
    std::cout << "  OptContractId: " << rec.optContractId << "\n";
    std::cout << "  OptionType: " << rec.optionType << "\n";
    std::cout << "  StrikePrice: " << rec.strikePrice << "\n";
    std::cout << "  OptionValue: " << rec.optionValue << "\n";
    std::cout << "  RiskArray:\n";
    std::cout << "    r: " << rec.riskArray.r << "\n";
    std::cout << "    a: ";
    for (double val : rec.riskArray.a) {
        std::cout << val << " ";
    }
    std::cout << "\n";
    std::cout << "    d: " << rec.riskArray.d << "\n";
    std::cout << "------------------------------------\n";

}

// Function to handle and log ODBC errors
void handleError(SQLSMALLINT handleType, SQLHANDLE handle, const char* functionName, int paramNumber) {
    SQLWCHAR SQLState[6];         // error-code
    SQLINTEGER nativeError;       // Pointer to store the driver-specific error code
    SQLWCHAR message[1024];       // error-desc
    SQLSMALLINT maxLength = sizeof(message) / sizeof(SQLWCHAR);
    SQLSMALLINT length;           // Pointer to store the length of the returned message
    SQLSMALLINT recNumber = 1;

    // Convert const char* to std::wstring
    std::string strIntermediate(functionName);
    std::wstring wstrFunctionName(strIntermediate.begin(), strIntermediate.end());

    while (SQLGetDiagRecW(handleType, handle, recNumber, SQLState, &nativeError, message, maxLength, &length) == SQL_SUCCESS ||
        SQLGetDiagRecW(handleType, handle, recNumber, SQLState, &nativeError, message, maxLength, &length) == SQL_SUCCESS_WITH_INFO)
    {
        wprintf(L"[ODBC Error %d] %s%s%d: SQLSTATE=%s, NativeError=%d, Message=%s\n",
            recNumber, wstrFunctionName.c_str(), paramNumber ? L" (Parameter " : L" ", paramNumber, SQLState, nativeError, message);
        recNumber++; // Move to next record
    }
}

double safeStod(const std::string& value, double defaultVal) {
    try {
        return value.empty() ? defaultVal : std::stod(value);
    }
    catch (...) {
        return defaultVal;
    }
}

int safeStoi(const std::string& value, int defaultVal) {
    try {
        return value.empty() ? defaultVal : std::stoi(value);
    }
    catch (...) {
        return defaultVal;
    }
}

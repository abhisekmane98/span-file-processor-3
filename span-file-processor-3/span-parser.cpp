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

RiskArray extractRiskArray(const std::string& block) {
    RiskArray riskArray;
    size_t raStart = block.find("<ra>");
    size_t raEnd = block.find("</ra>");
    if (raStart != std::string::npos && raEnd != std::string::npos && raEnd > raStart) {
        std::string raContent = block.substr(raStart, raEnd - raStart + 5);
        riskArray.r = std::stoi(extractTag(raContent, "r"));
        riskArray.d = std::stod(extractTag(raContent, "d"));
        size_t pos = 0;

        while ((pos = raContent.find("<a>", pos)) != std::string::npos) {
            size_t end = raContent.find("</a>", pos);
            if (end == std::string::npos)
                break;
            std::string val = raContent.substr(pos + 3, end - pos - 3);
            riskArray.a.push_back(std::stod(val));
            pos = end + 4;
        }
    }
    return riskArray;
}

void parseSpanXmlBlock(const std::string& block, std::vector<SpanRecord>& recs) {
    std::string segment;

    if (block.find("<phyPf>") != std::string::npos)
        segment = "phypf";
    else if (block.find("<futPf>") != std::string::npos)
        segment = "futpf";
    else
        segment = "oofpf";


    // Common root-level tags
    int pfId = std::stoi(extractTag(block, "pfId"));
    std::string pfCode = extractTag(block, "pfCode");
    std::string currency = extractTag(block, "currency");
    double cvf = std::stod(extractTag(block, "cvf"));
    std::string valueMeth = extractTag(block, "valueMeth");
    std::string priceMeth = extractTag(block, "priceMeth");
    std::string setlMeth = extractTag(block, "setlMeth");

    SpanRecord rec;
    rec.segment = segment;
    rec.pfId = pfId;
    rec.pfCode = pfCode;
    rec.currency = currency;
    rec.cvf = cvf;
    rec.valueMeth = valueMeth;
    rec.priceMeth = priceMeth;
    rec.setlMeth = setlMeth;

    if (segment == "phypf") {
        std::smatch m;
        std::regex_search(block, m, std::regex("<phy>[\\s\\S]*?</phy>"));
        std::string phyBlock = m[0];

        rec.contractId = std::stoi(extractTag(phyBlock, "cId"));
        rec.expiry = extractTag(phyBlock, "pe");
        rec.volatility = std::stod(extractTag(phyBlock, "v"));
        rec.priceScan = std::stod(extractTag(phyBlock, "priceScan"));
        rec.volScan = std::stod(extractTag(phyBlock, "volScan"));
        rec.riskArray = extractRiskArray(phyBlock);

        recs.push_back(rec);
    }
    else if (rec.segment == "futpf") {
        std::regex rgx("<fut>([\\s\\S]*?)</fut>");
        auto futBegin = std::sregex_iterator(block.begin(), block.end(), rgx);
        auto futEnd = std::sregex_iterator();

        for (auto it = futBegin; it != futEnd; ++it) {
            std::string fut = (*it)[0];

            rec.contractId = std::stoi(extractTag(fut, "cId"));
            rec.expiry = extractTag(fut, "pe");
            rec.volatility = std::stod(extractTag(fut, "v"));
            rec.settleDate = extractTag(fut, "setlDate");

            //val two tags, choosing 2nd one
            std::string intrBlock = extractTag(fut, "intrRate");
            rec.intraRate = std::stod(extractTag(intrBlock, "val"));

            rec.priceScan = std::stod(extractTag(fut, "priceScan"));
            rec.volScan = std::stod(extractTag(fut, "volScan"));
            rec.riskArray = extractRiskArray(fut);

            recs.push_back(rec);
        }

    }
    else if (segment == "oofpf") {

        // Common root-level tags
        rec.svf = std::stod(extractTag(block, "svf"));

        std::regex seriesRgx("<series>([\\s\\S]*?)</series>");
        auto seriesBegin = std::sregex_iterator(block.begin(), block.end(), seriesRgx);
        auto seriesEnd = std::sregex_iterator();

        for (auto sit = seriesBegin; sit != seriesEnd; ++sit) {
            std::string series = (*sit)[0];

            rec.expiry = extractTag(series, "pe");
            rec.settleDate = extractTag(series, "setlDate");
            rec.volatility = std::stod(extractTag(series, "v"));
            rec.intraRate = std::stod(extractTag(series, "val")); // it will find interRate as 1st appear
            rec.priceScan = std::stod(extractTag(series, "priceScan"));
            rec.volScan = std::stod(extractTag(series, "volScan"));
            rec.contractId = std::stoi(extractTag(series, "cId"));

            std::regex optRgx("<opt>([\\s\\S]*?)</opt>");
            auto optBegin = std::sregex_iterator(series.begin(), series.end(), optRgx);
            auto optEnd = std::sregex_iterator();
            for (auto oit = optBegin; oit != optEnd; ++oit) {
                std::string opt = (*oit)[0];

                rec.optContractId = std::stoi(extractTag(opt, "cId"));
                rec.optionType = extractTag(opt, "o");
                rec.strikePrice = std::stod(extractTag(opt, "k"));
                rec.optionValue = std::stod(extractTag(opt, "val"));
                rec.riskArray = extractRiskArray(opt);

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
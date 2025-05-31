#pragma once
#ifndef SPAN_PARSER_H
#define SPAN_PARSER_H

#include <string>
#include <vector>
#include <windows.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>
#include <unordered_map>

// Risk array structure from <ra>
struct RiskArray {
    int r = 0;                  // <r>
    std::vector<double> a;     // <a> values
    double d = 0.0;            // <d>
};

// Main record for all segments (phypf, futpf, oofpf)
struct SpanRecord {
    std::string segment;       // phypf, futpf, oofpf

    // Common SPAN fields
    int pfId = 0;
    std::string pfCode;
    std::string currency;
    double cvf = 0.0;
    double svf = 0.0;          // Only for oofpf
    std::string valueMeth;
    std::string priceMeth;
    std::string setlMeth;

    // Contract fields
    int contractId = 0;
    std::string expiry;
    double volatility = 0.0;
    std::string settleDate;    // For futpf, oofpf
    double intraRate = 0.0;    // From <intrRate><val>

    // Scan risk values
    double priceScan = 0.0;
    double volScan = 0.0;

    // Option-specific (for oofpf <opt>)
    int optContractId = 0;  // <- for oofpf only: opt-level <cId>
    std::string optionType;    // C or P
    double strikePrice = 0.0;
    double optionValue = 0.0;

    RiskArray riskArray;
};

using ConfigSection = std::unordered_map<std::string, std::string>;
using ConfigMap = std::unordered_map<std::string, ConfigSection>;


// File Reading
bool readConnectionString(const std::string& filePath, std::wstring& connStr);
ConfigMap loadTagConfigIni(const std::string& path);

// Tag extractors and XML parsing
std::string extractTag(const std::string& block, const std::string& tag);
RiskArray extractRiskArray(const std::string& block, const ConfigMap& config);
void parseSpanXmlBlock(const std::string& block, std::vector<SpanRecord>& recs, const ConfigMap& config);
std::wstring joinRiskArray(RiskArray riskArray);

// DB functions
bool connectToMSSQL(SQLHENV& hEnv, SQLHDBC& hDbc, const std::wstring& connStr);
bool insertSpanRecords(SQLHDBC hDbc, const std::vector<SpanRecord>& records);
void handleError(SQLSMALLINT handleType, SQLHANDLE handle, const char* functionName, int paramNumber = 0);

double safeStod(const std::string& value, double defaultVal = 0.0);
int safeStoi(const std::string& value, int defaultVal = 0);

// printig & logging
void printSpanRecords(const SpanRecord& records);
void logConfigMap(const ConfigMap& config);



#endif // SPAN_PARSER_H

#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <fstream>
#include <string>
#include <ctime>
#include <mutex>

enum class LogLevel { INFO, WARNING, ERRORS };

class Logger {
public:
    Logger(const std::string& filename) {
        logfile.open(filename, std::ios::app);
    }

    ~Logger() {
        if (logfile.is_open())
            logfile.close();
    }

    void log(const std::string& message, LogLevel level = LogLevel::INFO) {
        std::lock_guard<std::mutex> lock(logMutex);
        std::string levelStr = levelToString(level);
        std::string timestamp = currentTime();

        std::string output = "[" + timestamp + "] [" + levelStr + "] " + message;
        if (logfile.is_open())
            logfile << output << std::endl; // log to file
    }

private:
    std::ofstream logfile;
    std::mutex logMutex;

    std::string levelToString(LogLevel level) {
        switch (level) {
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARNING:
            return "WARN";
        case LogLevel::ERRORS:
            return "ERROR";
        default:
            return "UNKNOWN";
        }
    }

    std::string currentTime() {
        std::time_t now = std::time(nullptr);
        char buf[20];
        std::tm local_tm;

        localtime_s(&local_tm, &now);  // Safer, thread-safe version
        strftime(buf, sizeof(buf), "%F %T", &local_tm);
        return buf;
    }
};

#endif // LOGGER_H
#pragma once

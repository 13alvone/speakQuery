#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <ctime>
#include <string>
#include <vector>
#include <iostream>
#include <optional>
#include <algorithm>

namespace py = pybind11;

// Attempts to parse a single date string using multiple formats.
// Returns 0 if parsing fails for all formats.
static long long parse_single_date(const std::string &date_str) {
    std::vector<std::string> formats = {
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
        "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d", // Date only
        "%Y-%m-%dT%H:%M:%S",
        "%B %d, %Y %H:%M:%S", "%d %B %Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p", "%m-%d-%Y %I:%M:%S %p",
        "%Y%m%d%H%M%S",
        "%Y-W%W-%w %H:%M:%S", "%Y-W%U-%w %H:%M:%S"
    };

    auto strip_fractional = [](const std::string &in) {
        size_t pos = in.find('.');
        if (pos != std::string::npos) {
            return in.substr(0, pos);
        }
        return in;
    };
    std::string candidate = strip_fractional(date_str);

    auto try_parse = [&](const std::string &fmt, const std::string &inp) -> std::optional<long long> {
        struct tm tm = {};
        char *res = strptime(inp.c_str(), fmt.c_str(), &tm);
        if (!res || *res != '\0') {
            return std::nullopt;
        }

        time_t t = timegm(&tm); // timegm converts tm to time_t as UTC
        return (long long)t;
    };

    for (auto &fmt : formats) {
        auto val = try_parse(fmt, candidate);
        if (val.has_value()) {
            return val.value();
        }
    }

    std::cerr << "Failed to parse: '" << date_str << "' using given formats.\n";
    return 0;
}

// Takes a list of date strings and returns a list of corresponding epoch timestamps.
static std::vector<long long> parse_dates_to_epoch(const std::vector<std::string> &dates) {
    std::vector<long long> result;
    result.reserve(dates.size());
    for (auto &d: dates) {
        result.push_back(parse_single_date(d));
    }
    return result;
}

PYBIND11_MODULE(cpp_datetime_parser, m) {
    m.doc() = "C++ translation of datetime_parser functionality";
    m.def("parse_dates_to_epoch", &parse_dates_to_epoch, "Parse a list of date strings into epoch timestamps");
}


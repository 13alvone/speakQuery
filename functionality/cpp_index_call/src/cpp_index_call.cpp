#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/pytypes.h>
#include <pybind11/numpy.h>
#include <pybind11/iostream.h>

#include <string>
#include <vector>
#include <deque>
#include <regex>
#include <stdexcept>
#include <iostream>
#include <optional>
#include <ctime>
#include <algorithm>
#include <sys/stat.h>
#include <glob.h>
#include <filesystem>
#include <unordered_set>

namespace py = pybind11;
using namespace std::filesystem;

namespace {
// Logging functions for consistent console output with flush
inline void log_info(const std::string &msg) { std::cerr << "[i] " << msg << "\n" << std::flush; }
inline void log_warning(const std::string &msg) { std::cerr << "[!] " << msg << "\n" << std::flush; }
inline void log_error(const std::string &msg) { std::cerr << "[x] " << msg << "\n" << std::flush; }

// Parse dates: attempts to convert a date string into epoch time
static long long parse_date_to_epoch_single(const std::string &date_str) {
    bool all_digits = std::all_of(date_str.begin(), date_str.end(), ::isdigit);
    if (all_digits && !date_str.empty()) {
        try { return std::stoll(date_str); } catch (...) {}
    }

    std::vector<std::string> formats = {
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
        "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S",
        "%B %d, %Y %H:%M:%S", "%d %B %Y %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p", "%m-%d-%Y %I:%M:%S %p",
        "%Y%m%d%H%M%S",
        "%Y-W%W-%w %H:%M:%S", "%Y-W%U-%w %H:%M:%S"
    };

    auto strip_fractional = [&](const std::string &in) {
        size_t pos = in.find('.');
        return (pos != std::string::npos) ? in.substr(0, pos) : in;
    };
    std::string candidate = strip_fractional(date_str);

    auto try_parse = [&](const std::string &fmt, const std::string &inp) -> std::optional<long long> {
        struct tm tm = {};
        char *res = strptime(inp.c_str(), fmt.c_str(), &tm);
        if (!res || *res != '\0') return std::nullopt;
        time_t t = timegm(&tm);
        return (long long)t;
    };

    for (auto &fmt : formats) {
        auto val = try_parse(fmt, candidate);
        if (val.has_value()) return val.value();
    }

    log_warning("Failed to parse date: '" + date_str + "'");
    return 0;
}

enum class TokenType { Identifier, StringLiteral, NumberLiteral, Operator, Parenthesis, Comma, Literal };

struct Token {
    TokenType type;
    std::string value;
};

enum class ASTNodeType { Comparison, LogicalOp, InClause, Identifier, Literal };

struct ASTNode {
    ASTNodeType type;
    std::string operator_;
    std::string identifier;
    std::vector<std::string> values;
    std::unique_ptr<ASTNode> left;
    std::unique_ptr<ASTNode> right;
    std::string literal_or_ident;
};

// Tokenize the provided tokens into Token objects
static std::vector<Token> tokenize_query_tokens(const std::vector<std::string> &tokens) {
    std::vector<Token> result;
    for (auto &token : tokens) {
        if (token == "(" || token == ")") {
            result.push_back({TokenType::Parenthesis, token});
        } else if (token == "=" || token == "!=" || token == "<" || token == ">" || token == "<=" || token == ">=" ||
                   token == "AND" || token == "OR" || token == "IN") {
            Token tk{TokenType::Operator, token};
            for (auto &c : tk.value) c = (char)toupper((unsigned char)c);
            result.push_back(tk);
        } else if (token == ",") {
            result.push_back({TokenType::Comma, token});
        } else if (token == "True" || token == "False") {
            // Boolean literal
            result.push_back({TokenType::Literal, token});
        } else {
            if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
                auto trimmed = token.substr(1, token.size() - 2);
                result.push_back({TokenType::StringLiteral, trimmed});
            } else {
                bool all_num = true;
                for (auto c : token) {
                    if (!isdigit((unsigned char)c)) { all_num = false; break; }
                }
                if (all_num && !token.empty()) {
                    result.push_back({TokenType::NumberLiteral, token});
                } else {
                    result.push_back({TokenType::Identifier, token});
                }
            }
        }
    }
    return result;
}

// Parser class to build an AST from tokens
class Parser {
public:
    Parser(const std::vector<Token> &toks) : tokens(toks.begin(), toks.end()) {}
    ASTNode parse_expression() { return parse_or(); }

private:
    std::deque<Token> tokens;

    ASTNode parse_or() {
        ASTNode node = parse_and();
        for (;;) {
            if (!tokens.empty() && tokens.front().type == TokenType::Operator && tokens.front().value == "OR") {
                tokens.pop_front();
                ASTNode right = parse_and();
                ASTNode new_node;
                new_node.type = ASTNodeType::LogicalOp;
                new_node.operator_ = "|";
                new_node.left = std::make_unique<ASTNode>(std::move(node));
                new_node.right = std::make_unique<ASTNode>(std::move(right));
                node = std::move(new_node);
            } else break;
        }
        return node;
    }

    ASTNode parse_and() {
        ASTNode node = parse_comparison();
        for (;;) {
            if (!tokens.empty() && tokens.front().type == TokenType::Operator && tokens.front().value == "AND") {
                tokens.pop_front();
                ASTNode right = parse_comparison();
                ASTNode new_node;
                new_node.type = ASTNodeType::LogicalOp;
                new_node.operator_ = "&";
                new_node.left = std::make_unique<ASTNode>(std::move(node));
                new_node.right = std::make_unique<ASTNode>(std::move(right));
                node = std::move(new_node);
            } else if (!tokens.empty() &&
                       (tokens.front().type == TokenType::Identifier ||
                        tokens.front().type == TokenType::StringLiteral ||
                        tokens.front().type == TokenType::NumberLiteral ||
                        (tokens.front().type == TokenType::Parenthesis && tokens.front().value == "("))) {
                // Implicit AND if next token starts a new condition
                ASTNode right = parse_comparison();
                ASTNode new_node;
                new_node.type = ASTNodeType::LogicalOp;
                new_node.operator_ = "&";
                new_node.left = std::make_unique<ASTNode>(std::move(node));
                new_node.right = std::make_unique<ASTNode>(std::move(right));
                node = std::move(new_node);
            } else break;
        }
        return node;
    }

    ASTNode parse_comparison() {
        if (!tokens.empty() && tokens.front().type == TokenType::Parenthesis && tokens.front().value == "(") {
            tokens.pop_front();
            ASTNode node = parse_expression();
            if (tokens.empty() || !(tokens.front().type == TokenType::Parenthesis && tokens.front().value == ")"))
                throw std::runtime_error("Expected ')'");
            tokens.pop_front();
            return node;
        }

        ASTNode left = parse_operand();
        if (!tokens.empty() && tokens.front().type == TokenType::Operator) {
            std::string op = tokens.front().value;
            tokens.pop_front();
            if (op == "IN") {
                if (left.type != ASTNodeType::Identifier)
                    throw std::runtime_error("Expected identifier before IN");
                if (tokens.empty() || tokens.front().type != TokenType::Parenthesis || tokens.front().value != "(")
                    throw std::runtime_error("Expected '(' after IN");
                tokens.pop_front();
                std::vector<std::string> values;
                for (;;) {
                    if (tokens.empty()) throw std::runtime_error("Unexpected end in IN clause");
                    if (tokens.front().type == TokenType::Parenthesis && tokens.front().value == ")") {
                        tokens.pop_front();
                        break;
                    } else if (tokens.front().type == TokenType::Comma) {
                        tokens.pop_front();
                        continue;
                    } else if (tokens.front().type == TokenType::StringLiteral) {
                        std::string s = "'" + tokens.front().value + "'";
                        tokens.pop_front();
                        values.push_back(s);
                    } else if (tokens.front().type == TokenType::NumberLiteral) {
                        std::string s = tokens.front().value;
                        tokens.pop_front();
                        values.push_back(s);
                    } else if (tokens.front().type == TokenType::Identifier) {
                        std::string s = "'" + tokens.front().value + "'";
                        tokens.pop_front();
                        values.push_back(s);
                    } else if (tokens.front().type == TokenType::Literal) {
                        std::string val = tokens.front().value;
                        if (val == "True" || val == "False") {
                            val = "'" + val + "'";
                        } else {
                            val = "'" + val + "'";
                        }
                        tokens.pop_front();
                        values.push_back(val);
                    } else {
                        throw std::runtime_error("Unexpected token in IN clause");
                    }
                }
                ASTNode in_node;
                in_node.type = ASTNodeType::InClause;
                in_node.identifier = left.literal_or_ident;
                in_node.values = values;
                return in_node;
            } else if (op == "=" || op == "!=" || op == "<" || op == ">" || op == "<=" || op == ">=") {
                ASTNode right = parse_operand();
                ASTNode cmp;
                cmp.type = ASTNodeType::Comparison;
                cmp.operator_ = op;
                cmp.left = std::make_unique<ASTNode>(std::move(left));
                cmp.right = std::make_unique<ASTNode>(std::move(right));
                return cmp;
            } else {
                throw std::runtime_error("Unknown operator: " + op);
            }
        }

        return left;
    }

    ASTNode parse_operand() {
        if (tokens.empty()) throw std::runtime_error("Unexpected end of input while parsing operand");
        Token tk = tokens.front();
        tokens.pop_front();
        if (tk.type == TokenType::Identifier) {
            std::regex re("^[a-zA-Z_][a-zA-Z0-9_]*$");
            if (!std::regex_match(tk.value, re))
                throw std::runtime_error("Invalid identifier: '" + tk.value + "'");
            ASTNode node;
            node.type = ASTNodeType::Identifier;
            node.literal_or_ident = tk.value;
            return node;
        } else if (tk.type == TokenType::StringLiteral) {
            ASTNode node;
            node.type = ASTNodeType::Literal;
            std::string s = tk.value;
            for (auto &ch : s) { if (ch == '\'') ch = '\\'; }
            node.literal_or_ident = "'" + s + "'";
            return node;
        } else if (tk.type == TokenType::NumberLiteral) {
            ASTNode node;
            node.type = ASTNodeType::Literal;
            node.literal_or_ident = tk.value;
            return node;
        } else if (tk.type == TokenType::Literal) {
            ASTNode node;
            node.type = ASTNodeType::Literal;
            if (tk.value == "True" || tk.value == "False") {
                node.literal_or_ident = tk.value;
            } else {
                node.literal_or_ident = "'" + tk.value + "'";
            }
            return node;
        }

        throw std::runtime_error("Unexpected token while parsing operand");
    }
};

// Convert the AST into a string-based query
static std::string ast_to_query(const ASTNode &ast) {
    switch (ast.type) {
        case ASTNodeType::Comparison: {
            std::string op;
            if (ast.operator_ == "=") op = "==";
            else if (ast.operator_ == "!=") op = "!=";
            else if (ast.operator_ == "<") op = "<";
            else if (ast.operator_ == ">") op = ">";
            else if (ast.operator_ == "<=") op = "<=";
            else if (ast.operator_ == ">=") op = ">=";
            else throw std::runtime_error("Unknown operator: " + ast.operator_);

            std::string left_str = ast_to_query(*ast.left);
            std::string right_str = ast_to_query(*ast.right);
            return "(" + left_str + " " + op + " " + right_str + ")";
        }
        case ASTNodeType::LogicalOp: {
            std::string op = (ast.operator_ == "|") ? "|" : "&";
            std::string left_str = ast_to_query(*ast.left);
            std::string right_str = ast_to_query(*ast.right);
            return "(" + left_str + " " + op + " " + right_str + ")";
        }
        case ASTNodeType::InClause: {
            std::string values_str;
            for (size_t i = 0; i < ast.values.size(); i++) {
                if (i > 0) values_str += ", ";
                values_str += ast.values[i];
            }
            return "(" + ast.identifier + " in [" + values_str + "])";
        }
        case ASTNodeType::Identifier:
            return ast.literal_or_ident;
        case ASTNodeType::Literal:
            if (ast.literal_or_ident == "True") return "True";
            if (ast.literal_or_ident == "False") return "False";
            return ast.literal_or_ident;
    }
    throw std::runtime_error("Unknown AST node type");
}

// Collect required identifiers from the AST; flag if epoch conversion is needed
static void collect_identifiers(const ASTNode &ast, std::vector<std::string> &columns, bool &need_epoch) {
    switch (ast.type) {
        case ASTNodeType::Comparison:
        case ASTNodeType::LogicalOp:
            if (ast.left) collect_identifiers(*ast.left, columns, need_epoch);
            if (ast.right) collect_identifiers(*ast.right, columns, need_epoch);
            break;
        case ASTNodeType::InClause:
            if (ast.identifier == "earliest" || ast.identifier == "latest") need_epoch = true;
            else if (std::find(columns.begin(), columns.end(), ast.identifier) == columns.end())
                columns.push_back(ast.identifier);
            break;
        case ASTNodeType::Identifier:
            if (ast.literal_or_ident == "earliest" || ast.literal_or_ident == "latest")
                need_epoch = true;
            else if (std::find(columns.begin(), columns.end(), ast.literal_or_ident) == columns.end())
                columns.push_back(ast.literal_or_ident);
            break;
        case ASTNodeType::Literal:
            // Do nothing for literals
            break;
    }
}

// Parse the AST from the given token vector
static std::optional<ASTNode> parse_ast(const std::vector<std::string> &tokens) {
    if (tokens.empty()) return std::nullopt;
    auto token_objects = tokenize_query_tokens(tokens);
    if (token_objects.empty()) return std::nullopt;
    try {
        Parser parser(token_objects);
        ASTNode ast = parser.parse_expression();
        return ast;
    } catch (...) {
        return std::nullopt;
    }
}

// Function to dynamically determine the project root directory.
// If PROJECT_ROOT is defined at compile-time, that path is used.
// Otherwise, falls back to the current working directory.
static std::filesystem::path get_project_root() {
#ifdef PROJECT_ROOT
    auto project_root = std::filesystem::path(PROJECT_ROOT);
    log_info("Using compile-time defined project root: " + project_root.string());
    return project_root;
#else
    auto project_root = std::filesystem::current_path();
    log_warning("Falling back to current_path() as project root: " + project_root.string());
    return project_root;
#endif
}

// Load and filter data from parquet files based on provided criteria
static py::object load_and_filter_data(
    const std::string &index_pattern,
    bool need_epoch,
    const std::optional<std::string> &pandas_query,
    const std::optional<long long> &earliest_epoch,
    const std::optional<long long> &latest_epoch,
    const path &indexes_dir,
    const std::vector<std::string> &filter_columns // all non-earliest/latest identifiers
) {
    py::object pandas = py::module_::import("pandas");
    py::object empty_df = pandas.attr("DataFrame")();

    auto adjust_pattern = [&](const std::string &p) -> std::string {
        std::string cpat = p;
        while (!cpat.empty() && (cpat.front() == '"' || cpat.front() == ',')) cpat.erase(cpat.begin());
        while (!cpat.empty() && (cpat.back() == '"' || cpat.back() == ',')) cpat.pop_back();
        while (!cpat.empty() && cpat.front() == '(') cpat.erase(cpat.begin());
        while (!cpat.empty() && cpat.back() == ')') cpat.pop_back();

        if (cpat.size() >= 3 && cpat.substr(cpat.size() - 3) == "/**") {
            std::string dir_pattern = cpat.substr(0, cpat.size() - 3);
            return indexes_dir.string() + "/" + dir_pattern + "/**/*.parquet";
        } else if (cpat.size() >= 2 && cpat.substr(cpat.size() - 2) == "/*") {
            std::string dir_pattern = cpat.substr(0, cpat.size() - 2);
            return indexes_dir.string() + "/" + dir_pattern + "/*.parquet";
        } else {
            auto possible_file = indexes_dir / cpat;
            struct stat stbuf;
            if (stat(possible_file.string().c_str(), &stbuf) == 0) {
                if (S_ISREG(stbuf.st_mode)) {
                    return possible_file.string();
                } else if (S_ISDIR(stbuf.st_mode)) {
                    return indexes_dir.string() + "/" + cpat + "/**/*.parquet";
                }
            }
            return indexes_dir.string() + "/" + cpat + "/**/*.parquet";
        }
    };

    std::string adjusted_pattern = adjust_pattern(index_pattern);
    log_info("Adjusted index pattern: " + adjusted_pattern);

    auto files = [&]() {
        std::vector<std::string> results;
        glob_t g;
        int r = glob(adjusted_pattern.c_str(), GLOB_TILDE, NULL, &g);
        if (r == 0) {
            for (size_t i = 0; i < g.gl_pathc; i++) {
                results.push_back(g.gl_pathv[i]);
            }
        } else {
            log_warning("glob() returned error code: " + std::to_string(r) + " for pattern: " + adjusted_pattern);
        }
        globfree(&g);
        return results;
    }();

    log_info("Found " + std::to_string(files.size()) + " candidate file(s) using pattern.");

    std::vector<py::object> dataframes;

    for (auto &path_str : files) {
        log_info("Evaluating candidate file: " + path_str);
        if (path_str.size() < indexes_dir.string().size()) {
            log_info("Skipping file " + path_str + " because its path appears shorter than indexes_dir.");
            continue;
        }
        auto p = std::filesystem::path(path_str);
        if (p.has_extension() && p.extension() == ".parquet") {
            struct stat stbuf;
            if (stat(path_str.c_str(), &stbuf) == 0 && S_ISREG(stbuf.st_mode)) {
                log_info("Found target parquet file: " + path_str);
                // Load all columns from the parquet file
                py::object df = pandas.attr("read_parquet")(path_str);

                auto rel_path = std::filesystem::relative(path_str, indexes_dir);
                df = df.attr("assign")(py::arg("_source_file") = py::str(rel_path.string()));

                py::object cols = df.attr("columns").attr("tolist")();
                auto col_list = cols.cast<std::vector<std::string>>();
                std::unordered_set<std::string> col_set(col_list.begin(), col_list.end());

                // Check if all filter columns exist
                bool all_cols_exist = true;
                for (auto &c : filter_columns) {
                    if (c == "earliest" || c == "latest") continue;
                    if (col_set.find(c) == col_set.end()) {
                        all_cols_exist = false;
                        log_info("File " + path_str + " missing required column: " + c);
                        break;
                    }
                }

                if (need_epoch) {
                    // Ensure timestamp column is loaded
                    bool has_timestamp = (col_set.find("timestamp") != col_set.end());
                    if (!has_timestamp) {
                        log_info("Skipping file " + path_str + " because it lacks 'timestamp' needed for earliest/latest.");
                        continue;
                    }
                }

                if (!all_cols_exist) {
                    log_info("Skipping file " + path_str + " because it lacks required columns.");
                    continue;
                }

                if (need_epoch) {
                    df = df.attr("assign")(py::arg("timestamp") = df["timestamp"].attr("astype")("str"));
                    size_t length = df.attr("shape").cast<py::tuple>()[0].cast<size_t>();
                    std::vector<long long> epochs(length, 0);
                    if (length > 0) {
                        py::object list_obj = df["timestamp"].attr("tolist")();
                        auto datetime_strings = list_obj.cast<std::vector<std::string>>();
                        for (size_t ii = 0; ii < datetime_strings.size(); ii++) {
                            epochs[ii] = parse_date_to_epoch_single(datetime_strings[ii]);
                        }
                    }
                    auto epoch_array = py::array_t<long long>(epochs.size(), epochs.data());
                    df = df.attr("assign")(py::arg("_epoch") = epoch_array);
                    col_set.insert("_epoch");
                }

                // Build final query
                std::string final_query = "True";
                if (earliest_epoch.has_value() || latest_epoch.has_value()) {
                    std::string epoch_cond = "True";
                    if (earliest_epoch.has_value()) {
                        epoch_cond = "(_epoch >= " + std::to_string(earliest_epoch.value()) + ")";
                    }
                    if (latest_epoch.has_value()) {
                        std::string lc = "(_epoch <= " + std::to_string(latest_epoch.value()) + ")";
                        if (epoch_cond == "True") epoch_cond = lc;
                        else epoch_cond = "(" + epoch_cond + " & " + lc + ")";
                    }
                    final_query = epoch_cond;
                }

                if (pandas_query.has_value() && !pandas_query->empty() && pandas_query.value() != "True") {
                    final_query = (final_query == "True") ? pandas_query.value() : "(" + final_query + " & (" + pandas_query.value() + "))";
                }

                if (final_query != "True") {
                    log_info("Applying filter to DataFrame from file: " + path_str);
                    log_info("Pandas query: " + final_query);
                    try {
                        df = df.attr("query")(final_query);
                    } catch (py::error_already_set &e) {
                        log_info("Filtering error: " + std::string(e.what()) + "; treating as no matches for file " + path_str);
                        df = pandas.attr("DataFrame")();
                    }
                }

                dataframes.push_back(df);
            } else {
                log_warning("File " + path_str + " is not a regular file.");
            }
        } else {
            log_info("Skipping candidate " + path_str + " because it does not have a .parquet extension.");
        }
    }

    if (dataframes.empty()) {
        log_info("No DataFrames loaded from any targeted parquet files; returning empty DataFrame.");
        return empty_df;
    } else if (dataframes.size() == 1) {
        log_info("One sub-query result loaded; returning it.");
        return dataframes[0];
    } else {
        log_info("Concatenating " + std::to_string(dataframes.size()) + " DataFrame(s) from index patterns.");
        py::list df_list;
        for (auto &df : dataframes) df_list.append(df);
        return pandas.attr("concat")(df_list);
    }
}

// Process index calls from the provided token vector
static py::object process_index_calls(const std::vector<std::string> &original_tokens) {
    // Use the determined project root instead of the current working directory.
    auto project_root = get_project_root();
    log_info("Project root directory: \"" + project_root.string() + "\"");
    auto indexes_dir = project_root / "indexes";
    log_info("Indexes directory: \"" + indexes_dir.string() + "\"");

    // Extract index= occurrences and separate filters
    std::vector<std::string> filter_tokens;
    std::vector<std::string> index_patterns;
    {
        for (size_t i = 0; i < original_tokens.size(); i++) {
            if (i + 2 < original_tokens.size() && original_tokens[i] == "index" && original_tokens[i + 1] == "=") {
                index_patterns.push_back(original_tokens[i + 2]);
                i += 2;
            } else {
                filter_tokens.push_back(original_tokens[i]);
            }
        }
        if (index_patterns.empty()) {
            index_patterns.push_back("\"system_logs/**\"");
        }
    }

    // Extract earliest/latest parameters
    std::optional<long long> earliest_epoch;
    std::optional<long long> latest_epoch;
    {
        std::vector<std::string> new_filters;
        for (size_t i = 0; i < filter_tokens.size(); i++) {
            if ((filter_tokens[i] == "earliest" || filter_tokens[i] == "latest") && i + 2 < filter_tokens.size() && filter_tokens[i + 1] == "=") {
                std::string val = filter_tokens[i + 2];
                if (val.size() >= 2 && val.front() == '"' && val.back() == '"') {
                    val = val.substr(1, val.size() - 2);
                }
                long long ep = parse_date_to_epoch_single(val);
                if (filter_tokens[i] == "earliest") earliest_epoch = ep;
                else latest_epoch = ep;
                i += 2;
            } else {
                new_filters.push_back(filter_tokens[i]);
            }
        }
        filter_tokens = new_filters;
    }

    bool need_epoch = false;
    std::optional<ASTNode> ast = parse_ast(filter_tokens);
    std::optional<std::string> pandas_query = std::nullopt;
    std::vector<std::string> required_columns;
    if (ast.has_value()) {
        collect_identifiers(ast.value(), required_columns, need_epoch);
        std::string q = ast_to_query(ast.value());
        if (q.empty()) q = "True";
        pandas_query = q;
    }

    if (earliest_epoch.has_value() || latest_epoch.has_value()) need_epoch = true;

    py::object pandas = py::module_::import("pandas");
    std::vector<py::object> results;

    std::vector<std::string> filter_cols_no_special;
    for (auto &c : required_columns) {
        if (c != "earliest" && c != "latest") filter_cols_no_special.push_back(c);
    }
    log_info("Using indexes directory: \"" + indexes_dir.string() + "\"");

    for (auto &ip : index_patterns) {
        log_info("Processing index pattern: " + ip);
        py::object df = load_and_filter_data(ip, need_epoch, pandas_query, earliest_epoch, latest_epoch, indexes_dir, filter_cols_no_special);
        results.push_back(df);
    }

    if (results.empty()) {
        log_info("No DataFrames from subqueries; returning empty DataFrame.");
        return pandas.attr("DataFrame")();
    } else if (results.size() == 1) {
        return results[0];
    } else {
        log_info("Concatenating " + std::to_string(results.size()) + " DataFrame(s) from index patterns.");
        py::list df_list;
        for (auto &df : results) df_list.append(df);
        return pandas.attr("concat")(df_list);
    }
}

} // anonymous namespace

PYBIND11_MODULE(cpp_index_call, m) {
    log_info("Initializing cpp_index_call module.");
    m.doc() = "C++ module with improved implicit AND handling for parenthetical conditions.";
    m.def("process_index_calls", &process_index_calls, "Process index calls");
}


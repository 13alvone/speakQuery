use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use glob::glob;
use std::collections::VecDeque;
use regex::Regex;

#[derive(Debug, Clone)]
enum Token {
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(String),
    Operator(String),
    Parenthesis(String),
    Comma,
}

#[derive(Debug)]
enum ASTNode {
    Comparison {
        left: Box<ASTNode>,
        operator: String,
        right: Box<ASTNode>,
    },
    LogicalOp {
        operator: String,
        left: Box<ASTNode>,
        right: Box<ASTNode>,
    },
    InClause {
        identifier: String,
        values: Vec<String>,
    },
    Identifier(String),
    Literal(String),
}

fn tokenize(tokens: Vec<String>) -> Vec<Token> {
    let mut result = Vec::new();
    for token in tokens {
        match token.as_str() {
            "(" | ")" => result.push(Token::Parenthesis(token)),
            "=" | "!=" | "<" | ">" | "<=" | ">=" | "AND" | "OR" | "IN" => {
                result.push(Token::Operator(token.to_uppercase()))
            }
            "," => result.push(Token::Comma),
            _ => {
                if token.starts_with('"') && token.ends_with('"') {
                    result.push(Token::StringLiteral(
                        token.trim_matches('"').to_string(),
                    ));
                } else if token.chars().all(|c| c.is_numeric()) {
                    result.push(Token::NumberLiteral(token));
                } else {
                    result.push(Token::Identifier(token));
                }
            }
        }
    }
    result
}

struct Parser {
    tokens: VecDeque<Token>,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens: tokens.into(),
        }
    }

    fn parse_expression(&mut self) -> Result<ASTNode, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<ASTNode, String> {
        let mut node = self.parse_and()?;
        while let Some(Token::Operator(op)) = self.tokens.front() {
            if op == "OR" {
                self.tokens.pop_front();
                let right = self.parse_and()?;
                node = ASTNode::LogicalOp {
                    operator: "or".to_string(),
                    left: Box::new(node),
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(node)
    }

    fn parse_and(&mut self) -> Result<ASTNode, String> {
        let mut node = self.parse_comparison()?;
        loop {
            match self.tokens.front() {
                Some(Token::Operator(op)) if op == "AND" => {
                    self.tokens.pop_front();
                    let right = self.parse_comparison()?;
                    node = ASTNode::LogicalOp {
                        operator: "and".to_string(),
                        left: Box::new(node),
                        right: Box::new(right),
                    };
                }
                Some(Token::Identifier(_)) | Some(Token::StringLiteral(_)) | Some(Token::NumberLiteral(_)) => {
                    // Implicit AND
                    let right = self.parse_comparison()?;
                    node = ASTNode::LogicalOp {
                        operator: "and".to_string(),
                        left: Box::new(node),
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(node)
    }

    fn parse_comparison(&mut self) -> Result<ASTNode, String> {
        if let Some(Token::Parenthesis(p)) = self.tokens.front() {
            if p == "(" {
                self.tokens.pop_front();
                let node = self.parse_expression()?;
                if let Some(Token::Parenthesis(p2)) = self.tokens.pop_front() {
                    if p2 != ")" {
                        return Err("Expected ')'".to_string());
                    }
                } else {
                    return Err("Expected ')'".to_string());
                }
                return Ok(node);
            }
        }

        let left = self.parse_operand()?;

        if let Some(Token::Operator(op)) = self.tokens.front().cloned() {
            if ["=", "!=", "<", ">", "<=", ">=", "IN"].contains(&op.as_str()) {
                self.tokens.pop_front();
                if op == "IN" {
                    // Handle IN clause
                    if let ASTNode::Identifier(ident) = left {
                        if let Some(Token::Parenthesis(p)) = self.tokens.pop_front() {
                            if p != "(" {
                                return Err("Expected '(' after IN".to_string());
                            }
                        } else {
                            return Err("Expected '(' after IN".to_string());
                        }

                        let mut values = Vec::new();
                        loop {
                            match self.tokens.pop_front() {
                                Some(Token::StringLiteral(s)) => values.push(format!("'{}'", s.replace("'", "\\'"))),
                                Some(Token::NumberLiteral(n)) => values.push(n),
                                Some(Token::Comma) => continue,
                                Some(Token::Parenthesis(p)) if p == ")" => break,
                                Some(t) => return Err(format!("Unexpected token in IN clause: {:?}", t)),
                                None => return Err("Unexpected end of input in IN clause".to_string()),
                            }
                        }
                        return Ok(ASTNode::InClause {
                            identifier: ident,
                            values,
                        });
                    } else {
                        return Err("Expected identifier before IN".to_string());
                    }
                } else {
                    let right = self.parse_operand()?;
                    return Ok(ASTNode::Comparison {
                        left: Box::new(left),
                        operator: op,
                        right: Box::new(right),
                    });
                }
            }
        }

        Ok(left)
    }

    fn parse_operand(&mut self) -> Result<ASTNode, String> {
        if let Some(token) = self.tokens.pop_front() {
            match token {
                Token::Identifier(ident) => {
                    // Validate identifier
                    let re = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
                    if !re.is_match(&ident) {
                        return Err(format!("Invalid identifier: '{}'", ident));
                    }
                    Ok(ASTNode::Identifier(ident))
                }
                Token::StringLiteral(s) => Ok(ASTNode::Literal(format!("'{}'", s.replace("'", "\\'")))),
                Token::NumberLiteral(n) => Ok(ASTNode::Literal(n)),
                _ => Err(format!("Unexpected token: {:?}", token)),
            }
        } else {
            Err("Unexpected end of input while parsing operand".to_string())
        }
    }
}

fn ast_to_query(ast: &ASTNode, timestamp_col: &str) -> Result<String, String> {
    match ast {
        ASTNode::Comparison { left, operator, right } => {
            let left_str = ast_to_query(left, timestamp_col)?;
            let right_str = ast_to_query(right, timestamp_col)?;
            let op = match operator.as_str() {
                "=" => "==",
                "!=" => "!=",
                "<" => "<",
                ">" => ">",
                "<=" => "<=",
                ">=" => ">=",
                _ => return Err(format!("Unknown operator: {}", operator)),
            };
            // Special handling for 'earliest' and 'latest'
            if let ASTNode::Identifier(ident) = &**left {
                if ident == "earliest" {
                    return Ok(format!("({} >= {})", timestamp_col, right_str));
                } else if ident == "latest" {
                    return Ok(format!("({} <= {})", timestamp_col, right_str));
                }
            }
            Ok(format!("({} {} {})", left_str, op, right_str))
        }
        ASTNode::LogicalOp { operator, left, right } => {
            let left_str = ast_to_query(left, timestamp_col)?;
            let right_str = ast_to_query(right, timestamp_col)?;
            Ok(format!("({} {} {})", left_str, operator, right_str))
        }
        ASTNode::InClause { identifier, values } => {
            let values_str = values.join(", ");
            Ok(format!("({} in [{}])", identifier, values_str))
        }
        ASTNode::Identifier(ident) => {
            // Validate identifier
            let re = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
            if !re.is_match(ident) {
                return Err(format!("Invalid identifier: {}", ident));
            }
            Ok(ident.clone())
        }
        ASTNode::Literal(value) => Ok(value.clone()),
    }
}

fn collect_identifiers(ast: &ASTNode, columns: &mut Vec<String>) {
    match ast {
        ASTNode::Comparison { left, right, .. } => {
            if let ASTNode::Identifier(ref ident) = **left {
                if ident != "earliest" && ident != "latest" {
                    if !columns.contains(ident) {
                        columns.push(ident.clone());
                    }
                } else {
                    // Ensure 'timestamp' is included if 'earliest' or 'latest' are used
                    if !columns.contains(&"timestamp".to_string()) {
                        columns.push("timestamp".to_string());
                    }
                }
            }
            collect_identifiers(right, columns);
        }
        ASTNode::LogicalOp { left, right, .. } => {
            collect_identifiers(left, columns);
            collect_identifiers(right, columns);
        }
        ASTNode::InClause { identifier, .. } => {
            if !columns.contains(identifier) {
                columns.push(identifier.clone());
            }
        }
        _ => {}
    }
}

fn get_required_columns(ast: &Option<ASTNode>) -> Vec<String> {
    let mut columns = Vec::new();
    if let Some(ref ast_node) = ast {
        collect_identifiers(ast_node, &mut columns);
    }
    columns
}

#[pyfunction]
fn process_index_calls(py: Python, tokens: Vec<String>) -> PyResult<PyObject> {
    // Collect index patterns and other filters from the tokens
    let mut index_patterns = Vec::new();
    let mut filter_tokens = Vec::new();
    let mut i = 0;

    println!("Tokens received: {:?}", tokens);

    while i < tokens.len() {
        if tokens[i] == "index" {
            if i + 2 < tokens.len() && tokens[i + 1] == "=" {
                let pattern_token = &tokens[i + 2];
                let pattern = clean_pattern_token(pattern_token);
                println!("Extracted index pattern: {}", pattern);
                index_patterns.push(pattern);
                i += 3;
            } else {
                i += 1;
            }
        } else {
            filter_tokens.push(tokens[i].clone());
            i += 1;
        }
    }

    // Get the absolute path to the project root directory
    let project_root = std::env::current_dir().expect("Failed to get current directory");
    println!("Project root directory: {:?}", project_root);

    // Construct the absolute path to the 'indexes' directory
    let indexes_dir = project_root.join("indexes");
    println!("Indexes directory: {:?}", indexes_dir);

    // Parse the filter tokens into an AST
    let token_objects = tokenize(filter_tokens);
    let mut parser = Parser::new(token_objects);
    let ast = match parser.parse_expression() {
        Ok(ast) => Some(ast),
        Err(e) => {
            println!("Parsing error: {}", e);
            None
        }
    };

    println!("Parsed AST: {:?}", ast);

    // Convert AST to pandas query string
    let timestamp_col = "timestamp"; // Adjust if your timestamp column has a different name
    let pandas_query = if let Some(ref ast_node) = ast {
        match ast_to_query(ast_node, timestamp_col) {
            Ok(query_str) => Some(query_str),
            Err(e) => {
                println!("Error generating pandas query: {}", e);
                None
            }
        }
    } else {
        None
    };

    println!("Pandas query string: {:?}", pandas_query);

    // Get required columns
    let required_columns = get_required_columns(&ast);
    println!("Required columns for filtering: {:?}", required_columns);

    // Initialize a list to hold DataFrames
    let mut dataframes = Vec::new();

for pattern in index_patterns {
    // Sanitize the pattern to prevent directory traversal
    if pattern.contains("..") || pattern.starts_with('/') || pattern.contains('\\') {
        println!("Invalid pattern detected (potential directory traversal): {}", pattern);
        continue;
    }

    // Construct the full glob pattern
    let full_pattern = indexes_dir.join(&pattern);

    // Adjust the pattern to include recursive search for parquet files
    let adjusted_pattern = if pattern.ends_with("/*") {
        // If the pattern ends with '/*', match files directly under this directory
        full_pattern.join("*.parquet")
    } else if full_pattern.is_dir() || pattern.ends_with("/") {
        // If it's a directory or ends with '/', search recursively
        full_pattern.join("**").join("*.parquet")
    } else if full_pattern.is_file() {
        // If it's a specific file, use it directly
        full_pattern
    } else {
        // For other patterns, append '**/*.parquet' to search recursively
        full_pattern.join("**").join("*.parquet")
    };

    let pattern_str = adjusted_pattern.to_str().unwrap();
    println!("Searching for files with pattern: {}", pattern_str);

        // Use glob to find files matching the pattern
        let glob_results = glob(pattern_str);

        if let Err(e) = &glob_results {
            println!("Failed to read glob pattern '{}': {:?}", pattern_str, e);
            continue;
        }

        let mut files_found = false;

        for entry in glob_results.unwrap() {
            match entry {
                Ok(path) => {
                    // Ensure that the file is within the 'indexes' directory
                    if !path.starts_with(&indexes_dir) {
                        println!("Skipping file outside of indexes directory: {:?}", path);
                        continue;
                    }

                    // Check if the path is a file and has a .parquet extension
                    if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                        let path_str = path.to_str().unwrap();
                        println!("Found file: {}", path_str);

                        // Load the Parquet file into a Pandas DataFrame
                        let pandas = py.import("pandas")?;
                        let kwargs = PyDict::new(py);

                        // Specify columns to load if required columns are known
                        if !required_columns.is_empty() {
                            kwargs.set_item("columns", &required_columns)?;
                        }

                        let mut df = pandas.call_method("read_parquet", (path_str,), Some(kwargs))?;

                        // Apply the pandas query if it exists
                        if let Some(ref query_str) = pandas_query {
                            println!("Applying filter to DataFrame");
                            df = df.call_method1("query", (query_str,))?;
                        }

                        dataframes.push(df);
                        files_found = true;
                    }
                }
                Err(e) => {
                    println!("Glob error: {:?}", e);
                }
            }
        }

        if !files_found {
            println!("No files found for pattern: {}", pattern_str);
        }
    }

    // Merge all DataFrames into a single DataFrame
    if dataframes.is_empty() {
        println!("No DataFrames loaded; returning empty DataFrame.");
        let pandas = py.import("pandas")?;
        let empty_df = pandas.call_method0("DataFrame")?;
        Ok(empty_df.to_object(py))
    } else if dataframes.len() == 1 {
        println!("One DataFrame loaded; returning it.");
        Ok(dataframes[0].to_object(py))
    } else {
        println!("Multiple DataFrames loaded; concatenating them.");
        let dataframes_pylist = PyList::new(py, &dataframes);
        let pandas = py.import("pandas")?;
        let merged_df = pandas.getattr("concat")?.call1((dataframes_pylist,))?;
        Ok(merged_df.to_object(py))
    }
}

fn clean_pattern_token(token: &str) -> String {
    token
        .trim_matches('"')
        .trim_matches(',')
        .trim_start_matches('(')
        .trim_end_matches(')')
        .to_string()
}

#[pymodule]
fn r_index_call(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process_index_calls, m)?)?;
    Ok(())
}

from antlr4 import *
from lexers.speakQueryLexer import speakQueryLexer
from lexers.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener

# Your query string
query = '''
(
    earliest="2024-01-01" latest="2024-12-31"
    (
        (status=="error" AND errorCode IN (500, 503, 504))
        OR
        (status=="warning" AND warningCode=300)
    )
    AND (userRole=="admin" OR userRole=="superuser")
    index="system_logs/system4.parquet"
)
OR
(
    earliest="2023-01-01" latest="2023-12-31"
    (action=="login" AND success==false)
    AND (attempts >= 3)
    index="security_logs/system4.parquet"
)
| eval test="This is a test query."
'''

# Create an input stream
input_stream = InputStream(query.strip(' ').strip('\n').strip(' ').strip('\n'))

# Create a lexer and parser
lexer = speakQueryLexer(input_stream)
stream = CommonTokenStream(lexer)
parser = speakQueryParser(stream)

# Parse the query
tree = parser.query()

# Create the listener and walk the tree
listener = speakQueryListener(query)
walker = ParseTreeWalker()
walker.walk(listener, tree)

# Process filter blocks to load data
listener.process_filter_blocks()

# Access the resulting DataFrame
result_df = listener.main_df

print("Results:")
# print(result_df.head())

print(result_df.head(10).to_string(index=False))

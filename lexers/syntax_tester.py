#!/usr/bin/env python3

import antlr4
import logging
from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

speak_query = '''
index="output_parquets/test1.parquet" userRole="admin" errorCode=404
'''

speak_query9 = '''
(index="system_logs/error_tracking/*" error_code=403) OR 
(index="system_logs/successes/*" earliest="1704067200" latest="1704067200")
'''

speak_query8 = '''
earliest="2024-01-01" latest="2024-10-11" 
x=4 y=5 OR z = 6 
index="output_parquets/test1.parquet"
'''

speak_query7 = '''
(userRole="admin" OR userRole="superuser")
index="system_logs/*"
'''

speak_query7 = '''
(index="system_logs/error_tracking/*" error_code=12)
OR
(index="system_logs/successes/*" earliest="2024-01-01" latest="2024-10-11")
| eval alert_msg="This is an example of complex logic, and the index call is everything before the first pipe."
'''

speak_query6 = '''
(status="error" OR status="critical") AND errorCode IN (403, 404)
index="system_logs/error_tracking/*"
'''

speak_query4 = '''
(earliest="2024-01-01" latest="2024-10-11"
( x=4 AND y<5 ) OR z=6
index="output_parquets/test0.system4.system4.parquet,output_parquets/test1.system4.system4.parquet") OR 
(earliest="2023-01-01" latest="2024-01-01"
x >= 84
index="output_parquets/*")
'''

speak_query3 = '''
earliest="2024-01-01" latest="2024-10-11"
( x=4 AND y=5 ) OR z=6
index="output_parquets/test0.system4.system4.parquet,output_parquets/test1.system4.system4.parquet"
'''

speak_query2 = '''
earliest="2024-01-01" latest="2024-10-11"
(( x=4 AND y=5 ) OR z=6) AND a>=5 OR test IN ("something", "something_else", "test")
index="output_parquets/test0.system4.system4.parquet,output_parquets/test1.system4.system4.parquet"
'''


def main():
    logging.info("Starting the parsing process.")
    cleaned_query = speak_query.strip('\n').strip(' ').strip('\n').strip(' ')
    input_stream = antlr4.InputStream(cleaned_query)  # Replace 'input_text' with your test query
    lexer = speakQueryLexer(input_stream)  # Set up the lexer and parser
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()  # Parse the input to obtain the parse tree
    listener = speakQueryListener(cleaned_query)  # Create an instance of your custom listener
    walker = antlr4.ParseTreeWalker()  # Walk the parse tree using the custom listener
    walker.walk(listener, tree)


if __name__ == "__main__":
    main()

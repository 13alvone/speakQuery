from antlr4 import *
from speakQueryLexer import speakQueryLexer
from speakQueryParser import speakQueryParser
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Read your test query into a string
with open('current_query.test', 'r') as file:
    query = file.read()

# Create a stream from the input string
input_stream = InputStream(query)

# Lexing
lexer = speakQueryLexer(input_stream)
stream = CommonTokenStream(lexer)

# Parsing
parser = speakQueryParser(stream)

# You can now use parser to parse the input according to your grammar rules
# For example, if you have a rule called `speakQuery`, you would invoke it like this:
tree = parser.speakQuery()

# To print the parse tree
logging.info(tree.toStringTree(recog=parser))

# Generated from logicalClause.g4 by ANTLR 4.13.1
# encoding: utf-8
from antlr4 import *
from io import StringIO
import sys
if sys.version_info[1] > 5:
	from typing import TextIO
else:
	from typing.io import TextIO

def serializedATN():
    return [
        4,1,23,135,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
        6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,2,12,7,12,2,13,7,13,
        2,14,7,14,1,0,1,0,3,0,33,8,0,1,1,1,1,1,1,1,1,3,1,39,8,1,5,1,41,8,
        1,10,1,12,1,44,9,1,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,3,2,54,8,2,1,
        3,1,3,1,3,1,3,1,3,1,3,1,3,1,3,3,3,64,8,3,1,4,1,4,1,4,3,4,69,8,4,
        1,5,1,5,1,5,1,5,3,5,75,8,5,1,6,1,6,1,6,1,6,4,6,81,8,6,11,6,12,6,
        82,1,6,1,6,1,7,1,7,1,7,1,7,4,7,91,8,7,11,7,12,7,92,1,7,1,7,1,8,1,
        8,1,8,1,8,4,8,101,8,8,11,8,12,8,102,1,8,1,8,1,9,1,9,1,9,1,9,3,9,
        111,8,9,1,9,1,9,1,9,1,9,3,9,117,8,9,4,9,119,8,9,11,9,12,9,120,1,
        9,1,9,1,10,1,10,1,11,1,11,1,12,1,12,1,13,1,13,1,14,1,14,1,14,0,0,
        15,0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,0,3,1,0,2,3,1,0,6,12,
        2,0,18,18,22,22,137,0,32,1,0,0,0,2,34,1,0,0,0,4,53,1,0,0,0,6,63,
        1,0,0,0,8,68,1,0,0,0,10,74,1,0,0,0,12,76,1,0,0,0,14,86,1,0,0,0,16,
        96,1,0,0,0,18,106,1,0,0,0,20,124,1,0,0,0,22,126,1,0,0,0,24,128,1,
        0,0,0,26,130,1,0,0,0,28,132,1,0,0,0,30,33,3,2,1,0,31,33,3,4,2,0,
        32,30,1,0,0,0,32,31,1,0,0,0,33,1,1,0,0,0,34,42,3,4,2,0,35,38,7,0,
        0,0,36,39,3,4,2,0,37,39,3,6,3,0,38,36,1,0,0,0,38,37,1,0,0,0,39,41,
        1,0,0,0,40,35,1,0,0,0,41,44,1,0,0,0,42,40,1,0,0,0,42,43,1,0,0,0,
        43,3,1,0,0,0,44,42,1,0,0,0,45,46,5,15,0,0,46,47,3,4,2,0,47,48,5,
        16,0,0,48,54,1,0,0,0,49,50,3,6,3,0,50,51,7,0,0,0,51,52,3,6,3,0,52,
        54,1,0,0,0,53,45,1,0,0,0,53,49,1,0,0,0,54,5,1,0,0,0,55,56,3,28,14,
        0,56,57,3,24,12,0,57,58,3,8,4,0,58,64,1,0,0,0,59,60,3,28,14,0,60,
        61,3,26,13,0,61,62,3,10,5,0,62,64,1,0,0,0,63,55,1,0,0,0,63,59,1,
        0,0,0,64,7,1,0,0,0,65,69,3,28,14,0,66,69,3,20,10,0,67,69,3,22,11,
        0,68,65,1,0,0,0,68,66,1,0,0,0,68,67,1,0,0,0,69,9,1,0,0,0,70,75,3,
        16,8,0,71,75,3,12,6,0,72,75,3,14,7,0,73,75,3,18,9,0,74,70,1,0,0,
        0,74,71,1,0,0,0,74,72,1,0,0,0,74,73,1,0,0,0,75,11,1,0,0,0,76,77,
        5,15,0,0,77,80,3,20,10,0,78,79,5,13,0,0,79,81,3,20,10,0,80,78,1,
        0,0,0,81,82,1,0,0,0,82,80,1,0,0,0,82,83,1,0,0,0,83,84,1,0,0,0,84,
        85,5,16,0,0,85,13,1,0,0,0,86,87,5,15,0,0,87,90,3,22,11,0,88,89,5,
        13,0,0,89,91,3,22,11,0,90,88,1,0,0,0,91,92,1,0,0,0,92,90,1,0,0,0,
        92,93,1,0,0,0,93,94,1,0,0,0,94,95,5,16,0,0,95,15,1,0,0,0,96,97,5,
        15,0,0,97,100,3,28,14,0,98,99,5,13,0,0,99,101,3,28,14,0,100,98,1,
        0,0,0,101,102,1,0,0,0,102,100,1,0,0,0,102,103,1,0,0,0,103,104,1,
        0,0,0,104,105,5,16,0,0,105,17,1,0,0,0,106,110,5,15,0,0,107,111,3,
        20,10,0,108,111,3,22,11,0,109,111,3,28,14,0,110,107,1,0,0,0,110,
        108,1,0,0,0,110,109,1,0,0,0,111,118,1,0,0,0,112,116,5,13,0,0,113,
        117,3,20,10,0,114,117,3,22,11,0,115,117,3,28,14,0,116,113,1,0,0,
        0,116,114,1,0,0,0,116,115,1,0,0,0,117,119,1,0,0,0,118,112,1,0,0,
        0,119,120,1,0,0,0,120,118,1,0,0,0,120,121,1,0,0,0,121,122,1,0,0,
        0,122,123,5,16,0,0,123,19,1,0,0,0,124,125,5,19,0,0,125,21,1,0,0,
        0,126,127,5,17,0,0,127,23,1,0,0,0,128,129,7,1,0,0,129,25,1,0,0,0,
        130,131,5,5,0,0,131,27,1,0,0,0,132,133,7,2,0,0,133,29,1,0,0,0,13,
        32,38,42,53,63,68,74,82,92,102,110,116,120
    ]

class logicalClauseParser ( Parser ):

    grammarFileName = "logicalClause.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "<INVALID>", 
                     "<INVALID>", "<INVALID>", "'=='", "'!='", "'<'", "'>'", 
                     "'<='", "'>='", "'~='", "','", "';'", "'('", "')'" ]

    symbolicNames = [ "<INVALID>", "WHERE", "AND", "OR", "NOT", "IN", "EQ", 
                      "NOTEQ", "LT", "GT", "LTEQ", "GTEQ", "CONTAINS", "COMMA", 
                      "SEMI", "LPAREN", "RPAREN", "NUMBER", "SINGLE_QUOTED_STRING", 
                      "DOUBLE_QUOTED_STRING", "NEWLINE", "COMMENT", "ID", 
                      "WS" ]

    RULE_logicalClause = 0
    RULE_multiLogicalExpression = 1
    RULE_singleLogicalExpression = 2
    RULE_singleComparison = 3
    RULE_comparisonSingleValue = 4
    RULE_comparisonMultiValue = 5
    RULE_mvString = 6
    RULE_mvNumber = 7
    RULE_mvVariable = 8
    RULE_mvMixed = 9
    RULE_stringValue = 10
    RULE_numericValue = 11
    RULE_comparisonOperand = 12
    RULE_inOperand = 13
    RULE_variableName = 14

    ruleNames =  [ "logicalClause", "multiLogicalExpression", "singleLogicalExpression", 
                   "singleComparison", "comparisonSingleValue", "comparisonMultiValue", 
                   "mvString", "mvNumber", "mvVariable", "mvMixed", "stringValue", 
                   "numericValue", "comparisonOperand", "inOperand", "variableName" ]

    EOF = Token.EOF
    WHERE=1
    AND=2
    OR=3
    NOT=4
    IN=5
    EQ=6
    NOTEQ=7
    LT=8
    GT=9
    LTEQ=10
    GTEQ=11
    CONTAINS=12
    COMMA=13
    SEMI=14
    LPAREN=15
    RPAREN=16
    NUMBER=17
    SINGLE_QUOTED_STRING=18
    DOUBLE_QUOTED_STRING=19
    NEWLINE=20
    COMMENT=21
    ID=22
    WS=23

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.13.1")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None




    class LogicalClauseContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def multiLogicalExpression(self):
            return self.getTypedRuleContext(logicalClauseParser.MultiLogicalExpressionContext,0)


        def singleLogicalExpression(self):
            return self.getTypedRuleContext(logicalClauseParser.SingleLogicalExpressionContext,0)


        def getRuleIndex(self):
            return logicalClauseParser.RULE_logicalClause

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterLogicalClause" ):
                listener.enterLogicalClause(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitLogicalClause" ):
                listener.exitLogicalClause(self)




    def logicalClause(self):

        localctx = logicalClauseParser.LogicalClauseContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_logicalClause)
        try:
            self.state = 32
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,0,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 30
                self.multiLogicalExpression()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 31
                self.singleLogicalExpression()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class MultiLogicalExpressionContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def singleLogicalExpression(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.SingleLogicalExpressionContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.SingleLogicalExpressionContext,i)


        def AND(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.AND)
            else:
                return self.getToken(logicalClauseParser.AND, i)

        def OR(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.OR)
            else:
                return self.getToken(logicalClauseParser.OR, i)

        def singleComparison(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.SingleComparisonContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.SingleComparisonContext,i)


        def getRuleIndex(self):
            return logicalClauseParser.RULE_multiLogicalExpression

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMultiLogicalExpression" ):
                listener.enterMultiLogicalExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMultiLogicalExpression" ):
                listener.exitMultiLogicalExpression(self)




    def multiLogicalExpression(self):

        localctx = logicalClauseParser.MultiLogicalExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 2, self.RULE_multiLogicalExpression)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 34
            self.singleLogicalExpression()
            self.state = 42
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while _la==2 or _la==3:
                self.state = 35
                _la = self._input.LA(1)
                if not(_la==2 or _la==3):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 38
                self._errHandler.sync(self)
                la_ = self._interp.adaptivePredict(self._input,1,self._ctx)
                if la_ == 1:
                    self.state = 36
                    self.singleLogicalExpression()
                    pass

                elif la_ == 2:
                    self.state = 37
                    self.singleComparison()
                    pass


                self.state = 44
                self._errHandler.sync(self)
                _la = self._input.LA(1)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SingleLogicalExpressionContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(logicalClauseParser.LPAREN, 0)

        def singleLogicalExpression(self):
            return self.getTypedRuleContext(logicalClauseParser.SingleLogicalExpressionContext,0)


        def RPAREN(self):
            return self.getToken(logicalClauseParser.RPAREN, 0)

        def singleComparison(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.SingleComparisonContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.SingleComparisonContext,i)


        def AND(self):
            return self.getToken(logicalClauseParser.AND, 0)

        def OR(self):
            return self.getToken(logicalClauseParser.OR, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_singleLogicalExpression

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleLogicalExpression" ):
                listener.enterSingleLogicalExpression(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleLogicalExpression" ):
                listener.exitSingleLogicalExpression(self)




    def singleLogicalExpression(self):

        localctx = logicalClauseParser.SingleLogicalExpressionContext(self, self._ctx, self.state)
        self.enterRule(localctx, 4, self.RULE_singleLogicalExpression)
        self._la = 0 # Token type
        try:
            self.state = 53
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [15]:
                self.enterOuterAlt(localctx, 1)
                self.state = 45
                self.match(logicalClauseParser.LPAREN)
                self.state = 46
                self.singleLogicalExpression()
                self.state = 47
                self.match(logicalClauseParser.RPAREN)
                pass
            elif token in [18, 22]:
                self.enterOuterAlt(localctx, 2)
                self.state = 49
                self.singleComparison()
                self.state = 50
                _la = self._input.LA(1)
                if not(_la==2 or _la==3):
                    self._errHandler.recoverInline(self)
                else:
                    self._errHandler.reportMatch(self)
                    self.consume()
                self.state = 51
                self.singleComparison()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class SingleComparisonContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def variableName(self):
            return self.getTypedRuleContext(logicalClauseParser.VariableNameContext,0)


        def comparisonOperand(self):
            return self.getTypedRuleContext(logicalClauseParser.ComparisonOperandContext,0)


        def comparisonSingleValue(self):
            return self.getTypedRuleContext(logicalClauseParser.ComparisonSingleValueContext,0)


        def inOperand(self):
            return self.getTypedRuleContext(logicalClauseParser.InOperandContext,0)


        def comparisonMultiValue(self):
            return self.getTypedRuleContext(logicalClauseParser.ComparisonMultiValueContext,0)


        def getRuleIndex(self):
            return logicalClauseParser.RULE_singleComparison

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterSingleComparison" ):
                listener.enterSingleComparison(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitSingleComparison" ):
                listener.exitSingleComparison(self)




    def singleComparison(self):

        localctx = logicalClauseParser.SingleComparisonContext(self, self._ctx, self.state)
        self.enterRule(localctx, 6, self.RULE_singleComparison)
        try:
            self.state = 63
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,4,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 55
                self.variableName()
                self.state = 56
                self.comparisonOperand()
                self.state = 57
                self.comparisonSingleValue()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 59
                self.variableName()
                self.state = 60
                self.inOperand()
                self.state = 61
                self.comparisonMultiValue()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ComparisonSingleValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def variableName(self):
            return self.getTypedRuleContext(logicalClauseParser.VariableNameContext,0)


        def stringValue(self):
            return self.getTypedRuleContext(logicalClauseParser.StringValueContext,0)


        def numericValue(self):
            return self.getTypedRuleContext(logicalClauseParser.NumericValueContext,0)


        def getRuleIndex(self):
            return logicalClauseParser.RULE_comparisonSingleValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComparisonSingleValue" ):
                listener.enterComparisonSingleValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComparisonSingleValue" ):
                listener.exitComparisonSingleValue(self)




    def comparisonSingleValue(self):

        localctx = logicalClauseParser.ComparisonSingleValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 8, self.RULE_comparisonSingleValue)
        try:
            self.state = 68
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [18, 22]:
                self.enterOuterAlt(localctx, 1)
                self.state = 65
                self.variableName()
                pass
            elif token in [19]:
                self.enterOuterAlt(localctx, 2)
                self.state = 66
                self.stringValue()
                pass
            elif token in [17]:
                self.enterOuterAlt(localctx, 3)
                self.state = 67
                self.numericValue()
                pass
            else:
                raise NoViableAltException(self)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ComparisonMultiValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def mvVariable(self):
            return self.getTypedRuleContext(logicalClauseParser.MvVariableContext,0)


        def mvString(self):
            return self.getTypedRuleContext(logicalClauseParser.MvStringContext,0)


        def mvNumber(self):
            return self.getTypedRuleContext(logicalClauseParser.MvNumberContext,0)


        def mvMixed(self):
            return self.getTypedRuleContext(logicalClauseParser.MvMixedContext,0)


        def getRuleIndex(self):
            return logicalClauseParser.RULE_comparisonMultiValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComparisonMultiValue" ):
                listener.enterComparisonMultiValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComparisonMultiValue" ):
                listener.exitComparisonMultiValue(self)




    def comparisonMultiValue(self):

        localctx = logicalClauseParser.ComparisonMultiValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 10, self.RULE_comparisonMultiValue)
        try:
            self.state = 74
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,6,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 70
                self.mvVariable()
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 71
                self.mvString()
                pass

            elif la_ == 3:
                self.enterOuterAlt(localctx, 3)
                self.state = 72
                self.mvNumber()
                pass

            elif la_ == 4:
                self.enterOuterAlt(localctx, 4)
                self.state = 73
                self.mvMixed()
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class MvStringContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(logicalClauseParser.LPAREN, 0)

        def stringValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.StringValueContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.StringValueContext,i)


        def RPAREN(self):
            return self.getToken(logicalClauseParser.RPAREN, 0)

        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.COMMA)
            else:
                return self.getToken(logicalClauseParser.COMMA, i)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_mvString

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMvString" ):
                listener.enterMvString(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMvString" ):
                listener.exitMvString(self)




    def mvString(self):

        localctx = logicalClauseParser.MvStringContext(self, self._ctx, self.state)
        self.enterRule(localctx, 12, self.RULE_mvString)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 76
            self.match(logicalClauseParser.LPAREN)
            self.state = 77
            self.stringValue()
            self.state = 80 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 78
                self.match(logicalClauseParser.COMMA)
                self.state = 79
                self.stringValue()
                self.state = 82 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la==13):
                    break

            self.state = 84
            self.match(logicalClauseParser.RPAREN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class MvNumberContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(logicalClauseParser.LPAREN, 0)

        def numericValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.NumericValueContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.NumericValueContext,i)


        def RPAREN(self):
            return self.getToken(logicalClauseParser.RPAREN, 0)

        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.COMMA)
            else:
                return self.getToken(logicalClauseParser.COMMA, i)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_mvNumber

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMvNumber" ):
                listener.enterMvNumber(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMvNumber" ):
                listener.exitMvNumber(self)




    def mvNumber(self):

        localctx = logicalClauseParser.MvNumberContext(self, self._ctx, self.state)
        self.enterRule(localctx, 14, self.RULE_mvNumber)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 86
            self.match(logicalClauseParser.LPAREN)
            self.state = 87
            self.numericValue()
            self.state = 90 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 88
                self.match(logicalClauseParser.COMMA)
                self.state = 89
                self.numericValue()
                self.state = 92 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la==13):
                    break

            self.state = 94
            self.match(logicalClauseParser.RPAREN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class MvVariableContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(logicalClauseParser.LPAREN, 0)

        def variableName(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.VariableNameContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.VariableNameContext,i)


        def RPAREN(self):
            return self.getToken(logicalClauseParser.RPAREN, 0)

        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.COMMA)
            else:
                return self.getToken(logicalClauseParser.COMMA, i)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_mvVariable

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMvVariable" ):
                listener.enterMvVariable(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMvVariable" ):
                listener.exitMvVariable(self)




    def mvVariable(self):

        localctx = logicalClauseParser.MvVariableContext(self, self._ctx, self.state)
        self.enterRule(localctx, 16, self.RULE_mvVariable)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 96
            self.match(logicalClauseParser.LPAREN)
            self.state = 97
            self.variableName()
            self.state = 100 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 98
                self.match(logicalClauseParser.COMMA)
                self.state = 99
                self.variableName()
                self.state = 102 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la==13):
                    break

            self.state = 104
            self.match(logicalClauseParser.RPAREN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class MvMixedContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def LPAREN(self):
            return self.getToken(logicalClauseParser.LPAREN, 0)

        def RPAREN(self):
            return self.getToken(logicalClauseParser.RPAREN, 0)

        def stringValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.StringValueContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.StringValueContext,i)


        def numericValue(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.NumericValueContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.NumericValueContext,i)


        def variableName(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(logicalClauseParser.VariableNameContext)
            else:
                return self.getTypedRuleContext(logicalClauseParser.VariableNameContext,i)


        def COMMA(self, i:int=None):
            if i is None:
                return self.getTokens(logicalClauseParser.COMMA)
            else:
                return self.getToken(logicalClauseParser.COMMA, i)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_mvMixed

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterMvMixed" ):
                listener.enterMvMixed(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitMvMixed" ):
                listener.exitMvMixed(self)




    def mvMixed(self):

        localctx = logicalClauseParser.MvMixedContext(self, self._ctx, self.state)
        self.enterRule(localctx, 18, self.RULE_mvMixed)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 106
            self.match(logicalClauseParser.LPAREN)
            self.state = 110
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [19]:
                self.state = 107
                self.stringValue()
                pass
            elif token in [17]:
                self.state = 108
                self.numericValue()
                pass
            elif token in [18, 22]:
                self.state = 109
                self.variableName()
                pass
            else:
                raise NoViableAltException(self)

            self.state = 118 
            self._errHandler.sync(self)
            _la = self._input.LA(1)
            while True:
                self.state = 112
                self.match(logicalClauseParser.COMMA)
                self.state = 116
                self._errHandler.sync(self)
                token = self._input.LA(1)
                if token in [19]:
                    self.state = 113
                    self.stringValue()
                    pass
                elif token in [17]:
                    self.state = 114
                    self.numericValue()
                    pass
                elif token in [18, 22]:
                    self.state = 115
                    self.variableName()
                    pass
                else:
                    raise NoViableAltException(self)

                self.state = 120 
                self._errHandler.sync(self)
                _la = self._input.LA(1)
                if not (_la==13):
                    break

            self.state = 122
            self.match(logicalClauseParser.RPAREN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class StringValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def DOUBLE_QUOTED_STRING(self):
            return self.getToken(logicalClauseParser.DOUBLE_QUOTED_STRING, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_stringValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterStringValue" ):
                listener.enterStringValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitStringValue" ):
                listener.exitStringValue(self)




    def stringValue(self):

        localctx = logicalClauseParser.StringValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 20, self.RULE_stringValue)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 124
            self.match(logicalClauseParser.DOUBLE_QUOTED_STRING)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class NumericValueContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def NUMBER(self):
            return self.getToken(logicalClauseParser.NUMBER, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_numericValue

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterNumericValue" ):
                listener.enterNumericValue(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitNumericValue" ):
                listener.exitNumericValue(self)




    def numericValue(self):

        localctx = logicalClauseParser.NumericValueContext(self, self._ctx, self.state)
        self.enterRule(localctx, 22, self.RULE_numericValue)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 126
            self.match(logicalClauseParser.NUMBER)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class ComparisonOperandContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def EQ(self):
            return self.getToken(logicalClauseParser.EQ, 0)

        def NOTEQ(self):
            return self.getToken(logicalClauseParser.NOTEQ, 0)

        def LTEQ(self):
            return self.getToken(logicalClauseParser.LTEQ, 0)

        def GTEQ(self):
            return self.getToken(logicalClauseParser.GTEQ, 0)

        def LT(self):
            return self.getToken(logicalClauseParser.LT, 0)

        def GT(self):
            return self.getToken(logicalClauseParser.GT, 0)

        def CONTAINS(self):
            return self.getToken(logicalClauseParser.CONTAINS, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_comparisonOperand

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterComparisonOperand" ):
                listener.enterComparisonOperand(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitComparisonOperand" ):
                listener.exitComparisonOperand(self)




    def comparisonOperand(self):

        localctx = logicalClauseParser.ComparisonOperandContext(self, self._ctx, self.state)
        self.enterRule(localctx, 24, self.RULE_comparisonOperand)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 128
            _la = self._input.LA(1)
            if not((((_la) & ~0x3f) == 0 and ((1 << _la) & 8128) != 0)):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class InOperandContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def IN(self):
            return self.getToken(logicalClauseParser.IN, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_inOperand

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterInOperand" ):
                listener.enterInOperand(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitInOperand" ):
                listener.exitInOperand(self)




    def inOperand(self):

        localctx = logicalClauseParser.InOperandContext(self, self._ctx, self.state)
        self.enterRule(localctx, 26, self.RULE_inOperand)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 130
            self.match(logicalClauseParser.IN)
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx


    class VariableNameContext(ParserRuleContext):
        __slots__ = 'parser'

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def ID(self):
            return self.getToken(logicalClauseParser.ID, 0)

        def SINGLE_QUOTED_STRING(self):
            return self.getToken(logicalClauseParser.SINGLE_QUOTED_STRING, 0)

        def getRuleIndex(self):
            return logicalClauseParser.RULE_variableName

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterVariableName" ):
                listener.enterVariableName(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitVariableName" ):
                listener.exitVariableName(self)




    def variableName(self):

        localctx = logicalClauseParser.VariableNameContext(self, self._ctx, self.state)
        self.enterRule(localctx, 28, self.RULE_variableName)
        self._la = 0 # Token type
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 132
            _la = self._input.LA(1)
            if not(_la==18 or _la==22):
                self._errHandler.recoverInline(self)
            else:
                self._errHandler.reportMatch(self)
                self.consume()
        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx






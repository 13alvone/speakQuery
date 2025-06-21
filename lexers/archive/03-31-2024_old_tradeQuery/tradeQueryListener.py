# Generated from lexers/tradeQuery.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .tradeQueryParser import tradeQueryParser
else:
    from tradeQueryParser import tradeQueryParser

# This class defines a complete listener for a parse tree produced by tradeQueryParser.
class tradeQueryListener(ParseTreeListener):

    # Enter a parse tree produced by tradeQueryParser#program.
    def enterProgram(self, ctx:tradeQueryParser.ProgramContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#program.
    def exitProgram(self, ctx:tradeQueryParser.ProgramContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#tradeQuery.
    def enterTradeQuery(self, ctx:tradeQueryParser.TradeQueryContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#tradeQuery.
    def exitTradeQuery(self, ctx:tradeQueryParser.TradeQueryContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#validLine.
    def enterValidLine(self, ctx:tradeQueryParser.ValidLineContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#validLine.
    def exitValidLine(self, ctx:tradeQueryParser.ValidLineContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#directive.
    def enterDirective(self, ctx:tradeQueryParser.DirectiveContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#directive.
    def exitDirective(self, ctx:tradeQueryParser.DirectiveContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#expression.
    def enterExpression(self, ctx:tradeQueryParser.ExpressionContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#expression.
    def exitExpression(self, ctx:tradeQueryParser.ExpressionContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#expressionString.
    def enterExpressionString(self, ctx:tradeQueryParser.ExpressionStringContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#expressionString.
    def exitExpressionString(self, ctx:tradeQueryParser.ExpressionStringContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#logicalStringExpr.
    def enterLogicalStringExpr(self, ctx:tradeQueryParser.LogicalStringExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def exitLogicalStringExpr(self, ctx:tradeQueryParser.LogicalStringExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def enterArithmeticStringExpr(self, ctx:tradeQueryParser.ArithmeticStringExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def exitArithmeticStringExpr(self, ctx:tradeQueryParser.ArithmeticStringExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#factorStringExpr.
    def enterFactorStringExpr(self, ctx:tradeQueryParser.FactorStringExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#factorStringExpr.
    def exitFactorStringExpr(self, ctx:tradeQueryParser.FactorStringExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#expressionNumeric.
    def enterExpressionNumeric(self, ctx:tradeQueryParser.ExpressionNumericContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#expressionNumeric.
    def exitExpressionNumeric(self, ctx:tradeQueryParser.ExpressionNumericContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#compareNumericExpr.
    def enterCompareNumericExpr(self, ctx:tradeQueryParser.CompareNumericExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def exitCompareNumericExpr(self, ctx:tradeQueryParser.CompareNumericExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def enterArithmeticNumericResultOnlyExpr(self, ctx:tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def exitArithmeticNumericResultOnlyExpr(self, ctx:tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def enterTermNumericResultOnlyExpr(self, ctx:tradeQueryParser.TermNumericResultOnlyExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def exitTermNumericResultOnlyExpr(self, ctx:tradeQueryParser.TermNumericResultOnlyExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def enterFactorNumericResultOnlyExpr(self, ctx:tradeQueryParser.FactorNumericResultOnlyExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def exitFactorNumericResultOnlyExpr(self, ctx:tradeQueryParser.FactorNumericResultOnlyExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#expressionAny.
    def enterExpressionAny(self, ctx:tradeQueryParser.ExpressionAnyContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#expressionAny.
    def exitExpressionAny(self, ctx:tradeQueryParser.ExpressionAnyContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx:tradeQueryParser.LogicalExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx:tradeQueryParser.LogicalExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#compareExpr.
    def enterCompareExpr(self, ctx:tradeQueryParser.CompareExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#compareExpr.
    def exitCompareExpr(self, ctx:tradeQueryParser.CompareExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#arithmeticExpr.
    def enterArithmeticExpr(self, ctx:tradeQueryParser.ArithmeticExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def exitArithmeticExpr(self, ctx:tradeQueryParser.ArithmeticExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#termExpr.
    def enterTermExpr(self, ctx:tradeQueryParser.TermExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#termExpr.
    def exitTermExpr(self, ctx:tradeQueryParser.TermExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#factorExpr.
    def enterFactorExpr(self, ctx:tradeQueryParser.FactorExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#factorExpr.
    def exitFactorExpr(self, ctx:tradeQueryParser.FactorExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#value.
    def enterValue(self, ctx:tradeQueryParser.ValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#value.
    def exitValue(self, ctx:tradeQueryParser.ValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#valueStringOnly.
    def enterValueStringOnly(self, ctx:tradeQueryParser.ValueStringOnlyContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#valueStringOnly.
    def exitValueStringOnly(self, ctx:tradeQueryParser.ValueStringOnlyContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def enterValueNumericResultOnly(self, ctx:tradeQueryParser.ValueNumericResultOnlyContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def exitValueNumericResultOnly(self, ctx:tradeQueryParser.ValueNumericResultOnlyContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx:tradeQueryParser.UnaryExprContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx:tradeQueryParser.UnaryExprContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#numericValue.
    def enterNumericValue(self, ctx:tradeQueryParser.NumericValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#numericValue.
    def exitNumericValue(self, ctx:tradeQueryParser.NumericValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#booleanValue.
    def enterBooleanValue(self, ctx:tradeQueryParser.BooleanValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#booleanValue.
    def exitBooleanValue(self, ctx:tradeQueryParser.BooleanValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#stringValue.
    def enterStringValue(self, ctx:tradeQueryParser.StringValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#stringValue.
    def exitStringValue(self, ctx:tradeQueryParser.StringValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#arrayValue.
    def enterArrayValue(self, ctx:tradeQueryParser.ArrayValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#arrayValue.
    def exitArrayValue(self, ctx:tradeQueryParser.ArrayValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def enterArrayValueNumeric(self, ctx:tradeQueryParser.ArrayValueNumericContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def exitArrayValueNumeric(self, ctx:tradeQueryParser.ArrayValueNumericContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#variableValue.
    def enterVariableValue(self, ctx:tradeQueryParser.VariableValueContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#variableValue.
    def exitVariableValue(self, ctx:tradeQueryParser.VariableValueContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx:tradeQueryParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx:tradeQueryParser.ComparisonOperatorContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#logicalOperator.
    def enterLogicalOperator(self, ctx:tradeQueryParser.LogicalOperatorContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#logicalOperator.
    def exitLogicalOperator(self, ctx:tradeQueryParser.LogicalOperatorContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#functionCall.
    def enterFunctionCall(self, ctx:tradeQueryParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#functionCall.
    def exitFunctionCall(self, ctx:tradeQueryParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx:tradeQueryParser.NumericFunctionCallContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx:tradeQueryParser.NumericFunctionCallContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx:tradeQueryParser.StringFunctionCallContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx:tradeQueryParser.StringFunctionCallContext):
        pass


    # Enter a parse tree produced by tradeQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx:tradeQueryParser.SpecificFunctionCallContext):
        pass

    # Exit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx:tradeQueryParser.SpecificFunctionCallContext):
        pass



del tradeQueryParser
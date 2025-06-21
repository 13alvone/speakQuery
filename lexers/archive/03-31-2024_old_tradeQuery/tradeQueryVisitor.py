# Generated from lexers/tradeQuery.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .tradeQueryParser import tradeQueryParser
else:
    from tradeQueryParser import tradeQueryParser

# This class defines a complete generic visitor for a parse tree produced by tradeQueryParser.

class tradeQueryVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by tradeQueryParser#program.
    def visitProgram(self, ctx:tradeQueryParser.ProgramContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#tradeQuery.
    def visitTradeQuery(self, ctx:tradeQueryParser.TradeQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#validLine.
    def visitValidLine(self, ctx:tradeQueryParser.ValidLineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#directive.
    def visitDirective(self, ctx:tradeQueryParser.DirectiveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#expression.
    def visitExpression(self, ctx:tradeQueryParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#expressionString.
    def visitExpressionString(self, ctx:tradeQueryParser.ExpressionStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def visitLogicalStringExpr(self, ctx:tradeQueryParser.LogicalStringExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def visitArithmeticStringExpr(self, ctx:tradeQueryParser.ArithmeticStringExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#factorStringExpr.
    def visitFactorStringExpr(self, ctx:tradeQueryParser.FactorStringExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#expressionNumeric.
    def visitExpressionNumeric(self, ctx:tradeQueryParser.ExpressionNumericContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def visitCompareNumericExpr(self, ctx:tradeQueryParser.CompareNumericExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def visitArithmeticNumericResultOnlyExpr(self, ctx:tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def visitTermNumericResultOnlyExpr(self, ctx:tradeQueryParser.TermNumericResultOnlyExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def visitFactorNumericResultOnlyExpr(self, ctx:tradeQueryParser.FactorNumericResultOnlyExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#expressionAny.
    def visitExpressionAny(self, ctx:tradeQueryParser.ExpressionAnyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#logicalExpr.
    def visitLogicalExpr(self, ctx:tradeQueryParser.LogicalExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#compareExpr.
    def visitCompareExpr(self, ctx:tradeQueryParser.CompareExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def visitArithmeticExpr(self, ctx:tradeQueryParser.ArithmeticExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#termExpr.
    def visitTermExpr(self, ctx:tradeQueryParser.TermExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#factorExpr.
    def visitFactorExpr(self, ctx:tradeQueryParser.FactorExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#value.
    def visitValue(self, ctx:tradeQueryParser.ValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#valueStringOnly.
    def visitValueStringOnly(self, ctx:tradeQueryParser.ValueStringOnlyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def visitValueNumericResultOnly(self, ctx:tradeQueryParser.ValueNumericResultOnlyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#unaryExpr.
    def visitUnaryExpr(self, ctx:tradeQueryParser.UnaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#numericValue.
    def visitNumericValue(self, ctx:tradeQueryParser.NumericValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#booleanValue.
    def visitBooleanValue(self, ctx:tradeQueryParser.BooleanValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#stringValue.
    def visitStringValue(self, ctx:tradeQueryParser.StringValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#arrayValue.
    def visitArrayValue(self, ctx:tradeQueryParser.ArrayValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def visitArrayValueNumeric(self, ctx:tradeQueryParser.ArrayValueNumericContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#variableValue.
    def visitVariableValue(self, ctx:tradeQueryParser.VariableValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:tradeQueryParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#logicalOperator.
    def visitLogicalOperator(self, ctx:tradeQueryParser.LogicalOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#functionCall.
    def visitFunctionCall(self, ctx:tradeQueryParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def visitNumericFunctionCall(self, ctx:tradeQueryParser.NumericFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def visitStringFunctionCall(self, ctx:tradeQueryParser.StringFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def visitSpecificFunctionCall(self, ctx:tradeQueryParser.SpecificFunctionCallContext):
        return self.visitChildren(ctx)



del tradeQueryParser
# Generated from logicalClause.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .logicalClauseParser import logicalClauseParser
else:
    from logicalClauseParser import logicalClauseParser

# This class defines a complete listener for a parse tree produced by logicalClauseParser.
class logicalClauseListener(ParseTreeListener):

    # Enter a parse tree produced by logicalClauseParser#logicalClause.
    def enterLogicalClause(self, ctx:logicalClauseParser.LogicalClauseContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#logicalClause.
    def exitLogicalClause(self, ctx:logicalClauseParser.LogicalClauseContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#multiLogicalExpression.
    def enterMultiLogicalExpression(self, ctx:logicalClauseParser.MultiLogicalExpressionContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#multiLogicalExpression.
    def exitMultiLogicalExpression(self, ctx:logicalClauseParser.MultiLogicalExpressionContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#singleLogicalExpression.
    def enterSingleLogicalExpression(self, ctx:logicalClauseParser.SingleLogicalExpressionContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#singleLogicalExpression.
    def exitSingleLogicalExpression(self, ctx:logicalClauseParser.SingleLogicalExpressionContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#singleComparison.
    def enterSingleComparison(self, ctx:logicalClauseParser.SingleComparisonContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#singleComparison.
    def exitSingleComparison(self, ctx:logicalClauseParser.SingleComparisonContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#comparisonSingleValue.
    def enterComparisonSingleValue(self, ctx:logicalClauseParser.ComparisonSingleValueContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#comparisonSingleValue.
    def exitComparisonSingleValue(self, ctx:logicalClauseParser.ComparisonSingleValueContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#comparisonMultiValue.
    def enterComparisonMultiValue(self, ctx:logicalClauseParser.ComparisonMultiValueContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#comparisonMultiValue.
    def exitComparisonMultiValue(self, ctx:logicalClauseParser.ComparisonMultiValueContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#mvString.
    def enterMvString(self, ctx:logicalClauseParser.MvStringContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#mvString.
    def exitMvString(self, ctx:logicalClauseParser.MvStringContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#mvNumber.
    def enterMvNumber(self, ctx:logicalClauseParser.MvNumberContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#mvNumber.
    def exitMvNumber(self, ctx:logicalClauseParser.MvNumberContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#mvVariable.
    def enterMvVariable(self, ctx:logicalClauseParser.MvVariableContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#mvVariable.
    def exitMvVariable(self, ctx:logicalClauseParser.MvVariableContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#mvMixed.
    def enterMvMixed(self, ctx:logicalClauseParser.MvMixedContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#mvMixed.
    def exitMvMixed(self, ctx:logicalClauseParser.MvMixedContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#stringValue.
    def enterStringValue(self, ctx:logicalClauseParser.StringValueContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#stringValue.
    def exitStringValue(self, ctx:logicalClauseParser.StringValueContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#numericValue.
    def enterNumericValue(self, ctx:logicalClauseParser.NumericValueContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#numericValue.
    def exitNumericValue(self, ctx:logicalClauseParser.NumericValueContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#comparisonOperand.
    def enterComparisonOperand(self, ctx:logicalClauseParser.ComparisonOperandContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#comparisonOperand.
    def exitComparisonOperand(self, ctx:logicalClauseParser.ComparisonOperandContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#inOperand.
    def enterInOperand(self, ctx:logicalClauseParser.InOperandContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#inOperand.
    def exitInOperand(self, ctx:logicalClauseParser.InOperandContext):
        pass


    # Enter a parse tree produced by logicalClauseParser#variableName.
    def enterVariableName(self, ctx:logicalClauseParser.VariableNameContext):
        pass

    # Exit a parse tree produced by logicalClauseParser#variableName.
    def exitVariableName(self, ctx:logicalClauseParser.VariableNameContext):
        pass



del logicalClauseParser
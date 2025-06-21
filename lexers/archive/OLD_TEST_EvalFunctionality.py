# Import the base EvalVisitor class
from EvalVisitor import EvalVisitor
from EvalParser import EvalParser


class ExtendedEvalVisitor(EvalVisitor):

    # Visit a parse tree produced by EvalParser#program.
    def visitProgram(self, ctx:EvalParser.ProgramContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#expression.
    def visitExpression(self, ctx:EvalParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#logicalExpr.
    def visitLogicalExpr(self, ctx:EvalParser.LogicalExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#compareExpr.
    def visitCompareExpr(self, ctx:EvalParser.CompareExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#arithmeticExpr.
    def visitArithmeticExpr(self, ctx:EvalParser.ArithmeticExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#termExpr.
    def visitTermExpr(self, ctx:EvalParser.TermExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#factorExpr.
    def visitFactorExpr(self, ctx:EvalParser.FactorExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#value.
    def visitValue(self, ctx:EvalParser.ValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#unaryExpr.
    def visitUnaryExpr(self, ctx:EvalParser.UnaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#numericValue.
    def visitNumericValue(self, ctx:EvalParser.NumericValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#booleanValue.
    def visitBooleanValue(self, ctx:EvalParser.BooleanValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#stringValue.
    def visitStringValue(self, ctx:EvalParser.StringValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#arrayValue.
    def visitArrayValue(self, ctx:EvalParser.ArrayValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#variableValue.
    def visitVariableValue(self, ctx:EvalParser.VariableValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:EvalParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#logicalOperator.
    def visitLogicalOperator(self, ctx:EvalParser.LogicalOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#functionCall.
    def visitFunctionCall(self, ctx:EvalParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#numericFunctionCall.
    def visitNumericFunctionCall(self, ctx:EvalParser.NumericFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#stringFunctionCall.
    def visitStringFunctionCall(self, ctx:EvalParser.StringFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#specificFunctionCall.
    def visitSpecificFunctionCall(self, ctx:EvalParser.SpecificFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#numericFunctionName.
    def visitNumericFunctionName(self, ctx:EvalParser.NumericFunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#stringFunctionName.
    def visitStringFunctionName(self, ctx:EvalParser.StringFunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#specificFunctionName.
    def visitSpecificFunctionName(self, ctx:EvalParser.SpecificFunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#noParametersFunctionName.
    def visitNoParametersFunctionName(self, ctx:EvalParser.NoParametersFunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by EvalParser#functionName.
    def visitFunctionName(self, ctx:EvalParser.FunctionNameContext):
        return self.visitChildren(ctx)


del EvalParser
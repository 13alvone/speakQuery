# Generated from lexers/speakQuery.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser

# This class defines a complete generic visitor for a parse tree produced by speakQueryParser.

class speakQueryVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by speakQueryParser#query.
    def visitQuery(self, ctx:speakQueryParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#speakQuery.
    def visitSpeakQuery(self, ctx:speakQueryParser.SpeakQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#filterExpression.
    def visitFilterExpression(self, ctx:speakQueryParser.FilterExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#filterTerm.
    def visitFilterTerm(self, ctx:speakQueryParser.FilterTermContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#preTimeFilter.
    def visitPreTimeFilter(self, ctx:speakQueryParser.PreTimeFilterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#earliestClause.
    def visitEarliestClause(self, ctx:speakQueryParser.EarliestClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#latestClause.
    def visitLatestClause(self, ctx:speakQueryParser.LatestClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#logicalExpr.
    def visitLogicalExpr(self, ctx:speakQueryParser.LogicalExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#logicalOrExpr.
    def visitLogicalOrExpr(self, ctx:speakQueryParser.LogicalOrExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#logicalAndExpr.
    def visitLogicalAndExpr(self, ctx:speakQueryParser.LogicalAndExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#logicalNotExpr.
    def visitLogicalNotExpr(self, ctx:speakQueryParser.LogicalNotExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#logicalPrimaryExpr.
    def visitLogicalPrimaryExpr(self, ctx:speakQueryParser.LogicalPrimaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#indexClause.
    def visitIndexClause(self, ctx:speakQueryParser.IndexClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#comparisonExpr.
    def visitComparisonExpr(self, ctx:speakQueryParser.ComparisonExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:speakQueryParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#additiveExpr.
    def visitAdditiveExpr(self, ctx:speakQueryParser.AdditiveExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#multiplicativeExpr.
    def visitMultiplicativeExpr(self, ctx:speakQueryParser.MultiplicativeExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#unaryExpr.
    def visitUnaryExpr(self, ctx:speakQueryParser.UnaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#primaryExpr.
    def visitPrimaryExpr(self, ctx:speakQueryParser.PrimaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#booleanExpr.
    def visitBooleanExpr(self, ctx:speakQueryParser.BooleanExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#inExpression.
    def visitInExpression(self, ctx:speakQueryParser.InExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#alternateStartingCall.
    def visitAlternateStartingCall(self, ctx:speakQueryParser.AlternateStartingCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#validLine.
    def visitValidLine(self, ctx:speakQueryParser.ValidLineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#directive.
    def visitDirective(self, ctx:speakQueryParser.DirectiveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#statsAgg.
    def visitStatsAgg(self, ctx:speakQueryParser.StatsAggContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#variableList.
    def visitVariableList(self, ctx:speakQueryParser.VariableListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#subsearchList.
    def visitSubsearchList(self, ctx:speakQueryParser.SubsearchListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#subsearch.
    def visitSubsearch(self, ctx:speakQueryParser.SubsearchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#sharedField.
    def visitSharedField(self, ctx:speakQueryParser.SharedFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#filename.
    def visitFilename(self, ctx:speakQueryParser.FilenameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#specialFunctionName.
    def visitSpecialFunctionName(self, ctx:speakQueryParser.SpecialFunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#expression.
    def visitExpression(self, ctx:speakQueryParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#ifExpression.
    def visitIfExpression(self, ctx:speakQueryParser.IfExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#caseExpression.
    def visitCaseExpression(self, ctx:speakQueryParser.CaseExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#caseMatch.
    def visitCaseMatch(self, ctx:speakQueryParser.CaseMatchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#caseTrue.
    def visitCaseTrue(self, ctx:speakQueryParser.CaseTrueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#catchAllExpression.
    def visitCatchAllExpression(self, ctx:speakQueryParser.CatchAllExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#functionCall.
    def visitFunctionCall(self, ctx:speakQueryParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#numericFunctionCall.
    def visitNumericFunctionCall(self, ctx:speakQueryParser.NumericFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#stringFunctionCall.
    def visitStringFunctionCall(self, ctx:speakQueryParser.StringFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#specificFunctionCall.
    def visitSpecificFunctionCall(self, ctx:speakQueryParser.SpecificFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#statsFunctionCall.
    def visitStatsFunctionCall(self, ctx:speakQueryParser.StatsFunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticNumber.
    def visitStaticNumber(self, ctx:speakQueryParser.StaticNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticString.
    def visitStaticString(self, ctx:speakQueryParser.StaticStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#multivalueField.
    def visitMultivalueField(self, ctx:speakQueryParser.MultivalueFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#multivalueStringField.
    def visitMultivalueStringField(self, ctx:speakQueryParser.MultivalueStringFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#multivalueNumericField.
    def visitMultivalueNumericField(self, ctx:speakQueryParser.MultivalueNumericFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticMultivalueStringField.
    def visitStaticMultivalueStringField(self, ctx:speakQueryParser.StaticMultivalueStringFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticMultivalueNumericField.
    def visitStaticMultivalueNumericField(self, ctx:speakQueryParser.StaticMultivalueNumericFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#regexTarget.
    def visitRegexTarget(self, ctx:speakQueryParser.RegexTargetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#mvfindObject.
    def visitMvfindObject(self, ctx:speakQueryParser.MvfindObjectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#mvindexIndex.
    def visitMvindexIndex(self, ctx:speakQueryParser.MvindexIndexContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#mvDelim.
    def visitMvDelim(self, ctx:speakQueryParser.MvDelimContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#inputCron.
    def visitInputCron(self, ctx:speakQueryParser.InputCronContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#cronformat.
    def visitCronformat(self, ctx:speakQueryParser.CronformatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#executionMacro.
    def visitExecutionMacro(self, ctx:speakQueryParser.ExecutionMacroContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#timeStringValue.
    def visitTimeStringValue(self, ctx:speakQueryParser.TimeStringValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#timespan.
    def visitTimespan(self, ctx:speakQueryParser.TimespanContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#variableName.
    def visitVariableName(self, ctx:speakQueryParser.VariableNameContext):
        return self.visitChildren(ctx)



del speakQueryParser
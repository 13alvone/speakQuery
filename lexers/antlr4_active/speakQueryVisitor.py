# Generated from lexers/speakQuery.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser

# This class defines a complete generic visitor for a parse tree produced by speakQueryParser.

class speakQueryVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by speakQueryParser#speakQuery.
    def visitSpeakQuery(self, ctx:speakQueryParser.SpeakQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#initialSequence.
    def visitInitialSequence(self, ctx:speakQueryParser.InitialSequenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#expression.
    def visitExpression(self, ctx:speakQueryParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#conjunction.
    def visitConjunction(self, ctx:speakQueryParser.ConjunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#comparison.
    def visitComparison(self, ctx:speakQueryParser.ComparisonContext):
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


    # Visit a parse tree produced by speakQueryParser#primary.
    def visitPrimary(self, ctx:speakQueryParser.PrimaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#timeClause.
    def visitTimeClause(self, ctx:speakQueryParser.TimeClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#earliestClause.
    def visitEarliestClause(self, ctx:speakQueryParser.EarliestClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#latestClause.
    def visitLatestClause(self, ctx:speakQueryParser.LatestClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#indexClause.
    def visitIndexClause(self, ctx:speakQueryParser.IndexClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:speakQueryParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#inExpression.
    def visitInExpression(self, ctx:speakQueryParser.InExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#inputlookupInit.
    def visitInputlookupInit(self, ctx:speakQueryParser.InputlookupInitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#loadjobInit.
    def visitLoadjobInit(self, ctx:speakQueryParser.LoadjobInitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#validLine.
    def visitValidLine(self, ctx:speakQueryParser.ValidLineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#directive.
    def visitDirective(self, ctx:speakQueryParser.DirectiveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#macro.
    def visitMacro(self, ctx:speakQueryParser.MacroContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#statsAgg.
    def visitStatsAgg(self, ctx:speakQueryParser.StatsAggContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#variableList.
    def visitVariableList(self, ctx:speakQueryParser.VariableListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#subsearch.
    def visitSubsearch(self, ctx:speakQueryParser.SubsearchContext):
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


    # Visit a parse tree produced by speakQueryParser#timespan.
    def visitTimespan(self, ctx:speakQueryParser.TimespanContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#variableName.
    def visitVariableName(self, ctx:speakQueryParser.VariableNameContext):
        return self.visitChildren(ctx)



del speakQueryParser
# Generated from speakQuery.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser

# This class defines a complete generic visitor for a parse tree produced by speakQueryParser.

class speakQueryVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by speakQueryParser#program.
    def visitProgram(self, ctx:speakQueryParser.ProgramContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#speakQuery.
    def visitSpeakQuery(self, ctx:speakQueryParser.SpeakQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#preFilter.
    def visitPreFilter(self, ctx:speakQueryParser.PreFilterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#preFilterEntry.
    def visitPreFilterEntry(self, ctx:speakQueryParser.PreFilterEntryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#preTimeFilter.
    def visitPreTimeFilter(self, ctx:speakQueryParser.PreTimeFilterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#tableCall.
    def visitTableCall(self, ctx:speakQueryParser.TableCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#tableName.
    def visitTableName(self, ctx:speakQueryParser.TableNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#timerangeCall.
    def visitTimerangeCall(self, ctx:speakQueryParser.TimerangeCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#validLine.
    def visitValidLine(self, ctx:speakQueryParser.ValidLineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#directive.
    def visitDirective(self, ctx:speakQueryParser.DirectiveContext):
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


    # Visit a parse tree produced by speakQueryParser#logicalExpr.
    def visitLogicalExpr(self, ctx:speakQueryParser.LogicalExprContext):
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


    # Visit a parse tree produced by speakQueryParser#primaryExpr.
    def visitPrimaryExpr(self, ctx:speakQueryParser.PrimaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#booleanExpr.
    def visitBooleanExpr(self, ctx:speakQueryParser.BooleanExprContext):
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


    # Visit a parse tree produced by speakQueryParser#inExpression.
    def visitInExpression(self, ctx:speakQueryParser.InExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#catchAllExpression.
    def visitCatchAllExpression(self, ctx:speakQueryParser.CatchAllExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#unaryExpr.
    def visitUnaryExpr(self, ctx:speakQueryParser.UnaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticNumber.
    def visitStaticNumber(self, ctx:speakQueryParser.StaticNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#staticString.
    def visitStaticString(self, ctx:speakQueryParser.StaticStringContext):
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


    # Visit a parse tree produced by speakQueryParser#stringFunctionTarget.
    def visitStringFunctionTarget(self, ctx:speakQueryParser.StringFunctionTargetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by speakQueryParser#httpStringField.
    def visitHttpStringField(self, ctx:speakQueryParser.HttpStringFieldContext):
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


    # Visit a parse tree produced by speakQueryParser#executionMaro.
    def visitExecutionMaro(self, ctx:speakQueryParser.ExecutionMaroContext):
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
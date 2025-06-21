# Generated from speakQuery.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser

# This class defines a complete listener for a parse tree produced by speakQueryParser.
class speakQueryListener(ParseTreeListener):

    # Enter a parse tree produced by speakQueryParser#program.
    def enterProgram(self, ctx:speakQueryParser.ProgramContext):
        pass

    # Exit a parse tree produced by speakQueryParser#program.
    def exitProgram(self, ctx:speakQueryParser.ProgramContext):
        pass


    # Enter a parse tree produced by speakQueryParser#speakQuery.
    def enterSpeakQuery(self, ctx:speakQueryParser.SpeakQueryContext):
        pass

    # Exit a parse tree produced by speakQueryParser#speakQuery.
    def exitSpeakQuery(self, ctx:speakQueryParser.SpeakQueryContext):
        pass


    # Enter a parse tree produced by speakQueryParser#validLine.
    def enterValidLine(self, ctx:speakQueryParser.ValidLineContext):
        pass

    # Exit a parse tree produced by speakQueryParser#validLine.
    def exitValidLine(self, ctx:speakQueryParser.ValidLineContext):
        pass


    # Enter a parse tree produced by speakQueryParser#directive.
    def enterDirective(self, ctx:speakQueryParser.DirectiveContext):
        pass

    # Exit a parse tree produced by speakQueryParser#directive.
    def exitDirective(self, ctx:speakQueryParser.DirectiveContext):
        pass


    # Enter a parse tree produced by speakQueryParser#specialFunctionName.
    def enterSpecialFunctionName(self, ctx:speakQueryParser.SpecialFunctionNameContext):
        pass

    # Exit a parse tree produced by speakQueryParser#specialFunctionName.
    def exitSpecialFunctionName(self, ctx:speakQueryParser.SpecialFunctionNameContext):
        pass


    # Enter a parse tree produced by speakQueryParser#expression.
    def enterExpression(self, ctx:speakQueryParser.ExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#expression.
    def exitExpression(self, ctx:speakQueryParser.ExpressionContext):
        pass


    # Enter a parse tree produced by speakQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx:speakQueryParser.LogicalExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx:speakQueryParser.LogicalExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#comparisonExpr.
    def enterComparisonExpr(self, ctx:speakQueryParser.ComparisonExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#comparisonExpr.
    def exitComparisonExpr(self, ctx:speakQueryParser.ComparisonExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#additiveExpr.
    def enterAdditiveExpr(self, ctx:speakQueryParser.AdditiveExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#additiveExpr.
    def exitAdditiveExpr(self, ctx:speakQueryParser.AdditiveExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#multiplicativeExpr.
    def enterMultiplicativeExpr(self, ctx:speakQueryParser.MultiplicativeExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#multiplicativeExpr.
    def exitMultiplicativeExpr(self, ctx:speakQueryParser.MultiplicativeExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#primaryExpr.
    def enterPrimaryExpr(self, ctx:speakQueryParser.PrimaryExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#primaryExpr.
    def exitPrimaryExpr(self, ctx:speakQueryParser.PrimaryExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#ifExpression.
    def enterIfExpression(self, ctx:speakQueryParser.IfExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#ifExpression.
    def exitIfExpression(self, ctx:speakQueryParser.IfExpressionContext):
        pass


    # Enter a parse tree produced by speakQueryParser#caseExpression.
    def enterCaseExpression(self, ctx:speakQueryParser.CaseExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#caseExpression.
    def exitCaseExpression(self, ctx:speakQueryParser.CaseExpressionContext):
        pass


    # Enter a parse tree produced by speakQueryParser#inExpression.
    def enterInExpression(self, ctx:speakQueryParser.InExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#inExpression.
    def exitInExpression(self, ctx:speakQueryParser.InExpressionContext):
        pass


    # Enter a parse tree produced by speakQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx:speakQueryParser.UnaryExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx:speakQueryParser.UnaryExprContext):
        pass


    # Enter a parse tree produced by speakQueryParser#functionCall.
    def enterFunctionCall(self, ctx:speakQueryParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#functionCall.
    def exitFunctionCall(self, ctx:speakQueryParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by speakQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx:speakQueryParser.NumericFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx:speakQueryParser.NumericFunctionCallContext):
        pass


    # Enter a parse tree produced by speakQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx:speakQueryParser.StringFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx:speakQueryParser.StringFunctionCallContext):
        pass


    # Enter a parse tree produced by speakQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx:speakQueryParser.SpecificFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx:speakQueryParser.SpecificFunctionCallContext):
        pass


    # Enter a parse tree produced by speakQueryParser#statsFunctionCall.
    def enterStatsFunctionCall(self, ctx:speakQueryParser.StatsFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#statsFunctionCall.
    def exitStatsFunctionCall(self, ctx:speakQueryParser.StatsFunctionCallContext):
        pass


    # Enter a parse tree produced by speakQueryParser#stringFunctionTarget.
    def enterStringFunctionTarget(self, ctx:speakQueryParser.StringFunctionTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#stringFunctionTarget.
    def exitStringFunctionTarget(self, ctx:speakQueryParser.StringFunctionTargetContext):
        pass


    # Enter a parse tree produced by speakQueryParser#httpStringField.
    def enterHttpStringField(self, ctx:speakQueryParser.HttpStringFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#httpStringField.
    def exitHttpStringField(self, ctx:speakQueryParser.HttpStringFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#multivalueField.
    def enterMultivalueField(self, ctx:speakQueryParser.MultivalueFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#multivalueField.
    def exitMultivalueField(self, ctx:speakQueryParser.MultivalueFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#multivalueStringField.
    def enterMultivalueStringField(self, ctx:speakQueryParser.MultivalueStringFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#multivalueStringField.
    def exitMultivalueStringField(self, ctx:speakQueryParser.MultivalueStringFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#multivalueNumericField.
    def enterMultivalueNumericField(self, ctx:speakQueryParser.MultivalueNumericFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#multivalueNumericField.
    def exitMultivalueNumericField(self, ctx:speakQueryParser.MultivalueNumericFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#staticMultivalueStringField.
    def enterStaticMultivalueStringField(self, ctx:speakQueryParser.StaticMultivalueStringFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#staticMultivalueStringField.
    def exitStaticMultivalueStringField(self, ctx:speakQueryParser.StaticMultivalueStringFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#staticMultivalueNumericField.
    def enterStaticMultivalueNumericField(self, ctx:speakQueryParser.StaticMultivalueNumericFieldContext):
        pass

    # Exit a parse tree produced by speakQueryParser#staticMultivalueNumericField.
    def exitStaticMultivalueNumericField(self, ctx:speakQueryParser.StaticMultivalueNumericFieldContext):
        pass


    # Enter a parse tree produced by speakQueryParser#regexTarget.
    def enterRegexTarget(self, ctx:speakQueryParser.RegexTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#regexTarget.
    def exitRegexTarget(self, ctx:speakQueryParser.RegexTargetContext):
        pass


    # Enter a parse tree produced by speakQueryParser#trimTarget.
    def enterTrimTarget(self, ctx:speakQueryParser.TrimTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#trimTarget.
    def exitTrimTarget(self, ctx:speakQueryParser.TrimTargetContext):
        pass


    # Enter a parse tree produced by speakQueryParser#substrTarget.
    def enterSubstrTarget(self, ctx:speakQueryParser.SubstrTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#substrTarget.
    def exitSubstrTarget(self, ctx:speakQueryParser.SubstrTargetContext):
        pass


    # Enter a parse tree produced by speakQueryParser#substrStart.
    def enterSubstrStart(self, ctx:speakQueryParser.SubstrStartContext):
        pass

    # Exit a parse tree produced by speakQueryParser#substrStart.
    def exitSubstrStart(self, ctx:speakQueryParser.SubstrStartContext):
        pass


    # Enter a parse tree produced by speakQueryParser#substrLength.
    def enterSubstrLength(self, ctx:speakQueryParser.SubstrLengthContext):
        pass

    # Exit a parse tree produced by speakQueryParser#substrLength.
    def exitSubstrLength(self, ctx:speakQueryParser.SubstrLengthContext):
        pass


    # Enter a parse tree produced by speakQueryParser#trimRemovalTarget.
    def enterTrimRemovalTarget(self, ctx:speakQueryParser.TrimRemovalTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#trimRemovalTarget.
    def exitTrimRemovalTarget(self, ctx:speakQueryParser.TrimRemovalTargetContext):
        pass


    # Enter a parse tree produced by speakQueryParser#mvfindObject.
    def enterMvfindObject(self, ctx:speakQueryParser.MvfindObjectContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvfindObject.
    def exitMvfindObject(self, ctx:speakQueryParser.MvfindObjectContext):
        pass


    # Enter a parse tree produced by speakQueryParser#mvindexIndex.
    def enterMvindexIndex(self, ctx:speakQueryParser.MvindexIndexContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvindexIndex.
    def exitMvindexIndex(self, ctx:speakQueryParser.MvindexIndexContext):
        pass


    # Enter a parse tree produced by speakQueryParser#mvDelim.
    def enterMvDelim(self, ctx:speakQueryParser.MvDelimContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvDelim.
    def exitMvDelim(self, ctx:speakQueryParser.MvDelimContext):
        pass


    # Enter a parse tree produced by speakQueryParser#inputCron.
    def enterInputCron(self, ctx:speakQueryParser.InputCronContext):
        pass

    # Exit a parse tree produced by speakQueryParser#inputCron.
    def exitInputCron(self, ctx:speakQueryParser.InputCronContext):
        pass


    # Enter a parse tree produced by speakQueryParser#cronformat.
    def enterCronformat(self, ctx:speakQueryParser.CronformatContext):
        pass

    # Exit a parse tree produced by speakQueryParser#cronformat.
    def exitCronformat(self, ctx:speakQueryParser.CronformatContext):
        pass


    # Enter a parse tree produced by speakQueryParser#executionMaro.
    def enterExecutionMaro(self, ctx:speakQueryParser.ExecutionMaroContext):
        pass

    # Exit a parse tree produced by speakQueryParser#executionMaro.
    def exitExecutionMaro(self, ctx:speakQueryParser.ExecutionMaroContext):
        pass


    # Enter a parse tree produced by speakQueryParser#timeStringValue.
    def enterTimeStringValue(self, ctx:speakQueryParser.TimeStringValueContext):
        pass

    # Exit a parse tree produced by speakQueryParser#timeStringValue.
    def exitTimeStringValue(self, ctx:speakQueryParser.TimeStringValueContext):
        pass



del speakQueryParser
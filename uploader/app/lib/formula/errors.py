class ExpressionError(Exception):
    pass


class ExpressionSyntaxError(ExpressionError):
    pass


class ExpressionEvaluationError(ExpressionError):
    pass

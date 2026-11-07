"""Error classification used by the retry machinery."""


class TransientError(Exception):
    """Retryable: timeouts, 429/5xx, connection resets, lost acks."""


class PermanentError(Exception):
    """Not retryable: validation errors, 4xx, budget/step limits, rejected input."""


class BudgetExceeded(PermanentError):
    pass


class LeaseLost(Exception):
    """The worker no longer owns the job (cancelled / timed out / lease stolen)."""

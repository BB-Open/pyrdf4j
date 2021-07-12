"""Error Api"""

class URINotReachable(Exception):
    """URINotReachable"""


class BulkLoadError(Exception):
    """BulkLoadError"""


class CreateRepositoryError(Exception):
    """CreateRepositoryError"""


class CreateRepositoryAlreadyExists(Exception):
    """CreateRepositoryAlreadyExists"""


class DropRepositoryError(Exception):
    """DropRepositoryError"""


class CannotStartTransaction(Exception):
    """CannotStartTransactionError"""


class CannotCommitTransaction(Exception):
    """CannotCommitTransaction"""


class CannotRollbackTransaction(Exception):
    """CannotRollbackTransaction"""


class RollbackOccurred(Exception):
    """RollbackOccurred"""


class TerminatingError(Exception):
    """TerminatingError"""


class RepositoryTypeUnknown(Exception):
    """RepositoryTypeUnknown"""


class QueryFailed(Exception):
    """QueryFailed"""

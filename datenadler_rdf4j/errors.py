"""Error Api"""

class HarvestURINotReachable(Exception):
    """HarvestURINotReachable"""


class TripleStoreBulkLoadError(Exception):
    """TripleStoreBulkLoadError"""


class TripleStoreCreateRepositoryError(Exception):
    """TripleStoreCreateRepositoryError"""


class TripleStoreCreateRepositoryAlreadyExists(Exception):
    """TripleStoreCreateRepositoryAlreadyExists"""


class TripleStoreDropRepositoryError(Exception):
    """TripleStoreDropRepositoryError"""


class TripleStoreCannotStartTransaction(Exception):
    """TripleStoreCannotStartTransactionError"""


class TripleStoreCannotCommitTransaction(Exception):
    """TripleStoreCannotCommitTransaction"""


class TripleStoreCannotRollbackTransaction(Exception):
    """TripleStoreCannotRollbackTransaction"""

class TripleStoreRollbackOccurred(Exception):
    """TripleStoreRollbackOccurred"""


class TripleStoreTerminatingError(Exception):
    """TripleStoreTerminatingError"""


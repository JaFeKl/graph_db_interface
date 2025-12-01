class GraphDBInterfaceError(Exception):
    """
    Base exception for the GraphDB interface.

    Serves as the common ancestor for all custom exceptions in this
    package to allow catching them collectively when desired.
    """

    pass


class InvalidRepositoryError(GraphDBInterfaceError):
    """
    Invalid repository configuration or selection.

    Args:
        message (str): Explanation of why the repository is invalid.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)


class AuthenticationError(GraphDBInterfaceError):
    """
    Authentication failure when communicating with GraphDB.

    Args:
        message (str): Details about the authentication error.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)


class InvalidQueryError(GraphDBInterfaceError):
    """
    Invalid SPARQL query or update string.

    Args:
        message (str): Description of why the query is invalid.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)


class InvalidInputError(GraphDBInterfaceError):
    """
    Invalid input provided to an interface method.

    Args:
        message (str): Explanation of the invalid input condition.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)


class InvalidIRIError(GraphDBInterfaceError):
    """
    Invalid IRI value or format encountered.

    Args:
        message (str): Description of the IRI validation failure.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)


class GraphDbException(GraphDBInterfaceError):
    """
    General error raised for HTTP or runtime issues with GraphDB.

    Args:
        message (str): The error message returned or constructed for the failure.
    """

    def __init__(
        self,
        message: str,
    ):
        self.message = message
        super().__init__(self.message)

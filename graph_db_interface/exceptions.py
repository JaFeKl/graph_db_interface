class GraphDBInterfaceError(Exception):
    """Base class for exceptions in this module."""

    pass


class InvalidQueryError(GraphDBInterfaceError):
    """Exception raised for invalid SPARQL queries."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class InvalidIRIError(GraphDBInterfaceError):
    """Exception raised for invalid IRIs."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

class GraphDBInterfaceError(Exception):
    """Base class for exceptions in this module."""

    pass


class InvalidRepositoryError(GraphDBInterfaceError):
    """Exception raised for invalid repository."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class AuthenticationError(GraphDBInterfaceError):
    """Exception raised for authentication errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class InvalidQueryError(GraphDBInterfaceError):
    """Exception raised for invalid SPARQL queries."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class InvalidInputError(GraphDBInterfaceError):
    """Exception raised for invalid input."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class InvalidIRIError(GraphDBInterfaceError):
    """Exception raised for invalid IRIs."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
        
        
class GraphDbException(GraphDBInterfaceError):
    """Exception raised for general GraphDB errors."""
    
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

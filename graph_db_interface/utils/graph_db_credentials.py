from dataclasses import dataclass
import os

@dataclass(frozen=True)
class GraphDBCredentials:
    """
    A class representing database credentials for connecting to a graph database.

    Attributes:
        host (str): The hostname or IP address of the database server.
        username (str): The username for authenticating with the database.
        password (str): The password for authenticating with the database.
        database_name (str): The name of the specific database to connect to.
        
    """
    base_url: str
    username: str
    password: str
    repository: str

    @classmethod
    def from_env(cls):
        '''
        Create a GraphDB instance using environment variables. The following environment variables must be set:
        - `GRAPHDB_USERNAME`: The username for GraphDB authentication.
        - `GRAPHDB_PASSWORD`: The password for GraphDB authentication.
        - `GRAPHDB_URL`: The base URL of the GraphDB instance.
        - `GRAPHDB_REPOSITORY`: The name of the GraphDB repository to use.

        Raises:
            ValueError: If any of the required environment variables are not set.
        '''

        if os.getenv("GRAPHDB_USERNAME") is None:
            raise ValueError("GRAPHDB_USERNAME environment variable is not set.")
        if os.getenv("GRAPHDB_PASSWORD") is None:
            raise ValueError("GRAPHDB_PASSWORD environment variable is not set.")
        if os.getenv("GRAPHDB_URL") is None:
            raise ValueError("GRAPHDB_URL environment variable is not set.")
        if os.getenv("GRAPHDB_REPOSITORY") is None:
            raise ValueError("GRAPHDB_REPOSITORY environment variable is not set.")

        username = os.getenv("GRAPHDB_USERNAME")
        password = os.getenv("GRAPHDB_PASSWORD")
        base_url = os.getenv("GRAPHDB_URL")
        repository = os.getenv("GRAPHDB_REPOSITORY")

        return cls(
            base_url=base_url,
            username=username,
            password=password,
            repository=repository
        )

    def __iter__(self):
        return iter((
            self.base_url,
            self.username,
            self.password,
            self.repository,
        ))
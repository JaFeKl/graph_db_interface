from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class GraphDBCredentials:
    """
    Immutable credentials for connecting to a GraphDB instance.

    Contains the base URL, user credentials, and target repository used by
    the client to authenticate and perform SPARQL operations.

    Args:
        base_url (str): Base URL of the GraphDB instance (e.g., "http://localhost:7200").
        username (str): Username for GraphDB authentication.
        password (str): Password for GraphDB authentication.
        repository (str): Repository identifier to target within the instance.
    """

    base_url: str
    username: str
    password: str
    repository: str

    @classmethod
    def from_env(cls) -> GraphDBCredentials:
        """
        Build credentials from environment variables.

        The following environment variables must be set: `GRAPHDB_USERNAME`,
        `GRAPHDB_PASSWORD`, `GRAPHDB_URL`, and `GRAPHDB_REPOSITORY`.

        Returns:
            GraphDBCredentials: A credentials instance populated from the environment.

        Raises:
            ValueError: If any of the required environment variables are missing.
        """

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
            repository=repository,
        )

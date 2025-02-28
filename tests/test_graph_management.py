import os
import json
from graph_db_interface.graph_db_interface import GraphDB
import graph_db_interface.utils.utils as utils


REQUIRED_ENV_VARS = [
    "GRAPHDB_URL",
    "GRAPHDB_USER",
    "GRAPHDB_PASSWORD",
    "GRAPHDB_REPOSITORY",
    "GRAPHDB_REPOSITORIES",
]
utils.check_env_vars(REQUIRED_ENV_VARS)

GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_USER = os.getenv("GRAPHDB_USER")
GRAPHDB_PASSWORD = os.getenv("GRAPHDB_PASSWORD")
GRAPHDB_REPOSITORIES = json.loads(os.getenv("GRAPHDB_REPOSITORIES"))
GRAPHDB_REPOSITORY = os.getenv("GRAPHDB_REPOSITORY")

db = GraphDB(
    base_url=GRAPHDB_URL,
    username=GRAPHDB_USER,
    password=GRAPHDB_PASSWORD,
    repository=GRAPHDB_REPOSITORY,
)


def test_get_repositories():
    repositories = db.get_list_of_repositories()
    ids = [repo["id"] for repo in repositories]  # get only the ids of each repo
    assert ids == GRAPHDB_REPOSITORIES  # compare to the expected repositories


def test_list_named_graphs():
    named_graphs = db.get_list_of_named_graphs()
    assert named_graphs is not None
    assert type(named_graphs == list)

from graph_db_interface.graph_db_interface import GraphDBinterface

interface = GraphDBinterface(username="test", password="test123")


def test_get_repositories():
    repositories = interface.get_list_of_repositories()
    ids = [repo["id"] for repo in repositories]  # get only the ids of each repo
    assert ids == ["Production", "Development", "Playground"]  # compare to the expected repositories


def test_list_named_graphs():
    named_graphs = interface.get_list_of_named_graphs()
    assert (named_graphs is not None)
    assert (type(named_graphs == list))

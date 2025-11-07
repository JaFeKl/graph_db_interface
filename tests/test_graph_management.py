from graph_db_interface import GraphDB


def test_get_repositories(db: GraphDB):
    _repositories = db.get_list_of_repositories()
    # check if repositories is not empty
    assert _repositories is not None
    assert type(_repositories) == list
    assert len(_repositories) > 0


def test_list_named_graphs(db: GraphDB):
    named_graphs = db.get_list_of_named_graphs()
    assert named_graphs is not None
    assert type(named_graphs == list)

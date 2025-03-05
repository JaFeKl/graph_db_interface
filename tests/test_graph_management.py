def test_get_repositories(db, repositories):
    _repositories = db.get_list_of_repositories()
    ids = [repo["id"] for repo in _repositories]  # get only the ids of each repo
    assert ids == repositories  # compare to the expected repositories


def test_list_named_graphs(db):
    named_graphs = db.get_list_of_named_graphs()
    assert named_graphs is not None
    assert type(named_graphs == list)

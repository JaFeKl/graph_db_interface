# graph_db_interface

[![PyPI version](https://badge.fury.io/py/graph-db-interface.svg)](https://badge.fury.io/py/graph-db-interface)
![Python](https://img.shields.io/badge/python-%3E=3.9-blue)
![License](https://img.shields.io/github/license/JaFeKl/graph_db_interface)


This repository acts as an interface to abstract SPARQL queries to callable methods to interact with a running GraphDB instance in an easier way.

# Installation


To install the current PyPI release, simply run the following command using your preferred python interpreter: 

```bash
pip install graph-db-interface
```

Or after cloning this repository you can also use poetry to install the package:
```bash
poetry install
```


# Getting Started
The package uses a single class named `GraphDB`. To use the interface, simply generate an object from this class:

```python
from graph_db_interface import GraphDB

credentials = GraphDBCredentials(
    base_url=<your_graph_db_url>,
    username=<your_graph_db_user>
    password=<your_graph_db_password>
    repository=<your_selected_repository_id>
)

myDB = GraphDB(
    credentials=credentials
)
```

# License

The package is licensed under the [MIT license](LICENSE).


# Acknowledgements
This package is developed as part of the INF subproject of the [CRC 1574: Circular Factory for the Perpetual Product](https://www.sfb1574.kit.edu/english/index.php). This work is therefore supported by the Deutsche Forschungsgemeinschaft (DFG, German Research Foundation) [grant-number: SFB-1574-471687386]

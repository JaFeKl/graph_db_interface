import logging
import json
from typing import TYPE_CHECKING, List, Dict, Optional
from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


class KafkaManager:
    """
    A manager for Kafka connectors in GraphDB.

    Reference:
    https://graphdb.ontotext.com/documentation/11.1/kafka-graphdb-connector.html
    """

    def __init__(self, db: "GraphDB"):
        self.db = db
        self.db.add_prefix("kafka", "<http://www.ontotext.com/connectors/kafka#>")
        self.db.add_prefix(
            "kafka-inst", "<http://www.ontotext.com/connectors/kafka/instance#>"
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("KafkaManager initialized")

    def get_existing_connector_ids(self) -> List[str]:
        """
        Get the IDs of existing Kafka connectors from the graph database.

        This method queries the graph database using SPARQL to retrieve all connector
        IDs that are registered in the system. It constructs a SELECT query that looks
        for resources with the kafka:listConnectors predicate.

        Args:
            None

        Returns:
            List[str]: A list of connector ID strings. Returns an empty list if no
                connectors are found in the database.
        """
        query = SPARQLQuery(prefixes=self.db.get_prefixes())
        query.add_select_block(
            variables=["?cntUri", "?cntStr"],
            where_clauses=["?cntUri kafka:listConnectors ?cntStr ."],
        )
        query_string = query.to_string(validate=True)
        results = self.db.query(query=query_string)
        return [res["cntStr"]["value"] for res in results["results"]["bindings"]]

    def get_status_of_connectors(
        self, id: Optional[str] = None
    ) -> Optional[Dict[str, Dict]]:
        """
        Get the status of Kafka connectors from the graph database.

        This method queries the graph database for connector status information. It can retrieve
        the status of all connectors or a specific connector if an ID is provided.

        Args:
            id (Optional[str], optional): The ID of a specific connector to query. If None,
                retrieves the status of all connectors. Defaults to None.

        Returns:
            Optional[Dict[str, Dict]]: A dictionary mapping connector names to their status
                information. Returns None if no connectors are found or the query returns no results.
        """
        query = SPARQLQuery(prefixes=self.db.get_prefixes())
        query.add_select_block(
            variables=["?cntUri", "?cntStr", "?cntStatus"],
            where_clauses=(
                ["?cntUri kafka:listConnectors ?cntStr ."]
                + [f"?cntUri kafka:connectorStatus ?cntStatus ."]
                if id is None
                else [f"kafka-inst:{id} kafka:connectorStatus ?cntStatus ."]
            ),
        )
        query_string = query.to_string(validate=True)
        results = self.db.query(query=query_string)
        if results["results"]["bindings"]:
            return {
                res["cntStr"]["value"]: res["cntStatus"]["value"]
                for res in results["results"]["bindings"]
            }
        else:
            return None

    def get_connector_create_options(self, id: str) -> Optional[Dict]:
        """
        Retrieve the create options for a Kafka connector from the graph database.

        This method queries the graph database to fetch the creation configuration string
        associated with a specific Kafka connector instance.

        Args:
            id (str): The identifier of the Kafka connector instance.

        Returns:
            Optional[Dict]: The create options string if found, None otherwise.
                           Note: Despite the return type hint suggesting Dict, this method
                           returns a string value from the query results or None.
        """
        query = SPARQLQuery(prefixes=self.db.get_prefixes())
        query.add_select_block(
            variables=["?createString"],
            where_clauses=[f"kafka-inst:{id} kafka:listOptionValues ?createString ."],
        )
        query_string = query.to_string(validate=True)
        results = self.db.query(query=query_string)
        if results["results"]["bindings"]:
            return results["results"]["bindings"][0]["createString"]["value"]
        return None

    def drop_connector(self, id: str) -> bool:
        """
        Drops the specified Kafka connector.

        Returns:
            bool: True if the connector was dropped successfully, False otherwise.
        """
        query = SPARQLQuery(prefixes=self.db.get_prefixes())
        query.add_insert_data_block(
            triples=[
                (f"kafka-inst:{id}", "kafka:dropConnector", "[]"),
            ]
        )
        query_string = query.to_string(validate=False)
        try:
            self.db.query(query=query_string, update=True)
            self.logger.info(f"Dropped Kafka connector with ID: {id}")
            return True
        except Exception as e:
            self.logger.error(
                f"Failed to drop Kafka connector with ID: {id}. Error: {e}"
            )
            return False

    def create_connector(
        self, id: str, connector_config: dict, overwrite: bool = False
    ):
        """
        Create a Kafka connector with the specified ID and configuration.

        This method creates a new Kafka connector by inserting the connector configuration
        into the graph database using a SPARQL INSERT DATA query. If a connector with the
        same ID already exists and overwrite is True, the existing connector will be dropped
        before creating the new one.

        Args:
            id (str): The unique identifier for the Kafka connector.
            connector_config (dict): A dictionary containing the configuration parameters
                for the Kafka connector. This will be serialized to JSON and stored in
                the database.
            overwrite (bool, optional): If True, drops any existing connector with the
                same ID before creating the new one. Defaults to False.

        Returns:
            None
        """
        if overwrite and id in self.get_existing_connector_ids():
            self.drop_connector(id)

        query = SPARQLQuery(prefixes=self.db.get_prefixes())
        query.add_insert_data_block(
            triples=[
                (
                    f"kafka-inst:{id}",
                    "kafka:createConnector",
                    f"'''{json.dumps(connector_config, indent=2)}'''",
                ),
            ]
        )
        query_string = query.to_string(validate=False)
        try:
            self.db.query(query=query_string, update=True)
            self.logger.info(f"Created Kafka connector with ID: {id}")
        except Exception as e:
            self.logger.error(
                f"Failed to create Kafka connector with ID: {id}. Error: {e}"
            )

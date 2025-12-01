import logging
import json
from typing import TYPE_CHECKING, List, Dict, Optional
from graph_db_interface.sparql_query import SPARQLQuery

if TYPE_CHECKING:
    from graph_db_interface import GraphDB


class KafkaManager:
    """
    Manage GraphDB Kafka connectors via SPARQL.

    Provides helpers to list, inspect, create, and drop Kafka connectors stored in
    GraphDB following Ontotext's connector ontology.

    Reference:
        https://graphdb.ontotext.com/documentation/11.1/kafka-graphdb-connector.html
    """

    def __init__(
        self,
        db: "GraphDB",
    ):
        self.db = db
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("KafkaManager initialized")

    def get_existing_connector_ids(self) -> List[str]:
        """
        Get the IDs of existing Kafka connectors.

        Queries the graph database for resources with the `kafka:listConnectors` predicate.

        Returns:
            List[str]: Connector IDs; empty when none are found.

        Raises:
            GraphDbException: If the underlying query execution fails.
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
        self,
        id: Optional[str] = None,
    ) -> Optional[Dict[str, Dict]]:
        """
        Get the status of Kafka connectors.

        When `id` is provided, returns the status of that connector; otherwise returns
        statuses for all connectors.

        Args:
            id (Optional[str]): Connector id to filter by. Defaults to None.

        Returns:
            Optional[Dict[str, Dict]]: Mapping of connector name to status; `None` when no results.

        Raises:
            GraphDbException: If the underlying query execution fails.
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

    def get_connector_create_options(
        self,
        id: str,
    ) -> Optional[str]:
        """
        Retrieve the creation options for a Kafka connector.

        Queries the graph database for the stored creation configuration string for the
        given connector instance.

        Args:
            id (str): The connector identifier.

        Returns:
            Optional[str]: The creation options string when available, otherwise `None`.

        Raises:
            GraphDbException: If the underlying query execution fails.
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

    def drop_connector(
        self,
        id: str,
    ) -> bool:
        """
        Drop the specified Kafka connector.

        Args:
            id (str): Connector identifier to drop.

        Returns:
            bool: True on success, False otherwise.
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
        self,
        id: str,
        connector_config: dict,
        overwrite: Optional[bool] = False,
    ) -> None:
        """
        Create a Kafka connector with the specified configuration.

        Inserts the connector configuration into GraphDB via an INSERT DATA query. If a
        connector with the same id exists and `overwrite=True`, it will be dropped first.

        Args:
            id (str): Unique connector identifier.
            connector_config (dict): Kafka connector configuration to serialize and store.
            overwrite (Optional[bool]): Drop any existing connector with the same id first. Defaults to False.
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

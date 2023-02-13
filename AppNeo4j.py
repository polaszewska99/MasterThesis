from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import pandas as pd


class AppNeo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def load_data_from_csv(self):
        """
        Loading data to db from csv file which contains parameters:
        "ID", "First Name", "Last Name", "genre", "orientation", "birthdate", "Country", "City",
        "Has_Characteristic", "Wants_Characteristic", "Hobbies"
        Single row is equivalent information about single person
        :return:
        """
        with self.driver.session() as session:
            session.write_transaction(
                self._load_data_from_csv
            )

    @staticmethod
    def _load_data_from_csv(tx):
        """
        Static method for loading basic data to db.
        :param tx: cursor for executing queries in database
        :return: result of query row by row
        """
        query = (
            """
            LOAD CSV WITH HEADERS FROM 'file:///person.csv' AS row
            MERGE (p:Person {first_name: row.`First Name`, last_name: row.`Last Name`, genre: row.genre,
            sexual_orientation: row.orientation, birthdate: row.birthdate})
            MERGE (c:City{name: row.City})
            MERGE (c2:Country{name: row.Country})
            MERGE (p)-[:LIVES_AT]->(c)
            MERGE (c)-[:LIES_IN]->(c2)
            WITH p, row
            UNWIND split(row.Hobbies, ':') AS hobby
            UNWIND split(row.Has_Characteristic, ':') AS char1
            UNWIND split(row.Wants_Characteristic, ':') AS char2
            MERGE (h:Hobby {name: hobby})
            MERGE(ch1:Characteristic {name: char1}) 
            MERGE (ch2:Characteristic {name:char2})
            MERGE (p)-[r:INTERESTED_IN]->(h)
            MERGE (ch2)<-[:WANTS_CHARACTERISTIC]-(p)-[:HAS_CHARACTERISTIC]->(ch1)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def hobbies_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._hobbies_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(h)]}".format(row=row),
                                    "{row[h.name]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['HobbyID', 'Name'])
            return df

    @staticmethod
    def _hobbies_to_df(tx):
        query = (
            """
            MATCH (h:Hobby)
            RETURN id(h), h.name
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def characteristics_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._characteristics_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(ch)]}".format(row=row),
                                    "{row[ch.name]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['CharacteristicID', 'Name'])
            return df

    @staticmethod
    def _characteristics_to_df(tx):
        query = (
            """
            MATCH (ch:Characteristic)
            RETURN id(ch), ch.name
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def persons_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._persons_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(p)]}".format(row=row),
                                    "{row[p.birthdate]}".format(row=row),
                                    "{row[p.first_name]}".format(row=row),
                                    "{row[p.genre]}".format(row=row),
                                    "{row[p.last_name]}".format(row=row),
                                    "{row[p.sexual_orientation]}".format(row=row),
                                    "{row[id(c)]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['PersonID', 'birthdate', 'first_name', 'genre',
                                                    'last_name', 'sexual_orientation', 'CityID'])
            return df

    @staticmethod
    def _persons_to_df(tx):
        query = (
            """
            MATCH (p:Person)-[:LIVES_AT]->(c:City)
            RETURN id(p), p.birthdate, p.first_name, p.genre, p.last_name, p.sexual_orientation, id(c)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def countries_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._countries_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(c)]}".format(row=row),
                                    "{row[c.name]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['CountryID', 'Name'])
            return df

    @staticmethod
    def _countries_to_df(tx):
        query = (
            """
            MATCH (c:Country)
            RETURN id(c), c.name
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def cities_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._cities_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(c)]}".format(row=row),
                                    "{row[c.name]}".format(row=row),
                                    "{row[id(c2)]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['CityID', 'Name', 'CountryID'])
            return df

    @staticmethod
    def _cities_to_df(tx):
        query = (
            """
            MATCH (c:City)-[:LIES_IN]->(c2:Country)
            RETURN id(c), c.name, id(c2)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def has_characteristic_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._has_characteristic_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(ch)]}".format(row=row),
                                    "{row[id(p)]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['CharacteristicID', 'PersonID'])
            return df

    @staticmethod
    def _has_characteristic_to_df(tx):
        query = (
            """
            MATCH (p:Person)-[:HAS_CHARACTERISTIC]->(ch:Characteristic)
            RETURN id(ch), id(p)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def wants_characteristic_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._wants_characteristic_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(ch)]}".format(row=row),
                                    "{row[id(p)]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['CharacteristicID', 'PersonID'])
            return df

    @staticmethod
    def _wants_characteristic_to_df(tx):
        query = (
            """
            MATCH (p:Person)-[:WANTS_CHARACTERISTIC]->(ch:Characteristic)
            RETURN id(ch), id(p)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def interested_to_df(self):
        with self.driver.session() as session:
            result = session.execute_read(
                self._interested_to_df
            )
            result_list = []
            for row in result:
                result_list.append(["{row[id(p)]}".format(row=row),
                                    "{row[id(h)]}".format(row=row)])
            df = pd.DataFrame(result_list, columns=['PersonID', 'HobbyID'])
            return df

    @staticmethod
    def _interested_to_df(tx):
        query = (
            """
            MATCH (p:Person)-[:INTERESTED_IN]->(h:Hobby)
            RETURN id(p), id(h)
            """
        )
        result = tx.run(query)
        try:
            return [row for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

import psutil
from faker import Faker
from faker.providers import BaseProvider
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import sqlite3
from time import process_time
import csv
import random
import string
import os
import pandas as pd
import re
import pypyodbc as odbc
from SQL_queries_variables import SQL_HOBBY_TABLE, TABLE_HOBBY_COLUMNS, SQL_CITY_TABLE, \
    TABLE_CITY_COLUMNS, SQL_COUNTRY_TABLE, TABLE_COUNTRY_COLUMNS, SQL_CHARACTERISTIC_TABLE,\
    TABLE_CHARACTERISTIC_COLUMNS


def limit_country_city(number_rows):
    worldcities_df = pd.read_csv("worldcities.csv",
                                 sep=',',
                                 names=["City", "Country"])
    country_df = worldcities_df["Country"]
    country_df = country_df.drop_duplicates()

    divide_num_city = 1
    if number_rows >= 1000:
        divide_num_city = 10
    if number_rows >= 100000:
        divide_num_city = 100
    elif number_rows <= 100:
        divide_num_city = number_rows
    else:
        divide_num_city = 10

    divide_num_countries = 1
    if number_rows <= 10000:
        divide_num_countries = number_rows
    elif 10000 >= number_rows <= 100000:
        divide_num_countries = int(number_rows / 10)
    elif 100000 >= number_rows <= 1000000:
        divide_num_countries = int(number_rows / 50)
    else:
        number_millions = int(number_rows / 1000000)
        divide_num_countries = 100 * number_millions

    num_cities = int(number_rows / divide_num_city)
    num_countries = int(number_rows / divide_num_countries)

    country_research = country_df.head(num_countries)

    worldcities_df_filtered = worldcities_df.query("Country in @country_research").head(num_cities)
    worldcities_df_filtered.to_csv("city_country_generate.csv", header=False, index=False)
    return "city_country_generate.csv"


class MyProvider(BaseProvider):
    __provider__ = "hobby"
    __provider__ = "characteristic"
    __provider__ = "genre"
    __provider__ = "orientation"
    __provider__ = "country_city"

    with open('hobbies.csv', newline='') as f:
        reader = csv.reader(f)
        hobbies = list(reader)

    with open('characteristics.csv', newline='') as f:
        reader = csv.reader(f)
        characteristics = list(reader)

    genres = ["Male", "Female"]
    sexual_orientations = ["heterosexual", "homosexual", "bisexual"]

    def hobby(self):
        h = str(self.random_element(self.hobbies))
        return h[2:len(h) - 2]

    def characteristic(self):
        h = str(self.random_element(self.characteristics))
        return h[2:len(h) - 2]

    def genre(self):
        return str(self.random_element(self.genres))

    def sexual_orientation(self):
        return str(self.random_element(self.sexual_orientations))

    def country_city(self):
        countries_cities = []
        with open('city_country_generate.csv', newline='\n') as f:
            reader = csv.reader(f)
            for row in reader:
                cc = row[0] + ':' + row[1]
                countries_cities.append(cc)
        return str(self.random_element(countries_cities))


class AppNeo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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
                self._cities_to_csv
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

class AppSQL:
    def __init__(self):
        """

        """
        self.con = odbc.connect('Driver={SQL Server};'
                                'Server=DESKTOP-NV2VHI6\SQLEXPRESS;'
                                'Database=ConnectPeople;'
                                'Trusted_Connection=yes;')
        self.cur = self.con.cursor()

    @staticmethod
    def load_data_from_df(cur, conn, df, columns_name, load_query_sql):
        for index, row in df.iterrows():
            cur.execute(load_query_sql, tuple([row[column] for column in columns_name]))
        conn.commit()

    def load_data_from_neo4j(self, df, columns, query):
        self.load_data_from_df(self.cur, self.con, df, columns, query)


def fake_person(number_rows):
    id_person = 0
    limit_country_city(number_rows)
    fake = Faker()
    fake.add_provider(MyProvider)

    with open('person.csv', 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if os.stat("person.csv").st_size == 0:
            writer.writerow(
                ["ID", "First Name", "Last Name", "genre", "orientation", "birthdate", "Country", "City",
                 "Has_Characteristic", "Wants_Characteristic", "Hobbies"])
        else:
            with open(
                    'C:\\Users\\alicj\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-1135eac1-af43-4af5-bcea-dcb29f1a1b3b'
                    '\\import',
                    'r') as f:
                last_line = f.readlines()[-1]
            result = re.split(',', last_line)
            id_person = int(result[0]) + 1

        for _ in range(number_rows):
            hobbies = ""
            characteristics_has = ""
            characteristics_want = ""
            iterator_hobby = 0
            iterator_ch_has = 0
            iterator_ch_wants = 0
            hobbies_amount = random.randint(1, 5)
            ch_has_amount = random.randint(1, 8)
            ch_want_amount = random.randint(1, 10)
            for _ in range(hobbies_amount):
                if iterator_hobby > 0:
                    hobbies = hobbies + ":" + str(fake.hobby())
                else:
                    hobbies = str(fake.hobby())
                iterator_hobby = iterator_hobby + 1

            for _ in range(ch_has_amount):
                if iterator_ch_has > 0:
                    characteristics_has = characteristics_has + ":" + str(fake.characteristic())
                else:
                    characteristics_has = str(fake.characteristic())
                iterator_ch_has = iterator_ch_has + 1

            for _ in range(ch_want_amount):
                if iterator_ch_wants > 0:
                    characteristics_want = characteristics_want + ":" + str(fake.characteristic())
                else:
                    characteristics_want = str(fake.characteristic())
                iterator_ch_wants = iterator_ch_wants + 1

            genre = fake.genre()
            country_and_city = fake.country_city()
            country = country_and_city[country_and_city.find(':') + 1:len(country_and_city)]
            city = country_and_city[0:country_and_city.find(':')]

            writer.writerow([id_person, fake.first_name_female() if genre == 'Female' else fake.first_name_male(),
                             fake.last_name(), genre, fake.sexual_orientation(),
                             fake.date_of_birth(minimum_age=18, maximum_age=70), country, city,
                             characteristics_has, characteristics_want, hobbies])
            id_person = id_person + 1


if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "withoutpswd"
    app_neo4j = AppNeo4j(uri, user, password)

    characteristics = app_neo4j.characteristics_to_df()
    app_sql = AppSQL()
    app_sql.load_data_from_neo4j(characteristics, TABLE_CHARACTERISTIC_COLUMNS, SQL_CHARACTERISTIC_TABLE)

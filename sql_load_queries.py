"""
Loading data equivalent to Neo4j state from basic csv file.
[csv file with all information about generated fake persons] -> [Neo4j loading query] -> [SQl queries to load data]*
*(first loading data from Neo4j, next write them to DataFrame and finally load to sql tables)
"""
SQL_HOBBY_TABLE = '''
                    SET IDENTITY_INSERT Hobbies ON
                    INSERT INTO Hobbies (HobbyID, Name)
                    VALUES (?, ?)
                    SET IDENTITY_INSERT Hobbies OFF
                    '''
TABLE_HOBBY_COLUMNS = ['HobbyID', 'Name']
SQL_CHARACTERISTIC_TABLE = '''
                    SET IDENTITY_INSERT Characteristics ON
                    INSERT INTO Characteristics (CharacteristicID, Name)
                    VALUES (?, ?)
                    SET IDENTITY_INSERT Characteristics OFF
                    '''
TABLE_CHARACTERISTIC_COLUMNS = ['CharacteristicID', 'Name']
SQL_CITY_TABLE = '''
                    INSERT INTO Cities (CityID, Name, CountryID)
                    VALUES (?, ?, ?)
                    '''
TABLE_CITY_COLUMNS = ['CityID', 'Name', 'CountryID']
SQL_COUNTRY_TABLE = '''
                    INSERT INTO Countries (CountryID, Name)
                    VALUES (?, ?)
                    '''
TABLE_COUNTRY_COLUMNS = ['CountryID', 'Name']
SQL_PERSON_TABLE = '''
                    SET IDENTITY_INSERT Persons ON
                    INSERT INTO Persons (PersonID, birthdate, first_name, genre, last_name, sexual_orientation, CityID)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    SET IDENTITY_INSERT Persons OFF
                    '''
TABLE_PERSON_COLUMNS = ['PersonID', 'birthdate', 'first_name', 'genre',
                        'last_name', 'sexual_orientation', 'CityID']
SQL_HAS_CHARACTERISTIC_TABLE = '''
                                SET IDENTITY_INSERT Has_Characteristic ON
                                INSERT INTO Has_Characteristic (CharacteristicID, PersonID)
                                VALUES (?, ?)
                                SET IDENTITY_INSERT Has_Characteristic OFF
                                '''
TABLE_HAS_CHARACTERISTIC = ['CharacteristicID', 'PersonID']
SQL_WANTS_CHARACTERISTIC_TABLE = '''
                                SET IDENTITY_INSERT Wants_Characteristic ON
                                INSERT INTO Wants_Characteristic (CharacteristicID, PersonID)
                                VALUES (?, ?)
                                SET IDENTITY_INSERT Wants_Characteristic OFF
                                '''
TABLE_WANTS_CHARACTERISTIC = ['CharacteristicID', 'PersonID']
SQL_INTERESTED_IN_TABLE = '''
                                SET IDENTITY_INSERT Interested_in ON
                                INSERT INTO Interested_in (PersonID, HobbyID)
                                VALUES (?, ?)
                                SET IDENTITY_INSERT Interested_in OFF
                                '''
TABLE_INTERESTED_IN = ['PersonID', 'HobbyID']


def load_all_data_model_from_neo4j_to_sql(app_neo4j, app_sql):
    """
    Load data from Neo4j nodes and relationships to sql tables according to database diagrams
    :param app_neo4j: object of AppNeo4j class
    :param app_sql: object of AppSQL class
    :return: void
    """
    # Dataframes
    countries = app_neo4j.countries_to_df()
    cities = app_neo4j.cities_to_df()
    hobbies = app_neo4j.hobbies_to_df()
    characteristics = app_neo4j.characteristics_to_df()
    persons = app_neo4j.persons_to_df()
    interested_in = app_neo4j.interested_to_df()
    has_characteristic = app_neo4j.has_characteristic_to_df()
    wants_characteristic = app_neo4j.has_characteristic_to_df()
    # Load dataframes to tables
    app_sql.load_data_from_neo4j(countries, TABLE_COUNTRY_COLUMNS, SQL_COUNTRY_TABLE)
    app_sql.load_data_from_neo4j(cities, TABLE_CITY_COLUMNS, SQL_CITY_TABLE)
    app_sql.load_data_from_neo4j(hobbies, TABLE_HOBBY_COLUMNS, SQL_HOBBY_TABLE)
    app_sql.load_data_from_neo4j(characteristics, TABLE_CHARACTERISTIC_COLUMNS, SQL_CHARACTERISTIC_TABLE)
    app_sql.load_data_from_neo4j(persons, TABLE_PERSON_COLUMNS, SQL_PERSON_TABLE)
    app_sql.load_data_from_neo4j(interested_in, TABLE_INTERESTED_IN, SQL_INTERESTED_IN_TABLE)
    app_sql.load_data_from_neo4j(has_characteristic, TABLE_HAS_CHARACTERISTIC, SQL_HAS_CHARACTERISTIC_TABLE)
    app_sql.load_data_from_neo4j(wants_characteristic, TABLE_WANTS_CHARACTERISTIC, SQL_WANTS_CHARACTERISTIC_TABLE)

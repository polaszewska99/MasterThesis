from AppSQL import AppSQL
from AppNeo4j import AppNeo4j
from sql_queries_variables import SQL_HOBBY_TABLE, TABLE_HOBBY_COLUMNS, SQL_CITY_TABLE, \
    TABLE_CITY_COLUMNS, SQL_COUNTRY_TABLE, TABLE_COUNTRY_COLUMNS, SQL_CHARACTERISTIC_TABLE, \
    TABLE_CHARACTERISTIC_COLUMNS, TABLE_PERSON_COLUMNS, SQL_PERSON_TABLE

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "withoutpswd"
    app_neo4j = AppNeo4j(uri, user, password)

    persons = app_neo4j.persons_to_df()
    app_sql = AppSQL()
    app_sql.load_data_from_neo4j(persons, TABLE_PERSON_COLUMNS, SQL_PERSON_TABLE)

    # characteristics = app_neo4j.characteristics_to_df()
    # app_sql = AppSQL()
    # app_sql.load_data_from_neo4j(characteristics, TABLE_CHARACTERISTIC_COLUMNS, SQL_CHARACTERISTIC_TABLE)

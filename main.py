from AppSQL import AppSQL
from AppNeo4j import AppNeo4j
from sql_load_queries import load_all_data_model_from_neo4j_to_sql

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "withoutpswd"
    app_neo4j = AppNeo4j(uri, user, password)
    server = 'DESKTOP-NV2VHI6\SQLEXPRESS'
    database = 'ConnectPeople'
    app_sql = AppSQL(server, database)
    load_all_data_model_from_neo4j_to_sql(app_neo4j, app_sql)

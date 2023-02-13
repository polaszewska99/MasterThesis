from AppSQL import AppSQL
from AppNeo4j import AppNeo4j
from sql_load_queries import load_all_data_model_from_neo4j_to_sql
from data_generator import fake_person

# Initial variables for Neo4j connect
uri = "bolt://localhost:7687"
user = "neo4j"
password = "withoutpswd"
# Initial variables for SQL Server connect
server = 'DESKTOP-NV2VHI6\SQLEXPRESS'
database = 'ConnectPeople'


if __name__ == "__main__":
    # Initialize connection for Neo4j
    app_neo4j = AppNeo4j(uri, user, password)
    # Generate file with fake data
    # fake_person(10000)
    # Load fake data to neo4j
    # app_neo4j.load_data_from_csv()
    # Initialize connection for SQL Server
    app_sql = AppSQL(server, database)
    # Clear and load fresh data from Neo4j to SQL Server
    #app_sql.clear_database()
    #load_all_data_model_from_neo4j_to_sql(app_neo4j, app_sql)





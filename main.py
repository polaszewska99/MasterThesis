from AppSQL import AppSQL
from AppNeo4j import AppNeo4j
from sql_load_queries import load_all_data_model_from_neo4j_to_sql
import psutil
from measure_rate import fn_cpu_memory_usage, fn_timer


# Initial variables for Neo4j connect
uri = "bolt://localhost:7687"
user = "neo4j"
password = "withoutpswd"
# Initial variables for SQL Server connect
server = 'DESKTOP-NV2VHI6\SQLEXPRESS'
database = 'ConnectPeople'

@fn_timer
@fn_cpu_memory_usage
def test_func():
    for i in range(10000000):
        i+=1



if __name__ == "__main__":
    #app_neo4j = AppNeo4j(uri, user, password)
    app_sql = AppSQL(server, database)
    #load_all_data_model_from_neo4j_to_sql(app_neo4j, app_sql)
    # app_sql.test_select()
    test_func()



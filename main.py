from AppSQL import AppSQL
from AppNeo4j import AppNeo4j
from sql_load_queries import load_all_data_model_from_neo4j_to_sql
from sql_load_queries import *
from data_generator import fake_person
from neo4j_test_queries import *
from mssql_test_queries import *
import time
import timeit

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
    '''Generate file with fake data'''
    #t = time.time()
    #fake_person(200000)
    '''Load fake data to neo4j'''
    # files = []
    # for i in range(3, 51):
    #     name = 'file:///person_500k-'
    #     name += str(i) + '.csv'
    #     files.append(name)
    # print(files)
    # for file in files:
    #     app_neo4j.load_data_from_csv(file)
    # file = 'file:///person.csv'
    # app_neo4j.load_data_from_csv(file)
    # t2 = time.time()
    # print("time: ", t2 - t)
    ''' Initialize connection for SQL Server'''
    #app_sql = AppSQL(server, database)
    '''
    Clear and load fresh data from Neo4j to SQL Server
    '''
    # app_sql.clear_database()
    #load_all_data_model_from_neo4j_to_sql(app_neo4j, app_sql)
    '''
    Quering for selecting information
    '''
    # for x in range(5):
    #     app_sql.test_select(SQL_NUMBER_WOMEN_FOR_CITIES)
    #     # app_neo4j.test_query(NEO4J_NUMBER_WOMEN_FOR_CITIES)
    # for x in range(5):
    #     #app_sql.test_select(SQL_NUMBER_WOMEN_FOR_CITIES)
    #     app_neo4j.test_query(NEO4J_NUMBER_WOMEN_FOR_CITIES)
    # for x in range(10):
    #     app_sql.test_select(SQL_GROUP_BY_ORIENTATION)
    # for x in range(10):
    #     app_neo4j.test_query(NEO4J_GROUP_BY_ORIENTATION)
    ''' ADD AGE '''
    #app_sql.back_changes(SQL_DELETE_AGE)
    # app_sql.back_changes(SQL_ADD_AGE)
    # app_neo4j.remove_last_change_for_test(NEO4J_ADD_AGE)
    # app_neo4j.remove_last_change_for_test(NEO4J_REMOVE_AGE)
    # for x in range(10):
    #     # app_sql.test_without_select(SQL_ADD_AGE)
    #     # app_sql.back_changes(SQL_DELETE_AGE)
    #     app_neo4j.test_query_write(NEO4J_ADD_AGE)
    #     app_neo4j.remove_last_change_for_test(NEO4J_REMOVE_AGE)
    '''LAST NAME UPPER CASE'''
    # for x in range(5):
    #     app_sql.test_without_select(SQL_MODIFY_LAST_NAME_UPPER_CASE)
    #     app_sql.test_without_select(SQL_MODIFY_LAST_NAME_LOWER_CASE)
        # app_neo4j.test_query_write(NEO4J_MODIFY_LAST_NAME_UPPER_CASE)
        # app_neo4j.test_query_write(NEO4J_MODIFY_LAST_NAME_LOWER_CASE)

    '''SELECT FOR INDEXING'''
    # for x in range(10):
    #     app_neo4j.test_query(NEO4J_SELECT_FOR_INDEXING)
        #app_sql.test_select(SQL_SELECT_FOR_INDEXING)
    '''MATCH PERSONS'''
    # for x in range(2):
    #     app_sql.back_changes(SQL_DELETE_FROM_MATCHES)
    #     app_sql.test_without_select(SQL_MATCH_PERSONS)
    #
    # for x in range(2):
    #     app_neo4j.remove_last_change_for_test(NEO4J_REMOVE_MATCHES)
    #     app_neo4j.test_query_write(NEO4J_MATCH_PERSONS)

    '''PERSONS2'''
    # for x in range(3):
    #     app_sql.back_changes(SQL_CLEAR_PERSONS2)
    #     app_sql.test_without_select(SQL_COPY_DATA)
    #
    # for x in range(3):
    #     app_neo4j.remove_last_change_for_test(NEO4J_DELETE_PERSON2)
    #     app_neo4j.test_query_write(NEO4J_COPY_DATA)

    '''
        STOP
        Backup
    '''

    '''DELETING'''
    #app_neo4j.test_query_write(NEO4J_DELETE_PERSONS_BY_LETTER)
    #app_sql.test_without_select(SQL_DELETE_PERSONS_BY_LETTER)

    '''LOAD PERSONS (bulk insert doesn't work properly'''
    # t1 = time.time()
    # persons = app_neo4j.persons_to_df()
    # app_sql.load_data_from_neo4j(persons, TABLE_PERSON_COLUMNS, SQL_PERSON_TABLE)
    # t2 = time.time()
    # print(t2-t1)


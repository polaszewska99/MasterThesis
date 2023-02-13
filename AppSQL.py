import pypyodbc as odbc
from measure_rate import *

class AppSQL:
    def __init__(self, server, database):
        """

        """
        self.con = odbc.connect('Driver={SQL Server};'
                                'Server=%s;'
                                'Database=%s;'
                                'Trusted_Connection=yes;'
                                % (server, database))
        self.cur = self.con.cursor()

    @staticmethod
    def load_data_from_df(cur, conn, df, columns_name, load_query_sql):
        for index, row in df.iterrows():
            cur.execute(load_query_sql, tuple([row[column] for column in columns_name]))
        conn.commit()

    def load_data_from_neo4j(self, df, columns, query):
        self.load_data_from_df(self.cur, self.con, df, columns, query)

    @fn_timer
    @fn_cpu_memory_usage
    def test_select(self):
        self.cur.execute('''SELECT * FROM Characteristics''')

    def clear_database(self):
        self.cur.execute('''DELETE FROM Persons;
                            DELETE FROM Cities;
                            DELETE FROM Countries;
                            DELETE FROM Hobbies;
                            DELETE FROM Characteristics;
                            DELETE FROM Has_Characteristic;
                            DELETE FROM Wants_Characteristic;
                            DELETE FROM Interested_in;
                            DELETE FROM Matches;
                        ''')

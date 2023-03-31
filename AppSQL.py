import pypyodbc as odbc
from measure_rate import *


class AppSQL:
    def __init__(self, server, database):
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
    def test_select(self, query):
        self.cur.execute(query)
        self.cur.fetchall()

    @fn_timer
    def test_without_select(self, query):
        self.cur.execute(query)
        self.cur.commit()

    def back_changes(self, query):
        self.cur.execute(query)
        self.cur.commit()

    def clear_database(self) -> object:
        self.cur.execute('''
                            DELETE FROM Has_Characteristic;
                            DELETE FROM Wants_Characteristic;
                            DELETE FROM Characteristics;
							DELETE FROM Interested_in;
                            DELETE FROM Hobbies;
                            DELETE FROM Matches;
                            DELETE FROM Persons;
                            DELETE FROM Persons2;
							DELETE FROM Cities;
                            DELETE FROM Countries;
                        ''')

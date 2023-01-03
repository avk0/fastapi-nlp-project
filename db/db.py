## module for DB initialization and with SQL queries wrapped in a class with methods: 
"""
class DatabaseConnection(object):

    def __enter__(self):
        # make a database connection and return it
        ...
        return self.dbconn

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.dbconn.close()
        ...
        
        
    with DatabaseConnection() as mydbconn:
        # do stuff


import MySQLdb

class Database_Test(object):
    def __init__(self, db_local):
        self.db_local = db_local
        self.db_conn = None
        self.db_cursor = None

    def __enter__(self):
        # This ensure, whenever an object is created using "with"
        # this magic method is called, where you can create the connection.
        self.db_conn = MySQLdb.connect(**self.db_local)
        self.db_cursor = self.db_conn.cursor()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        # once the with block is over, the __exit__ method would be called
        # with that, you close the connnection
        try:
           self.db_cursor.close()
           self.db_conn.close()
        except AttributeError: # isn't closable
           print 'Not closable.'
           return True # exception handled successfully

    def get_row(self, sql, data = None):
        self.db_cursor.execute(sql)
        self.resultset = self.db_cursor.fetchall()
        return self.resultset

db_config =  {
            'host':"127.0.0.1",                 # database host
            'port': 3306,                       # port
            'user':"root",                      # username
            'passwd':"admin",                   # password
            'db':"test",                        # database
            'charset':'utf8'                    # charset encoding
            }


sql = "SELECT * FROM mytest LIMIT 10" 

with Database_Test(db_config) as test:
    resultSet = test.get_row(sql)
    print(resultSet)

c.execute('create database if not exists pythontest')
""" 
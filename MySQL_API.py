"""
This file contains all the DB APIs.
class MySQLConnection for writing,
function exists_in_db for querying
"""

import getpass
from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker



class MySQLConnection:
    def __init__(self,user,hostname,db,):
        self.user = str(user)
        self.hostname = str(hostname)
        self.db = str(db)
        pword = getpass.getpass("Enter password for user {}".format(user))
        self.engine = create_engine("mysql://{}:{}@{}/{}".format(user,pword,hostname,db))
        # Writing logic in ObjectLogic.py depends on this being named cnx:
        self.cnx = self.engine.connect()
    
    def write_to_db(self, df, table_name):
        try:
            df.to_sql(table_name,con = self.cnx,if_exists='append',index=False)
        except Exception as e:
            print("\n SQL Write error with: ")
            print(df,"\n", e)


"""
# Redundant function. Functionality implemented through Player and Referee class methods.

def exists_in_db(type,conn,name=None,surname=None,nr=None):

        Function returns boolean to determine whether or not given param. combination already in exists
        in the database table

        boolean = False
        if type == "player":
                query = "Select player_index FROM Players WHERE Nr.={}".format(nr)
                returned = "" #query result
                if returned is not None:
                        boolean = True
        if type == "referee":
                query = "Select referee_index FROM Referees WHERE name={} AND surname = {}".format(name,surname)
                returned = "" #query result
                if returned is not None:
                        boolean = True

        return boolean
"""


"""
# The SQL Alchemy way.
# https://docs.sqlalchemy.org/en/latest/orm/query.html
# https://docs.sqlalchemy.org/en/latest/orm/session_api.html#sqlalchemy.orm.session.Session
# https://docs.sqlalchemy.org/en/latest/orm/session_api.html#sqlalchemy.orm.session.Session
# https://docs.sqlalchemy.org/en/latest/orm/session_basics.html
# On hold for now because the query syntax is terrible

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()

# work with sess
myobject = MyObject('foo', 'bar')
session.add(myobject)
session.commit()
"""

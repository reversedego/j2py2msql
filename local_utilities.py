import pandas as pd
import numpy
def seconds(timestring):
        mins, secs = timestring.split(":")
        time = int(mins) * 60 + int(secs)
        return int(time)

def get_index(SQL_table_column,SQL_Table, SQL):
        """
        This utility gets the maximum index of entries in the table.
        Usable for all tables and classes.
        TODO: Check if all appearences in ObjectLogic have the +1
        """
        query = "Select Max({}) from {}".format(SQL_table_column, SQL_Table)
        result = pd.read_sql(query, SQL.cnx)['Max({})'.format(SQL_table_column)][0]
        # Handling the first time something is read.
        if result == None:
                return 0
        return result

def goal_time(goals):
        """
        This function gets the time of the last goal, given a list or dict of goals
        """
        print(goals)
        if type(goals) == dict:
                return seconds(goals['Laiks'])
        elif type(goals) == list:
                return seconds(goals[-1]['Laiks'])
        else:
                print("Somethig broke in the processing of {} in the function goal_time", goals)
        
        
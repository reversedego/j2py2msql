"""
Classes need to be taking MySQLConnection instance
find_player needs to have MySQLConnectionInstance.cnx assigned to it
Any queries involving strings need to be put in ''
"""
# Dependancy imports:
import json
import pandas as pd
import numpy
# from typing import List

# Local imports:
from local_utilities import seconds, get_index


class Referee:
    """
    Class exists to create unique entries in MySQL 'Referee' table and index them accordingly
    """
    referees = [] # type: List[Referee]
    db_schema = ['referee_index','Vards', 'Uzvards']
    def __init__(self,ref_dict,SQL):
        self.nm = ref_dict['Vards']
        self.sn = ref_dict['Uzvards']
        self.index = get_index('referee_index', 'Referees', SQL) + 1
        self.df = pd.DataFrame([[self.index, 
                                self.nm, 
                                self.sn]], columns = Referee.db_schema)
        self.SQL = SQL
        found_referee = Referee.find_referee_from_db(name = self.nm, 
                                                    surname = self.sn,
                                                    SQL=self.SQL)
        if found_referee == 0:
            self.SQL.write_to_db(self.df,'Referees')
            Referee.referees.append(self)

    @classmethod
    def find_from_instances(cls,name, surname, SQL):
        """
        Class method parses the list of refs unique in this session,
        then attempts to parse the DB if not found
        Takes name and surname as search parameters
        Returns the referee index
        """
        for ref in cls.referees:
            if (ref.nm == name and ref.sn == surname):
                return ref.index
        return Referee.find_referee_from_db(name,surname,SQL = SQL)
    
    @staticmethod
    def find_referee_from_db(name,surname,SQL):
        """
        Returns referee index when searched by name and surname from the table 'Referees'
        """
        query = "SELECT referee_index FROM Referees WHERE Vards='{}' AND Uzvards='{}'".format(name,surname)
        result = pd.read_sql(query,SQL.cnx)
        # If there is ONE entry found:
        if type(result) == numpy.int64:
            return result
        # In the case of SEVERAL or NONE:
        elif type(result) == pd.core.frame.DataFrame:
            # In case of NONE:
            if len(result) == 0:
                return 0
            # In case of SEVERAL
            elif len(result) == 1:
                return result.to_dict()['referee_index'][0]
            else:
                print("Referee entry duplication detected, name = {}, surname = {}".format(name,surname))
        else:
            raise ValueError
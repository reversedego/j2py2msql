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



class Player:
    """
    Class exists to create unique entries in MySQL 'Players' table and index them accordingly
    """
    players = [] # type: List[Player]
    db_schema = ['player_index','Komandas_Nosaukums', 'Loma', 'Nr', 'Uzvards', 'Vards' ]
    def __init__(self,team_name,player_dict,SQL):
        self.name = player_dict['Vards']
        self.surname = player_dict['Uzvards']
        self.nr = player_dict['Nr']
        self.team = str(team_name)
        self.role = player_dict['Loma']
        self.index = get_index('player_index', 'Players',SQL) + 1
        self.df = pd.DataFrame([[self.index, 
                                self.team, 
                                self.role,
                                self.nr,
                                self.surname,
                                self.name]], columns = Player.db_schema)
        self.SQL = SQL
        found_player = Player.find_player_from_db(team = self.team, 
                                                nr = self.nr,
                                                SQL=self.SQL)
        if found_player == 0:
            self.SQL.write_to_db(self.df,'Players')
            # print(found_player)
            Player.players.append(self)
    
    @classmethod
    def find_from_instances(cls, team, nr, SQL):
        """
        Class method parses the list of players unique in this session,
        then attempts to parse the DB if not found
        Takes team name and player number as search parameters
        Returns the player index.
        """
        for player in cls.players:
            if player.team == team and player.nr == nr:
                return player.index
                # TODO: Test. There may be inconsistencies with what is returned from find_from_inst and find_from_db.
                # One is the index from MySQL the other is instance attribute
        return Player.find_player_from_db(team,nr, SQL = SQL)
    
    @staticmethod
    def find_player_from_db(team, nr, SQL):
        """
        Function that returns the index of a player from the MySQL table Players
        TODO: Find out why can one entry generate both an integer and a DataFrame.
        """
        query = "SELECT player_index FROM Players WHERE Komandas_Nosaukums='{}' AND Nr='{}'".format(team,nr)
        result = pd.read_sql(query,SQL.cnx)
        # If there is ONE entry found:
        if type(result) == numpy.int64:
            print("there is ONE entry found:")
            return result
        # In the case of SEVERAL or NONE:
        elif type(result) == pd.core.frame.DataFrame:
            # print("In the case of SEVERAL or NONE")
            # In case of NONE:
            if len(result) == 0:
                # print("NONE")
                return 0
            # In case of ONE, but a seperate case for some reason
            elif len(result) == 1:
                return result.to_dict()['player_index'][0]
            elif len(result >2):
                print("SEVERAL entries detected for, team = {}, nr = {}".format(team, nr))
        else:
            raise ValueError
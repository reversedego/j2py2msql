"""
Classes need to be taking MySQLConnection instance
find_player needs to have MySQLConnectionInstance.cnx assigned to it
Any queries involving strings need to be put in ''
"""
# Dependancy imports:
import json
import pandas as pd
# from typing import List

# Local imports:
from local_utilities import seconds, get_index, goal_time
from Player import Player
from Referee import Referee

class Game:
    """
    This class uses indexed players and refs to update and create entries for these tables:
    Goals, Penalties, Substitutions and Games
    CONTAINS ALL THE RELEVANT LOGIC FOR WRITING IN THE TABLES
    TODO: Implement playedtime calculations and logic
    """
    goal_db_schema = ['goal_index','game_index','Vartu_Guvejs_index', 'Laiks', 'Sitiena_Veids', 'Asistejoss_Speletajs_1_index', 'Asistejoss_Speletajs_2_index','Asistejoss_Speletajs_3_index','Asistejoss_Speletajs_4_index' ]
    penalty_db_schema = ['penalty_index','game_index','Soda_Laiks', 'player_index' ]
    substitutions_db_schema = ['substitution_index','game_index','Laiks','Noiet_index','Uziet_index']
    game_db_schema = ['game_index','Speles_Vieta','Speles_Laiks','Skatitaji','1_Komandas_Nosaukums','2_Komandas_Nosaukums', 'VT_index', 'LT1_index', 'LT2_index','1_Komandas_Punkti','2_Komandas_Punkti']
    def __init__(self, game_dict, SQL):
        self.index = get_index('game_index', 'Games', SQL) + 1
        self.time = str(game_dict['Laiks'])
        self.spectators = game_dict['Skatitaji']
        self.location = game_dict['Vieta']
        self.sen_ref = game_dict['VT']
        self.ref1 = game_dict['T'][0]
        self.ref2 = game_dict['T'][1]
        self.team1 = game_dict['Komanda'][0]['Nosaukums']
        self.team2 = game_dict['Komanda'][1]['Nosaukums']
        self.team1_goals = game_dict['Komanda'][0]['Varti']
        self.team2_goals = game_dict['Komanda'][1]['Varti']
        self.team1_penalties = game_dict['Komanda'][0]['Sodi']
        self.team2_penalties = game_dict['Komanda'][1]['Sodi']
        self.team1_subs = game_dict['Komanda'][0]['Mainas']
        self.team2_subs = game_dict['Komanda'][1]['Mainas']
        self.SQL = SQL
    def add_goals(self,goals, team_name):
        """
        Iterates through all the goals for a given team in the json,
        adds all the goals to the MySQL table 'Goals'.
        WARNING!The function does not support more than 4 assisting players
        # TODO: Check how pd.Dataframe handles None or 'None' while writing to MySQL
        # TODO: Find out how no assisting players is expressed
        """
        
        # Catch no goals scored:
        try:
            goals['VG']
        except TypeError:
            return None
        # IN CASE OF A SINGLE GOAL, the goals['VG'] is a dict and cannot be looped over with for.
        if type(goals['VG']) == dict:
            assisting_players = [0,0,0,0]
            goal = goals['VG']
            scorer = Player.find_from_instances(team_name,goal['Nr'], self.SQL)
            time = seconds(goal['Laiks'])
            shot_type = goal['Sitiens']
            i = 0
            # If there are NO ASSISTING players:
            if not('P' in goal.keys()):
                pass
            # If there is a SINGLE ASSISTING player:
            elif type(goal['P']) == dict:
                assisting_players[0] = Player.find_from_instances(team_name, goal['Nr'],self.SQL)
            # If there are MULTIPLE ASSISTING players:
            elif type(goal['P']) == list:
                for assisting_player in goal['P']:
                    assisting_players[i] = Player.find_from_instances(team_name, assisting_player['Nr'], self.SQL)
                    i+=1
            else:
                print("something broke with assisting players for goal ",goal," by teams ",self.team1," ", self.team2)
            goal_index = get_index('goal_index', 'Goals', self.SQL) + 1
            df = pd.DataFrame([[goal_index,
                                self.index,
                                scorer, 
                                time, 
                                shot_type, 
                                assisting_players[0], 
                                assisting_players[1], 
                                assisting_players[2], 
                                assisting_players[3]]], columns = Game.goal_db_schema)
            self.SQL.write_to_db(df, 'Goals')
            # AND FOR THE REST OF THE GOALS
        elif type(goals['VG']) == list:
            for goal in goals['VG']:
                assisting_players = [0,0,0,0]
                scorer = Player.find_from_instances(team_name,goal['Nr'], self.SQL)
                time = seconds(goal['Laiks'])
                shot_type = goal['Sitiens']
                i = 0
                # If there are NO ASSISTING players:
                if not('P' in goal.keys()):
                    pass
                # If there is a SINGLE ASSISTING player:
                elif type(goal['P']) == dict:
                    assisting_players[0] = Player.find_from_instances(team_name, goal['Nr'],self.SQL)
                # If there are MULTIPLE ASSISTING players:
                elif type(goal['P']) == list:
                    for assisting_player in goal['P']:
                        assisting_players[i] = Player.find_from_instances(team_name, assisting_player['Nr'], self.SQL)
                        i+=1
                else:
                    print("something broke with assisting players for goal ",goal," by teams ",self.team1," ", self.team2)
                goal_index = get_index('goal_index', 'Goals', self.SQL) + 1
                df = pd.DataFrame([[goal_index,
                                    self.index,
                                    scorer, 
                                    time, 
                                    shot_type, 
                                    assisting_players[0], 
                                    assisting_players[1], 
                                    assisting_players[2], 
                                    assisting_players[3]]], columns = Game.goal_db_schema)
                self.SQL.write_to_db(df, 'Goals')
    def add_penalties(self,penalties, team_name):
        """
        Iterates through all the penalties for a given team in the json,
        writes the penalties to the MySQL table 'Penalties'
        TODO: penalty index from get_index
        """
        # Catch empty lists of penalties.
        try:
            penalties['Sods']
        except TypeError:
            return None
        # In the case of a SINGLE penalty across the span of the game:
        if type(penalties['Sods']) == dict:
            penalty = penalties['Sods']
            penalized_player = Player.find_from_instances(team_name, penalty['Nr'], self.SQL)
            time = seconds(penalty['Laiks'])
            penalty_index = get_index('penalty_index', 'Penalties', self.SQL) + 1
            df = pd.DataFrame([[penalty_index, 
                                self.index, 
                                time,
                                penalized_player,]], columns = Game.penalty_db_schema)
            self.SQL.write_to_db(df, 'Penalties')
            print("penalty: ",df,penalized_player)
        # In the case of MULTIPLE penalties across the span of the game
        elif type(penalties['Sods']) == list:
            for penalty in penalties['Sods']:
                penalized_player = Player.find_from_instances(team_name, penalty['Nr'], self.SQL)
                time = seconds(penalty['Laiks'])
                penalty_index = get_index('penalty_index', 'Penalties',self.SQL) + 1
                df = pd.DataFrame([[penalty_index, 
                                    self.index, 
                                    time,
                                    penalized_player,]], columns = Game.penalty_db_schema)
                self.SQL.write_to_db(df, 'Penalties')
                print("penalty: ", df, penalized_player)
        else:
            print("something broke with penalties, ", penalties)
    def add_subs(self,subs,team_name):
        """
        This function writes substitutions from the json file into table 'Substitutions'
        TODO: Create the actual logic for substitutions
        TODO: penalty index from get_index
        """
        # Catch empty lists of substitutions (NO substitutions)
        try:
            subs['Maina']
        except TypeError:
            return None
        # In case of a SINGLE substitution across the span of the entire game: 
        if type(subs['Maina']) == dict:
            sub = subs['Maina']
            time = seconds(sub['Laiks'])
            leaving = Player.find_from_instances(team_name, sub['Nr1'], self.SQL)
            entering = Player.find_from_instances(team_name, sub['Nr2'], self.SQL)
            sub_index = get_index('substitution_index', 'Substitutions', self.SQL) + 1
            df = pd.DataFrame([[sub_index, 
                                self.index, 
                                time, 
                                leaving, 
                                entering]], columns = Game.substitutions_db_schema)
            self.SQL.write_to_db(df,'Substitutions')
        # In case of MULTIPLE substitutions across the span of the entire game: 
        elif type(subs['Maina']) == list:
            for sub in subs['Maina']:
                time = seconds(sub['Laiks'])
                leaving = Player.find_from_instances(team_name, sub['Nr1'], self.SQL)
                entering = Player.find_from_instances(team_name, sub['Nr2'], self.SQL)
                sub_index = get_index('substitution_index', 'Substitutions', self.SQL) + 1
                df = pd.DataFrame([[sub_index, 
                                    self.index, 
                                    time, 
                                    leaving, 
                                    entering]], columns = Game.substitutions_db_schema)
                self.SQL.write_to_db(df,'Substitutions')
        else:
            print("something broke with subs, ", subs)
    def add_game(self):
        """
        This function does all the logic of the scores and points
        Writes the game data into the MySQL table 'Games'
        TODO: Implement overtime counter
        TODO: Implement played time counter. Or find a way to do it with SQL.
        """
        sen_ref = Referee.find_from_instances(self.sen_ref['Vards'], self.sen_ref['Uzvards'], self.SQL)
        ref1 = Referee.find_from_instances(self.ref1['Vards'], self.ref1['Uzvards'], self.SQL)
        ref2 = Referee.find_from_instances(self.ref2['Vards'], self.ref2['Uzvards'], self.SQL)
        # If EITHER team1 or team2 have scored NO goals
        print("t1 goals: ", self.team1_goals)
        print("t2 goals: ", self.team2_goals)
        if type(self.team1_goals) == str or type(self.team2_goals) == str:
            if type(self.team1_goals) == str:
                """
                Executes if team 1 has scored no goals.
                There is no scenario in which team 1 wins with 0 goals
                """
                # Get last goal time of the second team
                goaltime = goal_time(self.team2_goals['VG'])
                if goaltime >= 3600:
                    t1p = 2
                    t2p = 3
                else:
                    t1p = 1
                    t2p = 5
            else:
                """
                Executes if team 2 has scored no goals.
                There is no scenario in which team 2 wins with 0 goals
                """
                # Get last goal time of the first team
                goaltime = goal_time(self.team1_goals['VG'])
                if goaltime >= 3600:
                    t1p = 3
                    t2p = 2
                else:
                    t1p = 5
                    t2p = 1
        else:
            t1t = type(self.team1_goals['VG'])
            t2t = type(self.team2_goals['VG'])
            # If EITHER team has scored only ONE goal
            if t1t == dict or t2t == dict:
                if t1t == dict:
                    """
                    The only case where team 1 wins with 1 goal is
                    If team 2 has zero goals and that is already covered
                    """
                    # t2t must be list
                    # Get last goal time for the first team
                    goaltime1 = goal_time(self.team1_goals['VG'])
                    # Get the last goal time for the second team
                    goaltime2 = goal_time(self.team2_goals['VG'])
                    if goaltime1 > 3600 or goaltime2 > 3600:
                        t1p = 2
                        t2p = 3
                    else:
                        t1p = 1
                        t2p = 5
                else:
                    """
                    The only case where team 2 wins with 1 goal:
                    If team 1 has zero goals and that is already covered
                    """
                    # t1t must be list
                    # Get last goal time for the first team
                    goaltime1 = goal_time(self.team1_goals['VG'])
                    # Get the last goal time for the second team
                    goaltime2 = goal_time(self.team2_goals['VG'])
                    if goaltime1 > 3600 or goaltime2 > 3600:
                        t1p = 3
                        t2p = 2
                    else:
                        t1p = 5
                        t2p = 1
            
            # if BOTH teams have scored MULTIPLE
            elif t1t == list and t2t == list:
                if len(self.team1_goals['VG']) > len(self.team2_goals['VG']):
                    if seconds(self.team1_goals['VG'][-1]['Laiks']) <= 3600:
                        # 1. uzvar pamatlaikā
                        t1p = 5
                        t2p = 1
                    else:
                    # elif seconds(self.team1_goals[-1]['Laiks']) > 3600:
                        # 1. uzvar papildlaikā
                        t1p = 3
                        t2p = 2
                else:
                    if seconds(self.team2_goals['VG'][-1]['Laiks']) <= 3600:
                        # 2. uzvar pamatlaikā
                        t1p = 1
                        t2p = 5
                    else:
                    # elif seconds(self.team2_goals[-1]['Laiks']) > 3600:
                        # 2. uzvar papildlaikā
                        t1p = 2
                        t2p = 3
        print("refs: ",ref1, ref2, sen_ref)
        df = pd.DataFrame([[self.index, 
                            self.location,
                            self.time,
                            self.spectators,
                            self.team1, 
                            self.team2, 
                            sen_ref, 
                            ref1, 
                            ref2, 
                            t1p, 
                            t2p]], columns = Game.game_db_schema)
        self.SQL.write_to_db(df,'Games')
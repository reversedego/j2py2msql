"""
This file gets all the stats and performs timekeeping and tournament table logic.
# TODO: Think of other things this file needs to do.

"""
# Standard imports

# Dependency imports
import pandas as pd


# Local imports
from MySQL_API import MySQLConnection
from Referee import Referee



SQL = MySQLConnection('root','localhost','testing')


def tournament_table():
    """
    This function retrieves the tournament table.   
    The teams need to be sorted descending by points.
    The result needs to have:
        * Team rank in the table
        * Team name
        * Total team points
        * Wins/Losses (non OT)
        * Wins/Losses (in OT)
        * Nr. of Goals scored
        * Nr. of Goals lost
    TODO: Finish the output
    """
    distinct_team_query = "select distinct komandas_nosaukums from Players"
    list_of_teams = list(pd.read_sql(distinct_team_query, SQL.cnx)['komandas_nosaukums'])
    for team in list_of_teams:
        winsOT = 0
        lossesOT = 0
        wins = 0
        losses = 0
        points = 0
        query1 = "select 1_Komandas_Punkti from Games where 1_Komandas_Nosaukums = '{}'".format(team)
        team_1_column_summary = list(pd.read_sql(query1, SQL.cnx)['1_Komandas_Punkti'])
        for points_gained in team_1_column_summary:
            if points_gained == 1:
                lossesOT+=1
            elif points_gained == 5:
                wins+=1
            elif points_gained == 3:
                winsOT+=1
            elif points_gained == 2:
                lossesOT+=1
            points+=points_gained
        
        query2 = "select 2_Komandas_Punkti from Games where 2_Komandas_Nosaukums = '{}'".format(team)
        team_2_column_summary = list(pd.read_sql(query1, SQL.cnx)['2_Komandas_Punkti'])
        for points_gained in team_2_column_summary:
            if points_gained == 1:
                lossesOT+=1
            elif points_gained == 5:
                wins+=1
            elif points_gained == 3:
                winsOT+=1
            elif points_gained == 2:
                lossesOT+=1
            points+=points_gained
        # Count the amount of goals the team has scored:
        # create table Tournament_Summary(Komanda TEXT, Punkti INT, Uzvaras_OT INT, Zaudejumi_OT INT, Uzvaras INT, Zaudejumi INT
        query_scores = """select count from Goals where """
        
        print("")
def top_scorers():
    """
    This function retrieves 10 top scoring players.
    The players need to be sorted descending by goals scored.
    If nr of goals match, then sort by nr of assists.
    The result needs to have:
        * Player rank,
        * Player name, 
        * Player surname, 
        * Player team name,
        * Nr of goals scored,
        * Nr of assists.
    TODO: Add rankings
    TODO: Add assists
    """
    query = """create temporary table GoalsPlayers as 
                select * from Goals 
                    inner join 
                    Players on Goals.Vartu_Guvejs_index = Players.player_index"""
    query2 = """create temporary table player_index_goal_count as 
                select player_index, count(*) as Player_Goals from GoalsPlayers 
                group by
                    player_index"""
    # https://www.quora.com/Can-we-perform-a-JOIN-operation-on-a-temporary-table-on-MySQL
    # Yes, you can.
    # But you can reference a temporary table only once in a given query. So for example you can't do a self-join against a temp table.
    query3 = """create temporary table score_join as 
                select * from Players inner join player_index_goal_count on player_index_goal_count.player_index = Players.player_index order by Player_Goals desc"""
    
    query4 = """create temporary table """

    result = pd.read_sql(query, SQL.cnx)
    return result

# def top_goalies(team_name):
#     """
#     This function retrieves top 5 goalies.
#     The goalies need to be sorted ascending by average goals lost per game.
#     If a goalie has not been on the field in any game, he must not be on this list.
#     The result needs to have:
#         * Goalie rank
#         * Goalie name
#         * Goalie surname
#         * Goalie team name,
#         * Lost goals per game (float with a single decimal)
#     Most likely will need to create a t able or write a query that creates the table.
#     """
#     # Create table of goalies
#     query0 = """create temporary table drop table if exists Goalies as 
#                 select player_index, Komandas_Nosaukums , Nr , Uzvards , Vards from Players 
#                 where Loma = 'V'"""
#     # Select only the goalies that have played:
#     query1 = """"""
#     # Calculate goals lost per team, first
#     # Find the amount of goals that every team has lost per game
#         # Do this by first joining the games and goals by game_index and selecting only the relevant columns:
#     query2 = """create temporary table GamesOfGoals as 
#                 select goal_index, Vartu_Guvejs_index, 1_Komandas_Nosaukums, 2_Komandas_Nosaukums 
#                 from Goals inner join Games on Goals.game_index = Games.game_index"""
#     # Get all the relevant columns in one place
#     query3 = """create temporary table TeamGoalInformation as
#                 Select goal_index, Komandas_Nosaukums ,1_Komandas_Nosaukums, 2_Komandas_Nosaukums  
#                 from GamesOfGoals inner join Players on GamesOfGoals.Vartu_Guvejs_index = Players.player_index"""
    
#     # query4 = """
#     #     select * from Komandas_Nosaukums where Komandas_Nosaukums = 1_Komandas_Nosaukums"""

#     query4 = """create table TeamGoalInformation2 as select * from TeamGoalInformation
#     """
    
#     # result = pd.read_sql(query, SQL.cnx)
#     # return result

def roughest_players():
    """
    This function retrieves players sorted descending by the nr of penalties they have received.
    TODO: Done
    """
    query = """
    SELECT 
    Vards, Uzvards, Nr, Loma ,Komandas_Nosaukums, player_index, COUNT(*) as penalty_count
    FROM
        Players
            INNER JOIN
        Penalties USING (player_index)
    GROUP BY Vards, Uzvards, Nr, Loma ,Komandas_Nosaukums, player_index
    ORDER BY penalty_count DESC
    """
    result = pd.read_sql(query, SQL.cnx)
    print(result)
    return result

def get_team_summary(team_name):
    """
    This function collects a summary of the players for a given team.
    The results for each player need to contain :
        *Player nr
        *Player name
        *Player surname
        *Games played in starting line-up,
        *Minutes played
        *Nr of yellow and red cards
    Goalie stats need to be collected in a seperate table. In addition to all this:
        *Games played
        *Goals lost
        *Goals lost on average per game.
    """
    query = ""
    result = pd.read_sql(query, SQL.cnx)
    return result

def get_strictest_refs():
    """
    This function gets the strictest referees.
    The refs need to be sorted descending by the amount of penalties given.
    Ok, so ref table needs an additional column that contains the amount of penalties the ref has given.
    SQL implementation would look like first joining penalty table and game table so that
    the resulting table would be based off the table Penalties and contain:
        Penalties.penalty_index, Game.VT_index, Game.LT1_index, Game.LT2_Index
    In short, every penalty needs to have refs that gave the penalty.
    Then we can count the indexes that appear the most among all three columns.
    Then connect indexes to actual refs.
    """
    
    query = """SELECT * FROM Penalties LEFT JOIN Games ON Games.game_index=Penalties.game_index """
    # Join tables in MySQL and turn them into a DF
    penalty_game_join = pd.read_sql(query, SQL.cnx)
    # Take only the necessary columns
    role_cols = ['VT_index','LT1_index','LT2_index']
    necessary_pgj = penalty_game_join[role_cols]
    SQL.write_to_db(necessary_pgj, 'JOINED_Penalties_Games')
    # Get LIST of unique referee indexes
    ref_index_df = pd.read_sql("select referee_index from Referees", SQL.cnx)
    ref_index_list = ref_index_df['referee_index'].tolist()
    ref_penalty_counts = {}
    # For the VT column:
    for role_col in role_cols:
        # role_query = "select VT_index, count(*) as count from JOINED_Penalties_Games group by VT_index"
        role_query = "select {}, count(*) as count from JOINED_Penalties_Games group by {}".format(role_col, role_col)
        role_summary = pd.read_sql(role_query, SQL.cnx)
        n = 0
        for index in role_summary[role_col]:
            try:
                ref_penalty_counts[str(index)]
            except KeyError:
                ref_penalty_counts[str(index)] = 0
            ref_penalty_counts[str(index)]+=role_summary['count'][n]
            n+=1
    for key in ref_penalty_counts.keys():
        ref_from_index = pd.read_sql("Select Vards, Uzvards from Referees where referee_index = {}".format(int(key)), SQL.cnx)
        print(ref_from_index)


    # pd.DataFrame.from_dict(ref_penalty_counts)
    # Count instances of each index appearing the columns of VT, LT1, LT2
    

# def played_time(team,nr):
#     """
#     This function calculates the played time for a particular player
#     """
#     query = ""
#     result = pd.read_sql(query, SQL.cnx)
    




# The actual gathering of stats


roughest_players()
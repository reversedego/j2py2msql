"""
This is the main file. Here we use the high level logic to interact with MySQL tables and Python objects.
TODO: Test everything. Entries seem to work flawlessly.
"""

# Standard imports
import json
import sys
import os

# Dependancy imports
import pandas as pd

# Local imports
from local_utilities import seconds, get_index
from MySQL_API import MySQLConnection

# Class imports
from Player import Player
from Referee import Referee
from Game import Game

basepath = os.getcwd()
path = basepath + '/JSONSecondRound/'
# path = sys.argv[1]
def dictionary_from_file(JSONFile):
    """
    Takes a json file and returns a <class 'dict'> type object using json library
    """
    # using: file --mime-encoding 'JSONFirstRound/futbols0.json' yielded 'futbols0.json: iso-8859-1'
    with open(JSONFile, 'r', encoding = 'iso-8859-1') as myfile:
        data = myfile.read().replace('\n', '').replace('        ','')
    myfile.close()
    dataAsDict = json.loads(data)
    return dataAsDict['Spele']

# Put the files in the directory in a list
filenames = []
directory = os.fsencode(path)
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".json"):
        filenames.append(filename)
    else:
        pass
print("found files: ",filenames)

# Instantiate the connection to the MySQL database.
SQL = MySQLConnection('root','localhost','soccer')

# Parse through all the files and catch replicas of game files.
new_games = []
game_dictionaries = []
for filename in filenames:
    i = 0
    data = dictionary_from_file(path + filename)
    spectators =  data['Skatitaji']
    time_of_game = data['Laiks']
    location = data['Vieta']
    query = "SELECT * FROM Games WHERE Speles_Vieta='{}' AND Speles_Laiks='{}' AND Skatitaji = '{}' ".format(location, time_of_game,spectators)
    matching_games = pd.read_sql(query,SQL.cnx)
    if matching_games.shape[0] == 0:
        new_games.append(filename)
        game_dictionaries.append(data)
    else:
        print(filename, " seems to be a replica of an existing game.")
print("new games: ", new_games)


# Parse the dictionary:
n = 0
for game_dictionary in game_dictionaries:
    print(new_games[n])
    n+=1
    # Refs
    for ref in game_dictionary['T']:
        Referee(ref, SQL)
    Referee(game_dictionary['VT'], SQL)
    # Players
    for team in game_dictionary['Komanda']:
        for player in team['Speletaji']['Speletajs']:
            Player(team['Nosaukums'], player, SQL)
        # Then, parse the game itself
        instance = Game(game_dictionary,SQL)
        # Now to add goals, penalties and substitutions and the game itself to the according tables
        instance.add_goals(team['Varti'],team['Nosaukums'])
        instance.add_penalties(team['Sodi'],team['Nosaukums'])
        # Only subs did not pass the initial write. 
        instance.add_subs(team['Mainas'],team['Nosaukums'])
    instance.add_game()
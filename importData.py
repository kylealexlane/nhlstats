import numpy as np
import pandas as pd
import requests
import psycopg2
from datetime import datetime
import dateutil.parser

# Base numbers for testing
# baseYear = 2016
# gameType = 2
# gameID = 143
#
# # Loop that could be used for games
# if gameID < 10:
#     gameID = '000' + str(gameID)
# elif gameID < 100:
#     gameID = '00' + str(gameID)
# elif gameID < 1000:
#     gameID = '0' + str(gameID)
# else:
#     gameID = str(gameID)
#
# # For testing loop
# response = requests.get("https://statsapi.web.nhl.com/api/v1/game/"+str(baseYear)+"0"+str(gameType)+gameID+"/feed/live")
# gameData = response.json()

startDate = '1985-01-09'
endDate = '1990-01-09'

i = 0
# Fetching from csv
gamesList = pd.read_csv('gameslist-{}-{}.csv'.format(startDate, endDate))
for index, row in gamesList.iterrows():
    i += 1
    print(i)
    print(row[1])
    gamePk = str(row[1])
    # If using gamePk
    response = requests.get("https://statsapi.web.nhl.com/api/v1/game/"+str(row[1])+"/feed/live")
    gameData = response.json()


    # game info
    gamepk = gameData['gameData']['game']['pk']
    # gameSeason = gameData['gameData']['game']['season']
    # gameType = gameData['gameData']['game']['type']

    # datetime info
    dateTime = gameData['gameData']['datetime']['dateTime']
    # endDateTime = gameData['gameData']['datetime']['endDateTime']

    # status info
    # abstractGameState = gameData['gameData']['status']['abstractGameState']
    # codedGameState = gameData['gameData']['status']['codedGameState']
    # detailedState = gameData['gameData']['status']['detailedState']
    # statusCode = gameData['gameData']['status']['statusCode']
    # startTimeTBD = gameData['gameData']['status']['startTimeTBD']

    ## teams
    # awayId = gameData['gameData']['teams']['away']['id']
    # awayName = gameData['gameData']['teams']['away']['name']
    # homeId = gameData['gameData']['teams']['home']['id']
    # homeName = gameData['gameData']['teams']['home']['name']


    hostname = 'localhost'
    username = 'kylelane'
    password = ''
    database = 'kylelane'

    myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
    cur = myConnection.cursor()

    cur.execute('DELETE FROM nhlstats.allplays WHERE game_id = (%s) ;', (gamePk,))
    myConnection.commit()
    cur.close()
    myConnection.close()

    myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
    cur = myConnection.cursor()
    for index, play in enumerate(gameData['liveData']['plays']['allPlays']):
        cur.execute("""
            INSERT INTO nhlstats.allplays 
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s);
            """,
                    (gamepk,
                     dateutil.parser.parse(dateTime),
                     play['about']['eventIdx'],
                     play['about']['eventId'],
                     play['about']['period'],
                     play['about']['periodType'],
                     play['about']['ordinalNum'],
                     play['about']['periodTime'],
                     play['about']['periodTimeRemaining'],
                     play['about']['goals']['away'],
                     play['about']['goals']['home'],
                     play['coordinates']['x'] if len(play['coordinates']) > 0 and 'x' in play['coordinates'] else None,
                     play['coordinates']['y'] if len(play['coordinates']) >0 and 'y' in play['coordinates'] else None,
                     play['team']['id'] if 'team' in play else None,
                     play['team']['name'] if 'team' in play else None,
                     play['team']['triCode'] if 'team' in play and 'triCode' in play['team'] else None,
                     play['players'][0]['player']['id'] if 'players' in play else None,
                     play['players'][0]['player']['fullName'] if 'players' in play else None,
                     play['players'][0]['playerType'] if 'players' in play else None,
                     play['players'][1]['player']['id'] if ('players' in play) & (len(play['players']) > 1 if 'players' in play else False) else None,
                     play['players'][1]['player']['fullName'] if ('players' in play) & (len(play['players']) > 1 if 'players' in play else False) else None,
                     play['players'][1]['playerType'] if ('players' in play) & (len(play['players']) > 1 if 'players' in play else False) else None,

                     play['players'][2]['player']['id'] if ('players' in play) & (
                         len(play['players']) > 2 if 'players' in play else False) else None,
                     play['players'][2]['player']['fullName'] if ('players' in play) & (
                         len(play['players']) > 2 if 'players' in play else False) else None,
                     play['players'][2]['playerType'] if ('players' in play) & (
                         len(play['players']) > 2 if 'players' in play else False) else None,

                     play['players'][3]['player']['id'] if ('players' in play) & (
                         len(play['players']) > 3 if 'players' in play else False) else None,
                     play['players'][3]['player']['fullName'] if ('players' in play) & (
                         len(play['players']) > 3 if 'players' in play else False) else None,
                     play['players'][3]['playerType'] if ('players' in play) & (
                         len(play['players']) > 3 if 'players' in play else False) else None,

                     play['result']['event'],
                     play['result']['eventCode'],
                     play['result']['eventTypeId'],
                     play['result']['secondaryType'] if 'secondaryType' in play['result'] else None
                     ))

    # commit sql
    myConnection.commit()
    # Close communication with the database
    cur.close()
    myConnection.close()







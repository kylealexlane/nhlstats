import pandas as pd
import numpy as np
from sklearn import preprocessing
import os
from sqlalchemy import create_engine
import pickle
import requests
import psycopg2

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')
sql = """SELECT DISTINCT(player1_id) FROM nhlstats.adjusted_shots"""

allPlayerIds = pd.read_sql_query(sql, con=engine)

for index, row in allPlayerIds.iterrows():
    playerid = int(row['player1_id'])
    # Testing
    # playerid = 8477474
    #
    print(playerid, index)

    response = requests.get("https://statsapi.web.nhl.com/api/v1/people/" + str(playerid))
    playerData = response.json()



    hostname = 'localhost'
    username = 'kylelane'
    password = ''
    database = 'kylelane'

    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()

    cur.execute('DELETE FROM nhlstats.players WHERE id = (%s) ;', (playerid,))
    myConnection.commit()
    cur.close()
    myConnection.close()

    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    d = playerData['people'][0] if 'people' in playerData and len(playerData['people']) > 0 else {}
    ct = d['currentTeam'] if 'currentTeam' in d else {}
    pp = d['primaryPosition'] if 'primaryPosition' in d else {}

    cur.execute("""
            INSERT INTO nhlstats.players 
            VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s);
            """,
                (d['id'],
                 d['fullName'] if 'fullName' in d else None,
                 d['link'] if 'link' in d else None,
                 d['firstName'] if 'firstName' in d else None,
                 d['lastName'] if 'lastName' in d else None,
                 d['primaryNumber'] if 'primaryNumber' in d else None,
                 d['birthDate'] if 'birthDate' in d else None,
                 d['currentAge'] if 'currentAge' in d else None,
                 d['birthCity'] if 'birthCity' in d else None,
                 d['birthStateProvince'] if 'birthStateProvince' in d else None,
                 d['birthCountry'] if 'birthCountry' in d else None,
                 d['nationality'] if 'nationality' in d else None,
                 d['height'] if 'height' in d else None,
                 d['weight'] if 'weight' in d else None,
                 d['active'] if 'active' in d else None,
                 d['alternateCaptain'] if 'alternateCaptain' in d else None,
                 d['captain'] if 'captain' in d else None,
                 d['rookie'] if 'rookie' in d else None,
                 d['shootsCatches'] if 'shootsCatches' in d else None,
                 d['rosterStatus'] if 'rosterStatus' in d else None,
                 ct['id'] if 'id' in ct else None,
                 ct['name'] if 'name' in ct else None,
                 ct['link'] if 'link' in ct else None,
                 pp['code'] if 'code' in pp else None,
                 pp['name'] if 'name' in pp else None,
                 pp['type'] if 'type' in pp else None,
                 pp['abbreviation'] if 'abbreviation' in pp else None
                 ))

    # commit sql
    myConnection.commit()
    # Close communication with the database
    cur.close()
    myConnection.close()

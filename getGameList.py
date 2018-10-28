import numpy as np
import pandas as pd
import requests
import psycopg2
from datetime import datetime
import dateutil.parser
import os


startDate = "2018-01-09"
endDate = "2019-01-09"
response = requests.get(
    "https://statsapi.web.nhl.com/api/v1/schedule?startDate="+startDate+"&endDate="+endDate)
allData = response.json()

gamesList = []

for date in allData['dates']:
    for game in date['games']:
        gamesList.append(game['gamePk'])

s = pd.Series(gamesList)
wd = os.getcwd()
s.to_csv('gamesList-'+startDate+'-'+endDate+'.csv')







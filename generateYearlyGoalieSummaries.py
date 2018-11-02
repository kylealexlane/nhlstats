import pandas as pd
import numpy as np
from sklearn import preprocessing
import os
from sqlalchemy import create_engine
import pickle
import requests
import psycopg2
from ignore import engine

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

sql = """SELECT DISTINCT(player2_id) FROM nhlstats.adjusted_shots WHERE player2_type = 'Goalie'"""
allPlayerIds = pd.read_sql_query(sql, con=engine)
allPlayerIds = allPlayerIds.rename(index=str, columns={"player2_id": "player_id"})

sql = """SELECT DISTINCT(player3_id) FROM nhlstats.adjusted_shots WHERE player3_type = 'Goalie'"""
allPlayerIds2 = pd.read_sql_query(sql, con=engine)
allPlayerIds2 = allPlayerIds2.rename(index=str, columns={"player3_id": "player_id"})
allPlayerIds = allPlayerIds.append(allPlayerIds2)

sql = """SELECT DISTINCT(player4_id) FROM nhlstats.adjusted_shots WHERE player4_type = 'Goalie'"""
allPlayerIds3 = pd.read_sql_query(sql, con=engine)
allPlayerIds3 = allPlayerIds3.rename(index=str, columns={"player4_id": "player_id"})
allPlayerIds = allPlayerIds.append(allPlayerIds3)

allPlayerIds = allPlayerIds.drop_duplicates('player_id')

for index, row in allPlayerIds.iterrows():
    playerid = int(row['player_id'])
    # Testing
    # PRICE
    # playerid = 8471679
    #
    print(playerid, index)

    sql = """SELECT * FROM nhlstats.adjusted_shots 
        WHERE (player2_id = '%s' 
        AND player2_type = 'Goalie') 
        OR (player3_id = '%s' 
        AND player3_type = 'Goalie')
        OR (player4_id = '%s' 
        AND player4_type = 'Goalie')""" % (playerid, playerid, playerid)
    allShots = pd.read_sql_query(sql, con=engine)

    t_numShots = allShots.shape[0]
    t_predGoalsAgainst = allShots['pred'].sum()
    t_actGoalsAgainst = allShots['goal_binary'].sum()
    t_actSavePerc= 1 - (t_actGoalsAgainst / t_numShots)
    t_predSavePerc = 1 - (t_predGoalsAgainst / t_numShots)
    t_savesAddedPerShot = t_actSavePerc - t_predSavePerc

    segments = [
        (2010, 2011, '2010-10-06', '2011-04-11', 0),
        (2010, 2011, '2011-04-12', '2011-06-16', 1),
        (2011, 2012, '2011-10-05', '2012-04-08', 0),
        (2011, 2012, '2012-04-10', '2012-06-12', 1),
        (2012, 2013, '2013-01-18', '2013-04-29', 0),
        (2012, 2013, '2013-04-29', '2013-06-25', 1),
        (2013, 2014, '2013-09-30', '2014-04-14', 0),
        (2013, 2014, '2014-04-15', '2014-06-14', 1),
        (2014, 2015, '2014-10-07', '2015-04-12', 0),
        (2014, 2015, '2015-04-14', '2015-06-16', 1),
        (2015, 2016, '2015-10-06', '2016-04-11', 0),
        (2015, 2016, '2016-04-12', '2016-06-13', 1),
        (2016, 2017, '2016-10-11', '2017-04-10', 0),
        (2016, 2017, '2017-04-11', '2017-06-12', 1),
        (2017, 2018, '2017-10-03', '2018-04-09', 0),
        (2017, 2018, '2018-04-10', '2018-06-08', 1),
        (2018, 2019, '2018-10-02', '2019-04-07', 0),
        (2018, 2019, '2019-04-08', '2019-06-29', 1)
    ]
    results = {'year_start': [],
               'year_end': [],
               'num_shots_against': [],
               'pred_goals_against': [],
               'act_goals_against': [],
               'act_save_perc': [],
               'pred_save_perc': [],
               'saves_added_per_shot': [],
               'playoff': []
               }
    cols = ['year_start', 'year_end', 'num_shots_against', 'pred_goals_against', 'act_goals_against', 'act_save_perc', 'pred_save_perc', 'saves_added_per_shot', 'playoff']
    for yearstart, yearend, start, end, p in segments:
        shots = allShots.loc[
            (allShots['game_date'] > start) & (allShots['game_date'] < end)].copy()
        numShots = shots.shape[0]
        predGoalsAgainst = shots['pred'].sum()
        actGovalsAgainst = shots['goal_binary'].sum()
        actSavePerc = 1 - (actGovalsAgainst / numShots) if numShots != 0 else 0
        predSavePerc = 1 - (predGoalsAgainst / numShots) if numShots != 0 else 0
        savesAddedPerShot = actSavePerc - predSavePerc
        results['year_start'].append(yearstart)
        results['year_end'].append(yearend)
        results['num_shots_against'].append(numShots)
        results['pred_goals_against'].append(float(predGoalsAgainst))
        results['act_goals_against'].append(int(actGovalsAgainst))
        results['act_save_perc'].append(actSavePerc)
        results['pred_save_perc'].append(predSavePerc)
        results['saves_added_per_shot'].append(savesAddedPerShot)
        results['playoff'].append(p)


    sql = """DELETE FROM nhlstats.yearly_goalie_summaries
                WHERE id = %s""" % playerid
    connection = engine.connect()
    connection.execute(sql)

    shotResults = pd.DataFrame(data=results, columns=cols)
    shotResults['id'] = playerid
    shotResults.to_sql('yearly_goalie_summaries', schema='nhlstats', con=engine, if_exists='append', index=False)


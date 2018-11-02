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

# engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')
sql = """SELECT DISTINCT(player1_id) FROM nhlstats.adjusted_shots"""

allPlayerIds = pd.read_sql_query(sql, con=engine)

for index, row in allPlayerIds.iterrows():
    playerid = int(row['player1_id'])
    # Testing
    # playerid = 8471675
    #
    print(playerid, index)

    sql = """SELECT * FROM nhlstats.adjusted_shots 
        WHERE player1_id = %s""" % playerid
    allShots = pd.read_sql_query(sql, con=engine)

    t_numShots = allShots.shape[0]
    t_predGoals = allShots['pred'].sum()
    t_actGoals = allShots['goal_binary'].sum()
    t_actShootingPerc = t_actGoals / t_numShots
    t_predShootingPerc = t_predGoals / t_numShots
    t_addedShootingSkill = t_actShootingPerc - t_predShootingPerc

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
               'num_shots': [],
               'pred_goals': [],
               'act_goals': [],
               'act_shoot_perc': [],
               'pred_shoot_perc': [],
               'shoot_skill': [],
               'playoff': []
               }
    cols=['year_start', 'year_end', 'num_shots', 'pred_goals', 'act_goals', 'act_shoot_perc', 'pred_shoot_perc', 'shoot_skill', 'playoff']
    for yearstart, yearend, start, end, p in segments:
        shots = allShots.loc[
            (allShots['game_date'] > start) & (allShots['game_date'] < end)].copy()
        numShots = shots.shape[0]
        predGoals = shots['pred'].sum()
        actGoals = shots['goal_binary'].sum()
        actShootPerc = actGoals / numShots if numShots != 0 else 0
        predShootPerc = predGoals / numShots if numShots != 0 else 0
        shootSkill = actShootPerc - predShootPerc
        results['year_start'].append(yearstart)
        results['year_end'].append(yearend)
        results['num_shots'].append(numShots)
        results['pred_goals'].append(float(predGoals))
        results['act_goals'].append(int(actGoals))
        results['act_shoot_perc'].append(actShootPerc)
        results['pred_shoot_perc'].append(predShootPerc)
        results['shoot_skill'].append(shootSkill)
        results['playoff'].append(p)


    sql = """DELETE FROM nhlstats.yearly_shooter_summaries 
                WHERE id = %s""" % playerid
    connection = engine.connect()
    connection.execute(sql)

    shotResults = pd.DataFrame(data=results, columns=cols)
    shotResults['id'] = playerid
    shotResults.to_sql('yearly_shooter_summaries', schema='nhlstats', con=engine, if_exists='append', index=False)


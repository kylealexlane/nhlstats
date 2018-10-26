import pandas as pd
import numpy as np
from sklearn import preprocessing
import os
from sqlalchemy import create_engine
import pickle

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')
sql = """SELECT * from  nhlstats.allplays 
    WHERE (event_type = 'Shot' 
    OR event_type = 'Goal' 
    OR event_type ='Missed Shot') 
    AND game_date >= '2010-09-21'
    AND game_date < '2011-01-09'
    """

allShots = pd.read_sql_query(sql, con=engine)

filename = 'finalized_model_knn_1000.sav'
loaded_model = pickle.load(open(filename, 'rb'))

filename = 'finalized_model_knn_1000_scaled_factors.sav'
loaded_scaled_factors = pickle.load(open(filename, 'rb'))

# allShots.fillna(allShots.mean())
allShots = allShots.fillna(0)


# net center location
goal_x = 89
goal_y = 0
allShots['adjx'] = abs(allShots['x_coords'])
allShots['adjy'] = abs(allShots['y_coords'])
allShots['goal_binary'] = np.where(allShots['event_type'] == 'Goal', 1, 0)
allShots['wrist_shot'] = np.where(allShots['event_desc'] == 'Wrist Shot', 1, 0)
allShots['backhand'] = np.where(allShots['event_desc'] == 'Backhand', 1, 0)
allShots['slap_shot'] = np.where(allShots['event_desc'] == 'Slap Shot', 1, 0)
allShots['snap_shot'] = np.where(allShots['event_desc'] == 'Snap Shot', 1, 0)
allShots['tip_in'] = np.where(allShots['event_desc'] == 'Tip-In', 1, 0)
allShots['deflected'] = np.where(allShots['event_desc'] == 'Deflected', 1, 0)
allShots['wrap_around'] = np.where(allShots['event_desc'] == 'Wrap-around', 1, 0)
allShots['none'] = np.where(allShots['event_desc'] == 'None', 1, 0)
allShots = allShots[allShots['event_type_id'] != 'MISSED_SHOT']
# Distance and angle of location
allShots['dist'] = np.sqrt(np.power((allShots['adjx'] - goal_x), 2) + np.power((allShots['y_coords'] - goal_y), 2))
allShots['ang'] = 180 - (np.arctan2(allShots['adjy'], allShots['adjx']-100) * 180 / np.pi)

# def createShotsParams(allShots):
#     # net center location
#     goal_x = 89
#     goal_y = 0
#     allShots['adjx'] = abs(allShots['x_coords'])
#     allShots['adjy'] = abs(allShots['y_coords'])
#     allShots['goal_binary'] = np.where(allShots['event_type'] == 'Goal', 1, 0)
#     allShots['wrist_shot'] = np.where(allShots['event_desc'] == 'Wrist Shot', 1, 0)
#     allShots['backhand'] = np.where(allShots['event_desc'] == 'Backhand', 1, 0)
#     allShots['slap_shot'] = np.where(allShots['event_desc'] == 'Slap Shot', 1, 0)
#     allShots['snap_shot'] = np.where(allShots['event_desc'] == 'Snap Shot', 1, 0)
#     allShots['tip_in'] = np.where(allShots['event_desc'] == 'Tip-In', 1, 0)
#     allShots['deflected'] = np.where(allShots['event_desc'] == 'Deflected', 1, 0)
#     allShots['wrap_around'] = np.where(allShots['event_desc'] == 'Wrap-around', 1, 0)
#     allShots['none'] = np.where(allShots['event_desc'] == 'None', 1, 0)
#     allShots = allShots[allShots['event_type_id'] != 'MISSED_SHOT']
#     # Distance and angle of location
#     allShots['dist'] = np.sqrt(np.power((allShots['adjx'] - goal_x), 2) + np.power((allShots['y_coords'] - goal_y), 2))
#     allShots['ang'] = 180 - (np.arctan2(allShots['adjy'], allShots['adjx']-100) * 180 / np.pi)
#     return(allShots)


# allShots = createShotsParams(allShots)

X = np.asarray(allShots[['dist',
                         'ang',
                         'adjx',
                         'adjy',
                         'wrist_shot',
                         'backhand',
                         'snap_shot',
                         'slap_shot',
                         'tip_in',
                         'deflected',
                         'wrap_around'
                         ]])

X_pp = loaded_scaled_factors.transform(X)
np.argwhere(np.isnan(X_pp))


predictions = loaded_model.predict_proba(X_pp)

predictions_1 = predictions[:, 1]

allShots_predicted = allShots
allShots_predicted['pred'] = predictions_1

allShots_predicted.head()

engine = create_engine('postgresql://kylelane@localhost:5432/kylelane')

allShots.to_sql('adjusted_shots', schema = 'nhlstats', con=engine, if_exists='append', index=False)

import pandas as pd
import numpy as np
import pickle
from production.ignore import engine
import sys
from sqlalchemy import event
import io



pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()

def AdjustShots(startDate, endDate):
    # startDate = '2010-08-01'
    # endDate = '2019-08-01'

    startDate = '2013-01-18'
    endDate = '2013-04-29'

    print('fetching all plays...')
    sql = """SELECT * from  nhlstats.allplays 
        WHERE (event_type = 'Shot' 
        OR event_type = 'Goal' 
        OR event_type ='Missed Shot') 
        AND game_date > '%s'
        AND game_date < '%s'
        """ % (startDate, endDate)

    allShots = pd.read_sql_query(sql, con=engine)

    filename = './savedModels/finalized_model_knn_1000.sav'
    loaded_model = pickle.load(open(filename, 'rb'))

    filename = './savedModels/finalized_model_knn_1000_scaled_factors.sav'
    loaded_scaled_factors = pickle.load(open(filename, 'rb'))

    # allShots.fillna(allShots.mean())
    allShots = allShots.fillna(0)

    print('adjust shots and format...')
    # net center location
    goal_x = 89
    goal_y = 0
    allShots['adjx'] = abs(allShots['x_coords'])
    allShots['adjy'] = abs(allShots['y_coords'])
    allShots['goal_binary'] = np.where(allShots['event_type'] == 'Goal', 1, 0)
    allShots['wrist_shot'] = np.where((allShots['event_desc'] == 'Wrist Shot') | (allShots['event_second_type'] == 'Wrist Shot'), 1, 0)
    allShots['backhand'] = np.where((allShots['event_desc'] == 'Backhand') | (allShots['event_second_type'] == 'Backhand'), 1, 0)
    allShots['slap_shot'] = np.where((allShots['event_desc'] == 'Slap Shot') | (allShots['event_second_type'] == 'Slap Shot'), 1, 0)
    allShots['snap_shot'] = np.where((allShots['event_desc'] == 'Snap Shot') | (allShots['event_second_type'] == 'Snap Shot'), 1, 0)
    allShots['tip_in'] = np.where((allShots['event_desc'] == 'Tip-In') | (allShots['event_second_type'] == 'Tip-In'), 1, 0)
    allShots['deflected'] = np.where((allShots['event_desc'] == 'Deflected') | (allShots['event_second_type'] == 'Deflected'), 1, 0)
    allShots['wrap_around'] = np.where((allShots['event_desc'] == 'Wrap-around') | (allShots['event_second_type'] == 'Wrap-around'), 1, 0)
    allShots['none'] = np.where((allShots['event_desc'] == 'None') | (allShots['event_second_type'] == 'None'), 1, 0)
    allShots = allShots[allShots['event_type_id'] != 'MISSED_SHOT']
    # Distance and angle of location
    allShots['dist'] = np.sqrt(np.power((allShots['adjx'] - goal_x), 2) + np.power((allShots['y_coords'] - goal_y), 2))
    allShots['ang'] = 180 - (np.arctan2(allShots['adjy'], allShots['adjx']-100) * 180 / np.pi)

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

    print('making predictions...')
    predictions = loaded_model.predict_proba(X_pp)

    predictions_1 = predictions[:, 1]

    allShots_predicted = allShots
    allShots_predicted['pred'] = predictions_1

    allShots_predicted.head()

    print('deleting from db...')
    connection = engine.connect()
    result = connection.execute("""DELETE FROM nhlstats.adjusted_shots WHERE game_date > '%s'
        AND game_date < '%s'""" % (startDate, endDate))

    # print('writing to db...')
    # allShots.to_sql('adjusted_shots', schema = 'nhlstats', con=engine, if_exists='append', index=False)







    connection = engine.raw_connection()
    cursor = connection.cursor()

    # stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
    output = io.StringIO()
    # ignore the index
    toints = ['game_id', 'event_idx', 'event_id', 'period', 'away_goals', 'home_goals', 'x_coords', 'y_coords', 'team_id',
               'player1_id', 'player2_id', 'player3_id', 'player4_id', 'adjx', 'adjy', 'goal_binary', 'wrist_shot',
               'backhand', 'slap_shot', 'snap_shot', 'tip_in', 'deflected', 'wrap_around', 'none']
    allShots[toints] = allShots[toints].astype(int)
    allShots.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)
    contents = output.getvalue()
    cur = connection.cursor()
    # null values become ''
    cur.copy_from(output, 'nhlstats.adjusted_shots', null="")
    connection.commit()
    cur.close()


if __name__ == '__main__':
    script_name = sys.argv[0]
    num_args = len(sys.argv)
    args = str(sys.argv)
    print("Running: ", sys.argv[0])
    if num_args != 3:
        sys.exit('Need three arguments!')
    print("The arguments are: ", str(sys.argv))
    AdjustShots(sys.argv[1], sys.argv[2])
    AdjustShots('2010-08-01', '2011-08-01')
import pandas as pd
import datetime
from production.ignore import engine

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

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

sql = """SELECT * FROM nhlstats.adjusted_shots """
allShots = pd.read_sql_query(sql, con=engine)

cols = ['year_start', 'year_end', 'playoff', 'period_start', 'period_end', 'num_shots',
        'num_goals', 'sum_xgoals', 'avg_shoot_perc', 'avg_pred', 'wrist_shot_perc', 'backhand_perc',
        'slap_shot_perc', 'snap_shot_perc', 'tip_in_perc', 'deflected_perc', 'wrap_around_perc', 'mean_dist',
        'std_dist', 'mean_ang', 'std_ang']
results = {'year_start': [],
               'year_end': [],
               'playoff': [],
               'period_start': [],
               'period_end': [],
               'num_shots': [],
               'num_goals': [],
               'sum_xgoals': [],
                'avg_shoot_perc': [],
                'avg_pred': [],
                'wrist_shot_perc': [],
                'backhand_perc': [],
                'slap_shot_perc': [],
                'snap_shot_perc': [],
                'tip_in_perc': [],
                'deflected_perc': [],
                'wrap_around_perc': [],
                'mean_dist': [],
                'std_dist': [],
                'mean_ang': [],
                'std_ang': []
               }


for seg in segments:
    ### TESTING ###
    # seg = segments[0]
    ####

    year_start = seg[0]
    year_end = seg[1]
    playoff = seg[4]
    period_start = datetime.datetime.strptime(seg[2], '%Y-%m-%d')
    period_end = datetime.datetime.strptime(seg[3], '%Y-%m-%d')

    shots = allShots.loc[
        (allShots['game_date'] > period_start) & (allShots['game_date'] < period_end)].copy()

    describe = shots.describe()

    num_shots = shots.shape[0]
    num_goals = shots[shots['event_type_id'] == 'GOAL'].shape[0]

    results['year_start'].append(year_start)
    results['year_end'].append(year_end)
    results['playoff'].append(playoff)
    results['period_start'].append(period_start)
    results['period_end'].append(period_end)
    results['num_shots'].append(num_shots)
    results['num_goals'].append(num_goals)
    results['sum_xgoals'].append(shots['pred'].sum())
    results['avg_shoot_perc'].append( (num_goals / num_shots) if num_shots != 0 else 0)
    results['avg_pred'].append( describe.loc['mean', 'pred'])
    results['wrist_shot_perc'].append( describe.loc['mean', 'wrist_shot'])
    results['backhand_perc'].append( describe.loc['mean', 'backhand'])
    results['slap_shot_perc'].append( describe.loc['mean', 'slap_shot'])
    results['snap_shot_perc'].append( describe.loc['mean', 'snap_shot'])
    results['tip_in_perc'].append( describe.loc['mean', 'tip_in'])
    results['deflected_perc'].append( describe.loc['mean', 'deflected'])
    results['wrap_around_perc'].append( describe.loc['mean', 'wrap_around'])
    results['mean_dist'].append( describe.loc['mean', 'dist'])
    results['std_dist'].append( describe.loc['std', 'dist'])
    results['mean_ang'].append( describe.loc['mean', 'ang'])
    results['std_ang'].append( describe.loc['std', 'ang'])


shotResults = pd.DataFrame(data=results, columns=cols)
shotResults.to_sql('yearly_averages', schema='nhlstats', con=engine, if_exists='append', index=False)


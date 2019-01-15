import pandas as pd
from ignore import engine
import sys

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)


def GenerateAndPushYearlyAverages(gameSeason):
    ### TESTING ###
    # gameSeason = 'all'
    ####

    # Either generate for all or generate based on specific season
    print('fetching adjusted shots...')
    if gameSeason == "all":
        sql = """SELECT adjs.*, gd.game_type, gd.game_season FROM nhlstats.adjusted_shots AS adjs
                LEFT JOIN nhlstats.game_details AS gd ON gd.gamepk = adjs.game_id
                WHERE (gd.game_type = 'R' OR gd.game_type = 'P')
                AND period_type != 'SHOOTOUT'"""
        allShots = pd.read_sql_query(sql, con=engine)
    else:
        sql = """SELECT adjs.*, gd.game_type, gd.game_season FROM nhlstats.adjusted_shots AS adjs
                        LEFT JOIN nhlstats.game_details AS gd ON gd.gamepk = adjs.game_id
                        WHERE (gd.game_type = 'R' OR gd.game_type = 'P')
                        AND gd.game_season = %s
                        AND period_type != 'SHOOTOUT'""" % gameSeason
        allShots = pd.read_sql_query(sql, con=engine)

    print('calculating metrics...')

    def get_month(row):
        return row['game_date'].month

    def goal_and_value(row, label):
        return row[label] & row['goal_binary']

    def pred_value(row, label):
        if (row[label] == 1):
            return row['pred']
        else:
            return 0

    allShotsTempCopy = allShots.copy()

    allShotsTempCopy['month'] = 'year'
    allShots['month'] = allShots.apply(lambda row: get_month(row), axis=1)

    allShots = pd.concat([allShots, allShotsTempCopy])

    allShots['wrist_shot_goal'] = allShots.apply(lambda row: goal_and_value(row, 'goal_binary'), axis=1)
    allShots['backhand_goal'] = allShots.apply(lambda row: goal_and_value(row, 'backhand'), axis=1)
    allShots['slap_shot_goal'] = allShots.apply(lambda row: goal_and_value(row, 'slap_shot'), axis=1)
    allShots['snap_shot_goal'] = allShots.apply(lambda row: goal_and_value(row, 'snap_shot'), axis=1)
    allShots['tip_in_goal'] = allShots.apply(lambda row: goal_and_value(row, 'tip_in'), axis=1)
    allShots['deflected_goal'] = allShots.apply(lambda row: goal_and_value(row, 'deflected'), axis=1)
    allShots['wrap_around_goal'] = allShots.apply(lambda row: goal_and_value(row, 'wrap_around'), axis=1)

    allShots['wrist_shot_pred'] = allShots.apply(lambda row: pred_value(row, 'wrist_shot'), axis=1)
    allShots['backhand_pred'] = allShots.apply(lambda row: pred_value(row, 'backhand'), axis=1)
    allShots['slap_shot_pred'] = allShots.apply(lambda row: pred_value(row, 'slap_shot'), axis=1)
    allShots['snap_shot_pred'] = allShots.apply(lambda row: pred_value(row, 'snap_shot'), axis=1)
    allShots['tip_in_pred'] = allShots.apply(lambda row: pred_value(row, 'tip_in'), axis=1)
    allShots['deflected_pred'] = allShots.apply(lambda row: pred_value(row, 'deflected'), axis=1)
    allShots['wrap_around_pred'] = allShots.apply(lambda row: pred_value(row, 'wrap_around'), axis=1)

    tempShots = allShots.copy()

    shotsGrouped = tempShots.groupby(['game_season', 'month', 'game_type'])

    # Define the aggregation procedure outside of the groupby operation
    aggregations = {
        'goal_binary': 'sum',
        'wrist_shot': 'sum',
        'backhand': 'sum',
        'slap_shot': 'sum',
        'snap_shot': 'sum',
        'tip_in': 'sum',
        'deflected': 'sum',
        'wrap_around': 'sum',

        'wrist_shot_pred': 'sum',
        'backhand_pred': 'sum',
        'slap_shot_pred': 'sum',
        'snap_shot_pred': 'sum',
        'tip_in_pred': 'sum',
        'deflected_pred': 'sum',
        'wrap_around_pred': 'sum',

        'wrist_shot_goal': 'sum',
        'backhand_goal': 'sum',
        'slap_shot_goal': 'sum',
        'snap_shot_goal': 'sum',
        'tip_in_goal': 'sum',
        'deflected_goal': 'sum',
        'wrap_around_goal': 'sum',
        'dist': 'mean',
        'ang': 'mean',
        'pred': 'sum'
    }

    metrics = shotsGrouped.agg(aggregations)
    counts = shotsGrouped.agg({'pred': 'count'})
    metrics = metrics.merge(counts, on=['game_season', 'month', 'game_type'], how='inner',
                            suffixes=('', '_count'))

    def get_perc(row, label):
        return (row[label] / row['pred_count'])

    def get_pred_perc(row, label, label2):
        if row[label2] == 0:
            return 0
        return (row[label] / row[label2])

    def get_perc_specific(row, label, countLabel):
        if row[countLabel] == 0:
            return 0
        return (row[label] / row[countLabel])

    metrics.reset_index()
    metrics['shooting_perc'] = metrics.apply(lambda row: get_perc(row, 'goal_binary'), axis=1)
    metrics['pred_shooting_perc'] = metrics.apply(lambda row: get_perc(row, 'goal_binary'), axis=1)

    metrics['goals_aa_per_shot'] = metrics['shooting_perc'] - metrics['pred_shooting_perc']

    metrics['wrist_shot_freq'] = metrics.apply(lambda row: get_perc(row, 'wrist_shot'), axis=1)
    metrics['backhand_freq'] = metrics.apply(lambda row: get_perc(row, 'backhand'), axis=1)
    metrics['slap_shot_freq'] = metrics.apply(lambda row: get_perc(row, 'slap_shot'), axis=1)
    metrics['snap_shot_freq'] = metrics.apply(lambda row: get_perc(row, 'snap_shot'), axis=1)
    metrics['tip_in_freq'] = metrics.apply(lambda row: get_perc(row, 'tip_in'), axis=1)
    metrics['deflected_freq'] = metrics.apply(lambda row: get_perc(row, 'deflected'), axis=1)
    metrics['wrap_around_freq'] = metrics.apply(lambda row: get_perc(row, 'wrap_around'), axis=1)
    metrics['pred_shooting_perc'] = metrics.apply(lambda row: get_perc(row, 'pred'), axis=1)

    metrics['pred_shooting_perc_wrist_shot'] = metrics.apply(
        lambda row: get_pred_perc(row, 'wrist_shot_pred', 'wrist_shot'), axis=1)
    metrics['pred_shooting_perc_backhand'] = metrics.apply(lambda row: get_pred_perc(row, 'backhand_pred', 'backhand'),
                                                           axis=1)
    metrics['pred_shooting_perc_slap_shot'] = metrics.apply(
        lambda row: get_pred_perc(row, 'slap_shot_pred', 'slap_shot'), axis=1)
    metrics['pred_shooting_perc_snap_shot'] = metrics.apply(
        lambda row: get_pred_perc(row, 'snap_shot_pred', 'snap_shot'), axis=1)
    metrics['pred_shooting_perc_tip_in'] = metrics.apply(lambda row: get_pred_perc(row, 'tip_in_pred', 'tip_in'),
                                                         axis=1)
    metrics['pred_shooting_perc_deflected'] = metrics.apply(
        lambda row: get_pred_perc(row, 'deflected_pred', 'deflected'), axis=1)
    metrics['pred_shooting_perc_wrap_around'] = metrics.apply(
        lambda row: get_pred_perc(row, 'wrap_around_pred', 'wrap_around'), axis=1)


    metrics['wrist_shot_shooting_perc'] = metrics.apply(
        lambda row: get_perc_specific(row, 'wrist_shot_goal', 'wrist_shot'), axis=1)
    metrics['backhand_shooting_perc'] = metrics.apply(lambda row: get_perc_specific(row, 'backhand_goal', 'backhand'),
                                                      axis=1)
    metrics['slap_shot_shooting_perc'] = metrics.apply(
        lambda row: get_perc_specific(row, 'slap_shot_goal', 'slap_shot'), axis=1)
    metrics['snap_shot_shooting_perc'] = metrics.apply(
        lambda row: get_perc_specific(row, 'snap_shot_goal', 'snap_shot'), axis=1)
    metrics['tip_in_shooting_perc'] = metrics.apply(lambda row: get_perc_specific(row, 'tip_in_goal', 'tip_in'), axis=1)
    metrics['deflected_shooting_perc'] = metrics.apply(
        lambda row: get_perc_specific(row, 'deflected_goal', 'deflected'), axis=1)
    metrics['wrap_around_shooting_perc'] = metrics.apply(
        lambda row: get_perc_specific(row, 'wrap_around_goal', 'wrap_around'), axis=1)




    metrics = metrics.reset_index()
    columns={
        'game_season': 'year_code',
        'pred_count': 'num_shots',
        'goal_binary': 'num_goals',
        'pred': 'sum_xgoals',
        'shooting_perc': 'avg_shoot_perc',
        'pred_shooting_perc': 'avg_xgoals',

        'pred_shooting_perc_wrist_shot': 'avg_xgoals_wrist_shot',
        'pred_shooting_perc_backhand': 'avg_xgoals_backhand',
        'pred_shooting_perc_slap_shot': 'avg_xgoals_slap_shot',
        'pred_shooting_perc_snap_shot': 'avg_xgoals_snap_shot',
        'pred_shooting_perc_tip_in': 'avg_xgoals_tip_in',
        'pred_shooting_perc_deflected': 'avg_xgoals_deflected',
        'pred_shooting_perc_wrap_around': 'avg_xgoals_wrap_around',

        'wrist_shot': 'wrist_shot_num',
        'backhand': 'backhand_num',
        'slap_shot': 'slap_shot_num',
        'snap_shot': 'snap_shot_num',
        'tip_in': 'tip_in_num',
        'deflected': 'deflected_num',
        'wrap_around': 'wrap_around_num',
        'dist': 'mean_dist',
        'ang': 'mean_ang'
    }


    dropColumns=[
        'wrist_shot_goal',
        'backhand_goal',
        'slap_shot_goal',
        'snap_shot_goal',
        'tip_in_goal',
        'deflected_goal',
        'wrap_around_goal'
    ]
    if gameSeason == "all":
        dropColumns.append('index')


    fomattedDf = metrics.rename(index=str, columns=columns).round(3)
    fomattedDf = fomattedDf.drop(dropColumns, axis=1)

    print('deleting from db...')
    if gameSeason == "all":
        sql = """DELETE from nhlstats.yearly_averages"""
    else:
        sql = """DELETE FROM nhlstats.yearly_averages
                            WHERE year_code = %s""" % gameSeason
    connection = engine.connect()
    connection.execute(sql)

    print('pushing to db...')
    fomattedDf.to_sql('yearly_averages', schema='nhlstats', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    script_name = sys.argv[0]
    num_args = len(sys.argv)
    args = str(sys.argv)
    print("Running: ", sys.argv[0])
    if num_args != 2:
        sys.exit('Need two arguments!')
    print("The arguments are: ", str(sys.argv))
    GenerateAndPushYearlyAverages(sys.argv[1])
    # GenerateAndPushYearlyAverages("all")
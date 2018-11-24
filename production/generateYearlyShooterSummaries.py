import pandas as pd
from production.ignore import engine
import sys

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)

def GenerateAndPushShooterSummaries(gameSeason):

    # Either generate for all or generate based on specific season
    if gameSeason == "all":
        sql = """SELECT adjs.*, gd.game_type, gd.game_season FROM nhlstats.adjusted_shots AS adjs
            LEFT JOIN nhlstats.game_details AS gd ON gd.gamepk = adjs.game_id
            WHERE gd.game_type = 'R' OR gd.game_type = 'P'"""
        allShots = pd.read_sql_query(sql, con=engine)
    else:
        sql = """SELECT adjs.*, gd.game_type, gd.game_season FROM nhlstats.adjusted_shots AS adjs
                    LEFT JOIN nhlstats.game_details AS gd ON gd.gamepk = adjs.game_id
                    WHERE (gd.game_type = 'R' OR gd.game_type = 'P')
                    AND gd.game_season = %s""" % gameSeason
        allShots = pd.read_sql_query(sql, con=engine)

    sql = """SELECT * FROM nhlstats.yearly_averages"""
    yearlyAverages = pd.read_sql_query(sql, con=engine)

    def get_month(row):
        return row['game_date'].month

    allShots['month'] = allShots.apply(lambda row: get_month(row), axis=1)

    tempShots = allShots.copy()

    allShots = allShots.groupby(['player1_id', 'game_season', 'month', 'game_type'])

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
        'dist': 'mean',
        'ang': 'mean',
        'pred': ['sum', 'count', 'std']
    }

    metrics = allShots.agg(aggregations)

    tempMetrics = metrics.copy()

    ranks = metrics.rank(pct=True)

    sql = """DELETE FROM nhlstats.yearly_shooter_summaries 
                WHERE id = %s""" % playerid
    connection = engine.connect()
    connection.execute(sql)

    shotResults = pd.DataFrame(data=results, columns=cols)
    shotResults['id'] = playerid
    shotResults.to_sql('yearly_shooter_summaries', schema='nhlstats', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    script_name = sys.argv[0]
    num_args = len(sys.argv)
    args = str(sys.argv)
    print("Running: ", sys.argv[0])
    if num_args != 2:
        sys.exit('Need two arguments!')
    print("The arguments are: ", str(sys.argv))
    GenerateAndPushShooterSummaries(sys.argv[1])
    # GenerateAndPushShooterSummaries("all")
    # GenerateAndPushShooterSummaries("20182019")


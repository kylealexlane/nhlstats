import pandas as pd
import sys
import time
import datetime
from production.fetchData.getGameList import fetch_games_create_csv
from production.fetchData.importData import fetchGameAndPopulate
from production.transformData.adjustShots import AdjustShots
from production.fetchData.generatePlayerInfo import GeneratePlayerInfo
from production.fetchData.generateTeamInfo import GenerateTeamInfo
from production.yearlySummaries.generateYearlyAverages import GenerateAndPushYearlyAverages
from production.yearlySummaries.generateYearlyGoalieSummaries import GenerateAndPushGoalieSummaries
from production.yearlySummaries.generateYearlyShooterSummaries import GenerateAndPushShooterSummaries
from production.yearlySummaries.generateYearlyTeamAgainstSummaries import GenerateAndPushTeamAgainstSummaries
from production.yearlySummaries.generateYearlyTeamShooterSummaries import GenerateAndPushTeamShooterSummaries



pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)


def import_daily(startDate, endDate, gameSeason):
    print("\n\n\nRunning daily import... \n\n\n")
    s = time.time()

    print("\n\n\nFetching games list... \n\n\n")
    fetch_games_create_csv(startDate, endDate)

    print("\n\n\nImporting Data \n\n\n....")
    fetchGameAndPopulate(startDate, endDate)

    print("\n\n\nAdjusting Shots.... \n\n\n")
    AdjustShots(startDate, endDate)

    print("\n\n\nGenerate player info.... \n\n\n")
    GeneratePlayerInfo(justnew=True)

    print("\n\n\nGenerate team info.... \n\n\n")
    GenerateTeamInfo()

    print("\n\n\nGenerate yearly averages.... \n\n\n")
    GenerateAndPushYearlyAverages(gameSeason)

    print("\n\n\nGenerate goalie summaries.... \n\n\n")
    GenerateAndPushGoalieSummaries(gameSeason)

    print("\n\n\nGenerate shooter summaries.... \n\n\n")
    GenerateAndPushShooterSummaries(gameSeason)

    print("\n\n\nGenerate team against summaries.... \n\n\n")
    GenerateAndPushTeamAgainstSummaries(gameSeason)

    print("\n\n\nGenerate team shooter summaries.... \n\n\n")
    GenerateAndPushTeamShooterSummaries(gameSeason)

    print("\n\n\nDone! Time taken....")
    print(time.time() - s)
    print("\n\n\n")



def run_import_today():
    startDate = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')
    endDate = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    gameSeason = '20182019'
    print('Running between dates: ', startDate, endDate)
    import_daily(startDate, endDate, gameSeason)



if __name__ == '__main__':
    script_name = sys.argv[0]
    num_args = len(sys.argv)
    args = str(sys.argv)
    print("Running: ", sys.argv[0])
    if num_args != 3:
        sys.exit('Need three arguments!')
    print("The arguments are: ", str(sys.argv))
    import_daily(sys.argv[1], sys.argv[2])

    # import_daily('2019-01-03', '2019-01-06', '20182019')
    # import_daily('2018-08-01', '2019-08-01', '20182019)
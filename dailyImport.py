import pandas as pd
import sys
import time
from getGameList import fetch_games_create_csv
from importData import fetchGameAndPopulate
from adjustShots import AdjustShots
from generatePlayerInfo import GeneratePlayerInfo
from generateTeamInfo import GenerateTeamInfo
from generateYearlyAverages import GenerateAndPushYearlyAverages
from generateYearlyGoalieSummaries import GenerateAndPushGoalieSummaries
from generateYearlyShooterSummaries import GenerateAndPushShooterSummaries
from generateYearlyTeamAgainstSummaries import GenerateAndPushTeamAgainstSummaries
from generateYearlyTeamShooterSummaries import GenerateAndPushTeamShooterSummaries



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
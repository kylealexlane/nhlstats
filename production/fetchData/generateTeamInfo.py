import pandas as pd
import requests
import psycopg2
from ignore import engine, hostname, username, password, database
import time
import io

pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_row', 100)
pd.set_option('display.max_columns', 50)


def GenerateTeamInfo():
    response = requests.get("https://statsapi.web.nhl.com/api/v1/teams")
    teamData = response.json()

    for team in teamData["teams"]:
        # Testing
        # team = teamData["teams"][0]
        #
        print(team)
        id = team["id"]

        print('generating info for team {team}...'.format(team=id))
        s = time.time()

        venue = team["venue"] if "venue" in team else {}  # Can't rely on data being there
        division =  team["division"] if "division" in team else {}
        conference = team["conference"] if "conference" in team else {}
        franchise = team["franchise"] if "franchise" in team else {}

        team["venue"] = venue["id"] if "id" in venue else ""  # Can't rely on data being there
        team["venue_name"] = venue["name"] if "name" in venue else ""
        team["division"] = division["id"] if "id" in division else ""
        team["division_name"] = division["name"] if "name" in division else ""
        team["conference"] = conference["id"] if "id" in conference else ""
        team["conference_name"] = conference["name"] if "name" in conference else ""
        team["franchise"] = franchise["franchiseId"] if "franchiseId" in franchise else ""
        team["franchise_name"] = franchise["teamName"] if "teamName" in franchise else ""


        dteam = pd.DataFrame(team, index=[0])

        myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
        cur = myConnection.cursor()

        cur.execute('DELETE FROM nhlstats.teams WHERE id = {team} ;'.format(team=str(id)))
        myConnection.commit()
        cur.close()
        myConnection.close()


        # Change column names to correct names
        cols = {
            "teamName": "team_name",
            "locationName": "location_name",
            "firstYearOfPlay": "first_year_of_play",
            "shortName": "short_name",
            "officialSiteUrl": "official_site_url",
            "franchiseId": "franchise_id"
        }
        dteam = dteam.rename(index=str, columns=cols)

        # Push ranked data to ranks table
        print(time.time() - s)
        print('pushing to db...')
        s = time.time()

        columns = list(dteam.columns.values)

        output = io.StringIO()
        # ignore the index
        dteam.to_csv(output, sep='\t', header=False, index=False)
        output.getvalue()
        # jump to start of stream
        output.seek(0)

        connection = engine.raw_connection()
        cursor = connection.cursor()
        # null values become ''
        cursor.copy_from(output, 'teams', null="", columns=(columns))
        connection.commit()
        cursor.close()
        print(time.time() - s)



if __name__ == '__main__':
    GenerateTeamInfo()

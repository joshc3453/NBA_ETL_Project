from nba_api.stats.endpoints import CommonTeamRoster
from nba_api.stats.static import teams
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta

teams_list = teams.get_teams()

team_list_ids = [team['id'] for team in teams_list]

# This will be used to dynamically create roster tables for each team
ids_and_names_list = [{'id': team['id'], 'full_name': team['full_name']} for team in teams_list]


for team in teams_list[:3]:
    # CommonTeamRoster(team_id=team['id']).get_data_frames()[0]
    team_name = team['full_name'].lower().replace(' ', '_')
    df = CommonTeamRoster(team_id=team['id']).get_data_frames()[0]
    df.columns = [c.lower() for c in df.columns]
    df.rename(columns={'teamid': 'team_id', 'leagueid': 'league_id'})
    print(df.head())
    time.sleep(0.6)

## Next Steps - Convert birth_date to datetime

# # Original date string to be converted
# date_str = "APR 09, 1999"

# # Convert the string to a datetime object to enable manipulation.
# # "%b %d, %Y" specifies the format of the input string: abbreviated month, day, and full year.
# date_obj = datetime.strptime(date_str, "%b %d, %Y")

# # Format the datetime object into a string with the desired format.
# # "%Y-%m-%d" specifies the output format: full year, month, and day.
# formatted_date_str = date_obj.strftime("%Y-%m-%d")

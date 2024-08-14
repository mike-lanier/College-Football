import requests
from datetime import timedelta
from datetime import datetime
import pandas as pd


response = requests.get('http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard')
data = response.json()

season_calendar = data['leagues'][0]['calendar']

for i in range(len(season_calendar)):
    df = pd.json_normalize(season_calendar, record_path=['entries'])

df['startDate'] = pd.to_datetime(df['startDate'])
df['weekStart'] = df['startDate'].dt.strftime('%Y%m%d')

df['endDate'] = pd.to_datetime(df['endDate'])
df['endDate'] = df['endDate'] - timedelta(days=1)
df['weekEnd'] = df['endDate'].dt.strftime('%Y%m%d')

season_year = datetime.today().strftime('%Y')

df.to_csv(season_year + '.csv', index=False)


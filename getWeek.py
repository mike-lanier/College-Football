import pandas as pd
from datetime import datetime
from datetime import timedelta
import os


def findSeasonFile(folder, season):
    for filename in os.listdir(folder):
        if filename.split('.csv')[0] == season:
            return os.path.join(folder, filename)
        else:
            print("No matching file found")

def createDataFrame():
    current_year = datetime.today().strftime("%Y")
    next_year = (datetime.today() + timedelta(days=365)).strftime("%Y")
    season = current_year + '-' + next_year
    folder_path = './data/csv_season/'

    data = findSeasonFile(folder_path, season)
    df = pd.read_csv(data)
    return df

def calculateWeekStartEnd():
    df = createDataFrame()

    df['startDate'] = pd.to_datetime(df['startDate'])
    df['endDate'] = pd.to_datetime(df['endDate'])

    current_date = datetime.today()

    filtered_df = df[(df['startDate'] <= current_date) & (df['endDate'] >= current_date)]

    print(filtered_df)






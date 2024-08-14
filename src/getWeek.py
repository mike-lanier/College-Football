import pandas as pd
from datetime import date
from datetime import timedelta
import os


def findSeasonFile(folder, season):
    for filename in os.listdir(folder):
        if filename.split('.csv')[0] == season:
            return os.path.join(folder, filename)
        else:
            print("No matching file found")

def createDataFrame():
    current_year = date.today().strftime("%Y")
    next_year = (date.today() + timedelta(days=365)).strftime("%Y")
    season = current_year + '-' + next_year
    folder_path = './data/season_calendar/'

    data = findSeasonFile(folder_path, season)
    df = pd.read_csv(data, index_col=None)
    return df

def calculateWeek():
    df = createDataFrame()

    df['startDate'] = pd.to_datetime(df['startDate'])
    df['startDate'] = df['startDate'].dt.date
    df['endDate'] = pd.to_datetime(df['endDate'])
    df['endDate'] = df['endDate'].dt.date

    current_date = date.today() + timedelta(days=12)

    filtered_df = df[(df['startDate'] <= current_date) & (df['endDate'] >= current_date)]
    filtered_df = filtered_df.reset_index(drop=True)

    return filtered_df

def grabDateFilters():
    current_week = calculateWeek()
    start = current_week.loc[0, 'weekStart']
    end = current_week.loc[0, 'weekEnd']

    return start, end


week_start = grabDateFilters()[0]
week_end = grabDateFilters()[1]




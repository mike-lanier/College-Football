import pandas as pd
from datetime import date, timedelta
import os

class CalculateWeek:
    def __init__(self, folder_path='./data/season_calendar/'):
        self.folder_path = folder_path
        self.current_year = date.today().strftime("%Y")
        self.next_year = (date.today() + timedelta(days=365)).strftime("%Y")
        self.season = self.current_year + '-' + self.next_year
        self.df = None
        self.last_week = None

    def find_season_file(self):
        for filename in os.listdir(self.folder_path):
            if filename.split('.csv')[0] == self.season:
                return os.path.join(self.folder_path, filename)
            else:
                return "No matching file found"

    def create_data_frame(self):
        file = self.find_season_file()
        self.df = pd.read_csv(file, index_col=None)
        self.df['startDate'] = pd.to_datetime(self.df['startDate']).dt.date
        self.df['endDate'] = pd.to_datetime(self.df['endDate']).dt.date

    def calculate_week(self):
        self.create_data_frame()

        last_week_date = date.today() - timedelta(days=7) # To ensure game outcomes are included in schedule files

        filtered_df = self.df[(self.df['startDate'] <= last_week_date) & (self.df['endDate'] >= last_week_date)]
        self.last_week = filtered_df.reset_index(drop=True)

    def grab_date_filters(self):
        self.calculate_week()

        if self.last_week.empty:
            raise ValueError("No data available for last week")
        else:
            start = self.last_week.loc[0, 'weekStart']
            end = self.last_week.loc[0, 'weekEnd']
            season_week = self.current_year + '_' + self.last_week.loc[0, 'label']
            
        return start, end, season_week

    def get_week_start(self):
        week_start = str(self.grab_date_filters()[0])
        return week_start
    
    def get_week_end(self):
        week_end = str(self.grab_date_filters()[1])
        return week_end
    
    def get_week_id(self):
        week_id = self.grab_date_filters()[2]
        return week_id
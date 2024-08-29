# College Football

### ELT/ETL of game data from the college football season

- The focus is to practically apply common data tools and processing methods
- Development is done with Python, local PostgreSQL, local dbt and AWS


## Workflow

- At the beginning of every season, a script will run to fetch an API response holding the entire season schedule and create a calendar to reference weekly
- Once the season starts, another script calculates the date range of the previous week
- The date range is used in another API request in order to fetch and create the weekly schedule file with results of the previous week's matchups
- The schedule file is uploaded to an S3 bucket, where it is then parsed to collect game IDs
- Game IDs are used in another API request to collect team details and play by play information for each game
- Files are created and stored by game ID in the S3 bucket
- Schedule files and game files are pre-processed to cut down on currently unneeded data and loaded into raw tables in PostgreSQL
- dbt models parse the JSON dictionaries and lists, and incrementally load to prod tables based on last timestamp or existing values
- The resulting tables can be queried and used to create datasets for a prediction model


## Notes/Learnings

- Loading the full content of json files to postgres instead of pre-processing would be more direct, but the pre-processing to raw tables for schedules and game/play data makes for simpler transformations with dbt, ultimately saving me time
- Because of using Postgres, certain foreign keys were defined in the dbt schema.yml file in a best practice to protect integrity. Others were ignored/not defined to bypass errors otherwise (those referencing team_id). Since I'm essentially using postgres as a warehouse though, I'm not overly concerned about the foreign keys



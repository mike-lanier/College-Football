# College Football

### My personal project to collect and analyze game data from the full 2023-2024 college football season

- The focus is to display a practical application of common data tools and processing methods
- Development is done locally for now, using Python, PostgreSQL, dbt and eventually Airflow


## Notes/Learnings

- Loading the full content of json files to postgres instead of pre-processing would be more direct, but the pre-processing to raw tables for schedules and game/play data makes for simpler transformations with dbt, ultimately saving me time
- There's no significant impact to performance this way because of the size of the data, but if it were to grow, considerations around resources and compute costs for running the pre-processing scripts vs. processing primarily in-database could be taken, especially if in the cloud


# College Football

### ELT/ETL of game data from the college football season

- The focus is to practically apply common data tools and processing methods
- Development is done with Python, local PostgreSQL, local dbt and AWS


## Workflow
### Fetching, parsing and storing API response data in JSON format

- 


## Notes/Learnings

- Loading the full content of json files to postgres instead of pre-processing would be more direct, but the pre-processing to raw tables for schedules and game/play data makes for simpler transformations with dbt, ultimately saving me time
- There's no general/significant impact to performance this way because of the size of the data, but if it were to grow, considerations around resources and compute costs for running the pre-processing scripts vs. processing primarily in-database could be taken, especially if in the cloud
- Because of using Postgres, certain foreign keys were defined in the dbt schema.yml file in a best effort to protect integrity. Others were ignored/not defined to bypass errors otherwise (those referencing team_id). Since I'm essentially using postgres as a warehouse though, I'm not overly concerned about the foreign keys



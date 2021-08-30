# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Project Summary

In this project, I built an ETL pipeline for a database hosted on Redshift. I loaded the data from S3 to staging tables on Redshift and executed SQL statements that created the analytics tables from these staging tables.

## Datasets

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

- Log data json path: `s3://udacity-dend/log_json_path.json`

## Schema for Song Play Analysis

Using the song and event datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table

- songplays - records in event data associated with song plays i.e. records with page NextSong
    > songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

- users - users in the app
    > user_id, first_name, last_name, gender, level
- songs - songs in music database
    > song_id, title, artist_id, year, duration
- artists - artists in music database
    > artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    > start_time, hour, day, week, month, year, weekday
    
## Project Template

The project template includes four files:

- `create_table.py` is where fact and dimension tables for the star schema in Redshift is created.
- `etl.py` is where I loaded data from S3 into staging tables on Redshift and then processed that data into analytics tables on Redshift.
- `sql_queries.py` is where I defined SQL statements, which will be imported into the two other files above.
- `README.md` discussion on processes and decisions for this ETL pipeline

## Running the Project

- Add redshift database and IAM role info to `dwh.cfg`
- Test by running `create_tables.py` and checking the table schemas in the redshift database
- Create pipeline by running `etl.py`


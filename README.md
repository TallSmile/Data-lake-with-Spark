# Data Modeling with Postgres

## Purpose

The purpose of this project is to analyze data simulated by [eventsim](https://github.com/Interana/eventsim). The simulated data consists of songs and user activity from music streaming app. The main goal of the project is to  construct a postgres database with tables designed to optimize queries on song play analysis. Using the song and log datasets, an ETL pipeline is used to create star schema for this analysis.

## Getting Started

Before proceeding with deployment please create a S3 bucket for the storage of tables.

## Deployment

The project should be deployed on AWS. Configuration variables are read from `dl.cfg`.

* Copy example configuration file and fill it in.

```Shell
cp ./src/dl.template.cfg ./src/dl.cfg
```

## Usage

We leverage Docker containers for the ETL pipeline and database hosting.

1. Run docker compose in order to spin up required environment.

```Shell
docker-compose up
```

2. Access Jupyter environment via link provided by Docker.

3. In order to create data lake you need to execute ETL script

```Shell
python /src/etl.py
```

## Schema for Song Play Analysis

Below is presented the schema of the data lake. In order to optimize queries for song play analysis and reduce data duplication, we use **star scheme**.

### Fact Table

Records in log data associated with song plays i.e. records with page NextSong

| **songplays** |
|---------------|
| songplay_id   |
| start_time    |
| user_id       |
| level         |
| song_id       |
| artist_id     |
| session_id    |
| location      |
| user_agent    |

### Dimension Tables

Users in the app

| **users** |
|-----------|
| user_id   |
| first_name|
| last_name |
| gender    |
| level     |

Songs in music table

| **songs** |
|-----------|
| song_id   |
| title     |
| artist_id |
| year      |
| duration  |

Artists in music table

| **artists**  |
|--------------|
| artist_id    |
| name         |
| location     |
| lattitude    |
| longitude    |
|---------------|

Timestamps of records in songplays broken down into specific units

| **time**     |
|--------------|
| start_time   |
| hour         |
| day          |
| week         |
| month        |
| year         |
| weekday      |

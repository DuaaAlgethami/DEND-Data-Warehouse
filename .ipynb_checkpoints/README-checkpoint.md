# Project: Data Warehouse

### About Project

   The company Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

   My role specifically is build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of fact and dimensional tables . I loaded data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

### Project Template
  The project includes four files:
1. dwh.cfg to store configuration files that contains info about redshift database, IAM and S3.
2. create_tables.py drops and creates all tables.
3. etl. py reads and processes files from song_data and log_data and loads them into your tables.
5. sql_queries.py contains all sql queries .

### Schema for Song Play Analysis
Using the song and log datasets, I created the star schema has 1 fact table (songplays), and 4 dimension tables (users, songs, artists, time) and two staging tables {Staging events (Load log dataset) , Staging songs (Load songs dataset)}

```
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id integer IDENTITY(0,1),
        start_time timestamp,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id integer,
        location varchar,
        user_agent varchar,
        PRIMARY KEY (songplay_id)
```
        
``` 
     CREATE TABLE IF NOT EXISTS users(
        user_id varchar,
        first_name varchar,
        last_name varchar, 
        gender varchar,
        level varchar,
        PRIMARY KEY (user_id)
        )
```
  
```
      CREATE TABLE IF NOT EXISTS songs(
        song_id varchar,
        title varchar, 
        artist_id varchar, 
        year integer , 
        duration float,
        PRIMARY KEY (song_id)
        )
```
       
```
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar, 
        name varchar,
        location varchar, 
        latitude float, 
        longitude float,
        PRIMARY KEY (artist_id)
        )
```
        
``` 
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp,
        hour integer,
        day integer,
        week integer,
        month integer ,
        year integer ,
        weekday varchar,
        PRIMARY KEY (start_time)
        )
```

``` 
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer ,
        lastName varchar,
        length float ,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId varchar
        )
```
 
```
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs integer,
        artist_id  varchar, 
        artist_latitude float, 
        artist_longitude float,
        artist_location varchar,
        artist_name varchar, 
        song_id varchar,
        title varchar, 
        duration float, 
        year integer
        )
```


### Run Pipeline
1. Run IaC file to ensure that cluster is creating and running.
2. Run create_tables.py to create all tables.
3. Run etl. py to load data into tabels.
4. Go to Amazon Redshift > Cluster > Query Editor> Preview Data to make sure all the tables were successfully created and data were loaded into it.
5. Clean up cluster and resources.

![Image](https://k.top4top.io/p_1773rf3ub2.jpg)
![Image](https://j.top4top.io/p_1773qjas51.jpg)
#### reference:
1. http://blog.leonelatencio.com/infrastructure-as-code-aws-redshift/
2. https://docs.aws.amazon.com/cloudhsm/latest/userguide/activate-cluster.html


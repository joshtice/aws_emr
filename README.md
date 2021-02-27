# Sparkify ETL Pipeline

## Summary

To support the Sparkify Data Analytics team, a data pipeline was developed to extract user activity from JSON logs stored on S3. Using a Spark app running on Amazon EMR, the data were transformed into parquet tables and then re-uploaded to S3. The parquet tables utilize a star schema to optimize the data for querying.

## Usage

The ETL pipeline requires an active cluster on Amazon EMR, an S3 bucket containing the input data, and a designated S3 bucket for depositing the transformed data. To run the script, do the following:  

- Ensure that the AWS command line interface is installed and set up on your local machine  
- Modify the included bash script by entering a valid AWS access key and an S3 bucket URI where indicated  
- Upload the etl.py script to an active S3 bucket  
- Create an S3 bucket as a repository for output data  
- Launch an EMR cluster by calling the included bash script: ```bash ./launch_emr_cluster.sh```
- Navigate to the online AWS EMR management dashboard  
- Go to "Clusters" and select the active cluster you would like to use  
- Under the "Steps" tab, click the "Add step" button  
- Enter the following information in the pop-up:
  - Step type: Spark application  
  - Name: <Enter your choice of name>
  - Deploy mode: Cluster  
  - Spark-submit options: <Leave blank>  
  - Application location: <Enter S3 URI for etl.py>  
  - Arguments: s3://udacity-dend/ <enter S3 URI for output data>  
  - Action on failure: Continue
- Click the "Add" button

## File Descriptions

- README.md: This file, which contains documentation for the ETL pipeline
- launch_emr_cluster.sh: bash script for launching AWS EMR cluster
- etl.py: The production ETL Spark app that drives the pipeline

## Data Table Schema

The transformed data consists of five parquet tables, whose schemata are outlined below:

### artists table
```
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

### songs table
```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- year: integer (nullable = true)
 |-- artist_id: string (nullable = true)
```   
    
### time table
```
root
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- weekday: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```    

### users table
```
root
 |-- user_id: string (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```

### songplays table
```
root
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- songplay_id: long (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```

## Example Query

Print the top ten artists with the most song plays:

```
>>> from pyspark.sql import functions as F
>>> artists = spark.read.parquet('<path to artists table>')
>>> songplays = spark.read.parquet('<path to songplays table>')
>>> a = artists.alias('a')
>>> s = songplays.alias('s')
>>> j = s.join(a, s.artist_id == a.artist_id)
>>> j.select(a.name, s.songplay_id).groupBy(a.name).agg(F.count(s.songplay_id).alias('count')).orderBy('count', ascending=False).limit(10).show()

+--------------------+-----+
|                name|count|
+--------------------+-----+
|       Dwight Yoakam|   37|
|    Carleen Anderson|   17|
|      Eli Young Band|   13|
|         Gemma Hayes|   13|
|       Frozen Plasma|   13|
|Working For A Nuc...|   13|
|Kid Cudi / Kanye ...|   10|
|            Kid Cudi|   10|
|       Lonnie Gordon|    9|
|          Ron Carter|    9|
+--------------------+-----+
```

## Authors

- Joshua Tice
- The Udacity Team
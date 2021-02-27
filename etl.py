import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


def get_args():
    """
    Parse command line arguments

    Returns
    -------
    argparse.Namespace
        Object containing parsed command line arguments
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="s3 bucket with input data")
    parser.add_argument("output", help="s3 bucket location to write data")
    args = parser.parse_args()

    return args.input, args.output


def create_spark_session():
    """
    Create a spark session object

    Returns
    -------
    pyspark.sql.SparkSession
        Spark session object providing access to Spark
    """

    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transform song records into two tables: songs_table and artists_table

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Spark session object providing access to Spark
    input_data : str
        Path to S3 bucket with input data
    output_data : str
        Path to S3 bucket where transformed data will be saved
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "year", "duration"
    ).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    (
        songs_table.write.mode("overwrite")
        .partitionBy("year", "artist_id")
        .format("parquet")
        .save(output_data + "songs_table.parquet", header=True)
    )

    # extract columns to create artists table
    artists_table = (
        df.select(
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        )
        .withColumnRenamed("artist_name", "name")
        .withColumnRenamed("artist_location", "location")
        .withColumnRenamed("artist_latitude", "latitude")
        .withColumnRenamed("artist_longitude", "longitude")
        .dropDuplicates()
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").save(
        output_data + "artists_table.parquet", header=True
    )


def process_log_data(spark, input_data, output_data):
    """
    Transform log records into three tables: users_table, time_table, and songplays_table

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Spark session object providing access to Spark
    input_data : str
        Path to S3 bucket with input data
    output_data : str
        Path to S3 bucket where transformed data will be saved
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = (
        df.select("userId", "firstName", "lastName", "gender", "level")
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("firstName", "first_name")
        .withColumnRenamed("lastName", "last_name")
        .dropDuplicates()
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").save(
        output_data + "users_table.parquet", header=True
    )

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df =

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda ts: datetime.fromtimestamp(ts / 1.0e3), returnType=TimestampType()
    )
    df = df.withColumn("start_time", get_datetime(col("ts")))

    # extract columns to create time table
    time_table = (
        df.select("start_time")
        .withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", date_format(col("start_time"), "u"))
        .dropDuplicates()
    )

    # write time table to parquet files partitioned by year and month
    (
        time_table.write.mode("overwrite")
        .partitionBy("year", "month")
        .format("parquet")
        .save(output_data + "time_table.parquet", header=True)
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table
    l = df.alias("l")
    s = song_df.alias("s")
    t = time_table.alias("t")
    songplays_table = (
        l.join(s, l.song == s.title, "inner")
        .join(t, l.start_time == t.start_time, "inner")
        .select(
            col("l.start_time").alias("start_time"),
            col("l.userId").alias("user_id"),
            col("l.level").alias("level"),
            col("s.song_id").alias("song_id"),
            col("s.artist_id").alias("artist_id"),
            col("l.sessionId").alias("session_id"),
            col("l.location").alias("location"),
            col("l.userAgent").alias("user_agent"),
            col("t.year").alias("year"),
            col("t.month").alias("month"),
        )
        .dropDuplicates()
        .withColumn("songplay_id", monotonically_increasing_id())
    )

    # write songplays table to parquet files partitioned by year and month
    (
        songplays_table.write.mode("overwrite")
        .partitionBy("year", "month")
        .format("parquet")
        .save(output_data + "songplays_table.parquet", header=True)
    )


def main():
    """
    Script driver that processes raw data into star-schema parquet tables
    """

    spark = create_spark_session()
    input_data, output_data = get_args()
    input_data = input_data if input_data.endswith('/') else input_data + '/'
    output_data = output_data if output_data.endswith('/') else output_data + '/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

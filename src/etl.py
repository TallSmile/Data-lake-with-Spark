import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Establish spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data from json files and create songs and artist tables in parquet.
    """
    # get filepath to song data file
    song_data = input_data

    # read song data file
    df = spark.read.json("{}song_data/*/*/*/*.json".format(song_data))

    # extract columns to create songs table
    songs_table = df.select(
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'songs'), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude').distinct()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(
        output_data, 'artists'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process log data from json files and create users, time, and songplays tables in parquet.
    """
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json("{}log_data/*/*/*.json".format(log_data))

    # filter by actions for song plays
    df = df.filter(df['page'] == "NextSong")

    # extract columns for users table
    users_table = df.withColumn("last_stamp", max_(col('ts')).over(Window.partitionBy("userId"))) \
        .filter(col('ts') == col('last_stamp')) \
        .select('userId', 'firstName',
                'lastName', 'gender', 'level')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(
        output_data, 'users'), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(
        x/1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), DateType())
    df = df.withColumn("datetime", get_datetime(col('ts')))

    # extract columns to create time table
    time_table = df.withColumn('hour', hour(df.timestamp)) \
        .withColumn('day', dayofmonth(df.timestamp)) \
        .withColumn('week', weekofyear(df.timestamp)) \
        .withColumn('month', month(df.timestamp)) \
        .withColumn('year', year(df.timestamp)) \
        .withColumn('weekday', date_format('timestamp', 'u')) \
        .select('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'time_tbl'), mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))

    artist_df = spark.read.parquet(os.path.join(output_data, 'artists'))

    # extract columns from joined song and log data`sets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title)
                              & (df.length == song_df.duration), 'left_outer') \
                        .join(artist_df, (song_df.artist_id == artist_df.artist_id)
                              & (df.artist == artist_df.artist_name), 'left_outer') \
                        .select(
                            df.timestamp.alias("start_time"),
                            df.userId.alias("user_id"),
                            df.level, song_df.song_id,
                            song_df.artist_id, df.sessionId.alias(
                                "session_id"),
                            df.location, df.userAgent.alias("user_agent")
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.join(time_table, (songplays_table.start_time == time_table.timestamp)) \
                   .select(songplays_table["*"], time_table.year, time_table.month) \
                   .write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'), mode='overwrite')


def main():
    spark = create_spark_session()

    input_data = config['S3']['AWS_S3_INPUT']
    output_data = config['S3']['AWS_S3_OUTPUT']

    process_song_data(spark, input_data, output_data)

    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

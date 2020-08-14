'''
In order to properly setup my local environment on my Windows 10 machine:
- pip install boto3
- I had to install Spark, Java, Hadoop, and SBT.
- Follow the instructions at this link: https://www.youtube.com/watch?v=g7Qpnmi0Q-s for the Java/Hadoop portion.
- Follow the instructions at this link: https://www.youtube.com/watch?v=haMI6uoMKs0 for the SBT portion.
'''

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T
import  pyspark.sql.functions as F
import time
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    # this is supposed to speed up parquet write
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    sc._jsc.hadoopConfiguration().set("spark.speculation","false")

    print(spark)
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function reads the data from S3, processes the song data using Spark, and writes the processed data back to S3.

    Parameters:
    - spark: The spark session
    - input_data: The S3 path location up to, but not including `song_data`
    - output_data: The S3 bucket where the new dimensional tables will be written to
    '''
    # get filepath to song data file
    start = time.time()
    song_data =  input_data + 'song_data/*/*/*/*.json'
    end = time.time()
    print('get song filepath runtime (s):', end-start)
    
    # read song data file
    start = time.time()
    df = spark.read.json(song_data) # may not need this new schema
    end = time.time()
    print('read song data file runtime (s):', end-start)

    # extract columns to create songs table
    start = time.time()
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    end = time.time()
    print('extract columns to create songs table runtime (s):', end-start)

    # write songs table to parquet files partitioned by year and artist
    start = time.time()
    songs_table_parquet = df.select("song_id", "title", "artist_id", "year", "duration", col("artist_name").alias("artist")).distinct()
    songs_table_parquet.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs/")
    end = time.time()
    print('write songs table to parquet files runtime (s):', end-start)

    # extract columns to create artists table
    start = time.time()
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    end = time.time()
    print('extract columns to create artists table runtime (s):', end-start)

    # write artists table to parquet files
    start = time.time()
    artists_table.write.mode('overwrite').parquet(output_data + "artists/")
    end = time.time()
    print('write artists table to parquet files runtime (s):', end-start)


def process_log_data(spark, input_data, output_data):
    '''
    This function reads the data from S3, processes the log data using Spark, and writes the processed data back to S3.
    '''
    # get filepath to log data file
    start = time.time()
    log_data = input_data + 'log_data/*/*/*.json'
    end = time.time()
    print('get log filepath runtime (s):', end-start)

    # read log data file
    start = time.time()
    df_log_data = spark.read.json(log_data)
    end = time.time()
    print('read log data file runtime (s):', end-start)
    
    # filter by actions for song plays
    df_log_data = df_log_data.filter(df_log_data.page=='NextSong')

    # extract columns for users table    
    start = time.time()
    users_table = df_log_data.select("userid", "firstName", "lastName", "gender", "level").distinct()
    end = time.time()
    print('extract columns for users table runtime (s):', end-start)
    
    # write users table to parquet files
    start = time.time()
    users_table.write.mode('overwrite').parquet(output_data + "users/")
    end = time.time()
    print('write users table to parquet files runtime (s):', end-start)

    # create timestamp column from original timestamp column
    '''
    get_timestamp = udf()
    df = 
    '''
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(x/1000.0), T.TimestampType() )
    df_log_data = df_log_data.withColumn("start_time", get_datetime(df_log_data.ts)) 
    
    # extract columns to create time table
    time_table = df_log_data.select("start_time").distinct().withColumn("hour",    F.hour(df_log_data.start_time)) \
                                                        .withColumn("day",     F.dayofmonth(df_log_data.start_time)) \
                                                        .withColumn("week",    F.weekofyear(df_log_data.start_time)) \
                                                        .withColumn("month",   F.month(df_log_data.start_time)) \
                                                        .withColumn("year",    F.year(df_log_data.start_time)) \
                                                        .withColumn("weekday", F.date_format(df_log_data.start_time, "E")) # the 'E' formats this parameter to day-of-the-week
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_df_path = 'output_data/' + 'songs/*/*/*'
    song_df = spark.read.parquet(song_df_path)

    # extract columns from joined song and log datasets to create songplays table 
    ## need to get artist_id from artists_table
    ## read in artist data to get access to artist_id column
    artists_df = spark.read.parquet(output_data + 'artists/*')
    
    ## join songs and log data on title column
    songs_logs_df = df_log_data.join(song_df, df_log_data.song == song_df.title)

    ## join songs_logs_df with artists_df on artist_name
    songs_artists_df = songs_logs_df.join(artists_df, songs_logs_df.artist_name == artists_df.artist_name)
    
    ## songplays table columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = songs_artists_df.select("start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent").distinct()
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()) # create songplay_id and make it primary key
    songplays_table = songplays_table.select("songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent") # rearrange
    songplays_table = songplays_table.withColumn("year", F.year("start_time")).withColumn("month", F.month("start_time")) # add year/month columns

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    '''
    Create Spark session, provide paths to input data and output data, load songs/log data
    and create parquet tables (columnar format) with star schema DB.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-amiri-spark/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()

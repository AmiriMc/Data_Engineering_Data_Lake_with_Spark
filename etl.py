import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
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
    artists_table_parquet = artists_table.write.mode('overwrite').parquet(output_data + "artists/")
    end = time.time()
    print('write artists table to parquet files runtime (s):', end-start)


def process_log_data(spark, input_data, output_data):
   '''
    This function reads the data from S3, processes the log data using Spark, and writes the processed data back to S3.
   '''
    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data/'

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


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

# Project 4: Data Lake with Spark

## Introduction

This project was provided as part of Udacity's Data Engineering Nanodegree program.

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in Parquet file format. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, I applied what I have learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I loaded the data from Udacity's S3 bucket, processed the data into analytis tables using Spark, and then loaded them back into S3. This Spark process can be deployed on an AWS EMR cluster as well.


### To run the Python script, follow the instructions below:
1. Load credentials: Configure the `dl.cfg` file with your AWS IAM access keys, ensure your credentials are correct.
2. Input and output paths: Ensure that your `input_data` path is correct and correctly pointing to the root of `song_data` and `log_data` respectively. Also ensure that the `output_data` path is correct.
3. Read data from AWS S3:  In a terminal*, run the command `python etl.py` to run the etl.py script. This will create a Spark session, load the `song_data` and `log_data`, and create parquet tables (columnar format) for `songs_table`, `artists_table`, `users_table`, `time_table`, and `songplays_table`. The star schema was used for the database.
4. Step 3 above reads the data from S3 (`input_data`), processes it, and loads it back to S3 (`output_data`) in Parquet file format.

&ast; Alternatively, you can run these scripts directly in a Jupyter Notebook using the format: `! python etl.py`.

## Configure dl.cfg file
Fill in missing fields for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
```
[AWS]
AWS_ACCESS_KEY_ID = enter_key_here_do_not_use_quotes_around_this_key
AWS_SECRET_ACCESS_KEY = enter_key_here_do_not_use_quotes_around_this_key
region=us-west-2
output=json
```

## Paths to AWS S3 Bucket for Udacity Data
* Song data: `s3a://udacity-dend/song_data`
* Log data: `s3a://udacity-dend/log_data`

Reading from the Udacity S3 bucket was slow, typically 8 minutes or more. Writing the parquet files to my own S3 bucket was extremely slow and so I ended up writing some of the parquet files to my local machine instead and this was much faster, but still averaged around 8 minutes.

## Project Files
* `etl.py`: Reads data from S3, processes that data using Spark, and writes them back to S3 in Parquet format.
* `dl.cfg`: Contains your AWS credentials
* `etl_sandbox.ipynb`: This was where I prototyped out the `etl.py` functions by breaking each function down into multiple steps as I developed/debugged to get each function working properly. I then transferred the code from this "sandbox" notebook into `etl.py`.

## ETL Pipeline, Schema Justification, and Entity Relationship Diagram
The Star schema using dimensional tables is common in the industry as it allows data analyts, data scientists, and machine learning engineers alike easy and approachble access to the data for analytical insights. I built an ETL pipeline that extracted the data from S3 (Udacity), processed the data using Spark, and then loaded the data back into my S3 bucket as a set of dimensional tables in Parquet file format.

![](https://github.com/AmiriMc/Data_Engineering_Data_Lake_with_Spark/blob/master/data_lake_star_schema.png?raw=t)

## Parqet Formatting and Partitioning
Parquet is a columnar storage file format that is available to any project in the Hadoop ecosystem. It is designed for efficiency and performance compared to row based files such as CSV or TSV files. Data is stored in columns instead of rows and this brings lots of performance optimizations when working with bulk data. Not only is the performance optimized, the storage space required is typically a fraction of that required compared to other formats. [(databricks.com/glossary/what-is-parquet)](https://databricks.com/glossary/what-is-parquet#:~:text=Parquet%20is%20an%20open%20source,like%20CSV%20or%20TSV%20files.)

All of the tables created and written to S3 in this project were written using the parquet file format. 

Tables created:
* songplays (partitioned by year and month)
* artists
* songs (partitioned by year and artist)
* time (partitioned by year and month)
* users (partitioned by year and artist_id)

## Lessons Learned
This project was certainly challenging, but as the concepts start to crystalize in your mind as to what needs to be done at a high level, it becomes easier to successfully chip away at this project. One big take away from this project was that setting up a local machine for Spark and Hadoop was not trivial and at times frustrating and the errors returned are often vague and nebulous. My Windows 10 machine (DELL XPS 9560 laptop) already had Python installed on it, however, for this project, I also had to install Java SDK, Spark, Hadoop, and SBT. If I had to regularly set up machines to run Spark/Hadoop scripts,  I would certainly script this entire process, or better yet, move this to an AWS EMR cluster as this would greatly simplify the setup of the environment and easily justify the cost. 




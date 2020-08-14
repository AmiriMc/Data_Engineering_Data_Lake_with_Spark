# Project 4: Data Lake with Spark

## Introduction

This project was provided as part of Udacity's Data Engineering Nanodegree program.

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in Parquet file format. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

### To run the Python script, follow instructions below:
1. Load credentials: Configure the `dl.cfg` file with your AWS IAM access keys, ensure your credentials are correct.
2. Read data from AWS S3: In a terminal*, run the command `python etl.py` to run the etl.py script. This will create a Spark session, load the `song_data` and `log_data` and create parquet tables (columnar format) for `songs_table`, `artists_table`, `users_table`, `time_table`, and `songplays_table`. The star schema was used for the database.
3. Step 2 above processes reads the data from S3 (`input_data`), processes it and loads it back to S3 (`output_data`) in Parquet file format.

&ast; Alternatively, you can run these scripts directly in a Jupyter Notebook using the format: `! python my_script.py`.

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
* `etl_sandbox.ipynb`: This was where I prototyped out the `etl.py` functions by breaking each function down into multiple steps as I developed/debugged to get the each function working properly. I then transferred the code from this "sandbox" notebook into `etl.py`.

## ETL Pipeline and Schema Justification
The Star schema using dimensional tables is common in the industry as it allows data analyts, data scientists, and machine learning engineers alike easy and approachble access to the data for analytical insights.

## Parqet Formatting and Partitioning

## Lessons Learned
This project was certainly challenging, but as the concepts start to crystalize in your mind as to what needs to be done at a high level, it was easier to chip away at this project. My big take away from this project was that setting up a local machine for Spark and Hadoop was not trivial and at times frustrating and the errors returned are often vague and nebulous. My Windows 10 machine (DELL XPS 9650 laptop) already had Python installed on it, however, I also had to install Java SDK, Spark, Hadoop, and SBT. If I had to regularly set up machines to run Spark/Hadoop scripts,  I would certainly script this process, or better yet, move this to an AWS EMR cluster as this would greatly simplify the setup of the environment. 




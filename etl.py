import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Creates an Apache Spark Session'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' Loads song data from S3, processes it in Spark to create songs and artist table and
    then writes the data back to S3 as parquet files

    INPUTS:
    spark: Spark session to be used

    input_data: Location of songs metadata in JSON format

    output_data: Location of S3 bucket where the output parquet files for songs and artists are to be written
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/A/A/A/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs.parquet'),'overwrite')
    print('song table written')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artist.parquet'),'overwrite')
    print('artists table written')


def process_log_data(spark, input_data, output_data):
    ''' Loads log data from S3, processes it in Spark to create users, timestamp and songplays tables and
    then writes the data back to S3 as parquet files. The songplays table is created by using both the song and log data

    INPUTS:
    spark: Spark session to be used

    input_data: Location of log_data in JSON format containing events data

    output_data: Location of S3 bucket where the output parquet files for users, timestamp and songplays are to be written
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data,'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = df.filter(df['page']=='NextSong').select('ts', 'userId', 'level','song','artist','sessionId', 'location', 'userAgent')

    # extract columns for users table
    users_table = df['userId','firstName','lastName','gender','level']
    users_table = users_table.dropDuplicates(['userId'])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users.parquet'),'overwrite')
    print('users table written')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df['ts']))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    log_df = log_df.withColumn('datetime',get_datetime(log_df['ts']))

    # extract columns to create time table
    time_table = log_df.select(col('datetime').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year')
                          )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'timetable.parquet'),'overwrite')
    print('time table written')

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,'song_data/A/A/A/*.json'))

    # extract columns from joined song and log datasets to create songplays table
    song_log_df = log_df.join(song_df, log_df['song']==song_df['title'])

    songplays_table = song_log_df.select(col('ts').alias('start_time'),
                                        col('userId').alias('userId'),
                                        col('level').alias('level'),
                                        col('song_id').alias('song_id'),
                                        col('artist_id').alias('artist_id'),
                                        col('sessionId').alias('sessionId'),
                                        col('location').alias('location'),
                                        col('userAgent').alias('userAgent'),
                                        year('datetime').alias('year'),
                                        month('datetime').alias('month')
                                       )
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays.parquet'),'overwrite')
    print('songplays table written')


def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3a://datalake-wk/"


    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

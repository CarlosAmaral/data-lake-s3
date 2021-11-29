import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Stablishes a spark session
    
    """
    try:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(e, "error stablishing a spark session")


def process_song_data(spark, input_data, output_data):
    
    """ETL pipeline of extracting song data from json files from S3, transforforming it with Dataframes and Spark,
    and parquet (load) it back to an ERM cluster
    
    """
    
    # full path for the songs dataset
    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    
    song_df = spark.read.json(song_data)
    
    song_df = song_df.drop_duplicates()
    
    # extract fields from songs_data's dataframe
    extract_songs_table = song_df.select(
        col('song_id'), 
        col('title'), 
        col('artist_id'),
        col('year'), 
        col('duration')
    )
    
    # write fields from the dataframe field selection above to the ERM cluster
    write_songs_table = extract_songs_table.write.partitionBy(
        'year', 'artist_id'
        ).parquet(
        os.path.join(output_data, 'songs.parquet'), 'overwrite')
    
    # extract fields from songs_data's dataframe
    extract_artists_table = song_df.select(
        col('artist_id'), 
        col('artist_name').alias('name'), 
        col('artist_location').alias('location'),
        col('artist_latitude').alias('latitude'), 
        col('artist_longitude').alias('longitude')
    )

    # write fields from the dataframe field selection above to the ERM cluster
    write_artists_table = extract_artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    """ETL pipeline of extracting log data from json files from S3, transforforming it with Dataframes and Spark,
    and parquet (load) it back to an ERM cluster
    
    """
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')
    
    log_df = spark.read.json(log_data)
    
    log_df = log_df.drop_duplicates()
    
    # extract fields from log_data's dataframe for songplays table
    extract_songplays_table = log_df.select(
        col('ts'), 
        col('userId'), 
        col('level'),
        col('sessionId'), 
        col('location')
    )
    
    # extract fields from log_data's dataframe for users table
    extract_users_table = log_df.select(
        col('userId'), 
        col('firstName'), 
        col('lastName'),
        col('gender'), 
        col('level')
    )
    
    # write fields from the dataframe field selection above to the ERM cluster
    write_users_table = extract_users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    
    # timestamp function to ts column to timestamp
    get_timestamp_fn = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    
    # invoke get_timestamp_fn with the ts field as argument and write a new column to the dataframe
    log_df = log_df.withColumn('timestamp', get_timestamp_fn(log_df.ts))
    
    # extract fields for the time table 
    extract_time_table = log_df.select(
        col('timestamp').alias('start'), 
        year('timestamp').alias('year'), 
        month('timestamp').alias('month'),
        dayofmonth('timestamp').alias('day_of_month'), 
        hour('timestamp').alias('hour')
    )
    
    # write time table to parquet files partitioned by year and month to the ERM cluster
    write_time_table = extract_time_table.write.partitionBy(
        'year', 'month'
    ).parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    
    # full path for the songs dataset
    song_data = os.path.join(input_data, 'song-data/A/A/A/*.json')
    
    # song df for the songs dataset
    song_df = spark.read.json(song_data)
    
    song_df = song_df.drop_duplicates()
    
    # join log and song data dataframes
    log_and_song_joined_df = log_df.join(song_df, song_df.title == log_df.song)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_and_song_joined_df.select(
        col('ts').alias('ts'),
        col('userId'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        col('year'),
        month('timestamp').alias('month')
    )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table_write = songplays_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = os.environ['CLUSTER']=config['S3']['CLUSTER']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

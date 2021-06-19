from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import IntegerType, TimestampType
import os
import configparser


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("default", 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("default", 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This procedure executes COPY into the staging tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    configure = SparkConf().setAppName("app name").setMaster("local")
    sc = SparkContext(conf = configure)
    spark = SparkSession.builder\
                    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
                    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
                    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .appName("app name")\
                    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
                    .getOrCreate()    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This procedure executes COPY into the staging tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    df = spark.read.format("json").load(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data_table")
    songs_table = spark.sql("""
                        SELECT s.song_id, 
                        s.duration,
                        s.artist_id,
                        s.year,
                        s.title
                        FROM song_data_table s
                        WHERE s.song_id IS NOT NULL
                    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs/"), mode='overwrite', partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = spark.sql("""
                        SELECT s.artist_id, 
                        s.artist_latitude,
                        s.artist_longitude,
                        s.artist_location,
                        s.artist_name,
                        s.year,
                        s.num_songs
                        FROM song_data_table s
                        WHERE s.artist_id IS NOT NULL
                    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists/"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This procedure executes COPY into the staging tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                    SELECT DISTINCT user.userId as user_id, 
                    user.firstName as first_name,
                    user.lastName as last_name,
                    user.gender as gender,
                    user.level as level
                    FROM log_data_table user
                    WHERE user.userId IS NOT NULL
                """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    #df = df.withColumn('start_time', get_timestamp('ts'))
    df = df.withColumn('start_time', get_timestamp(df['ts']/1000))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    #df = df.withColumn('datetime', from_unixtime('start_time'))
    df = df.withColumn('datetime', to_date('start_time'))

    # extract columns to create time table
    time_table = df.withColumn("hour",hour("datetime"))\
                .withColumn("day",dayofmonth("datetime"))\
                .withColumn("week",weekofyear("datetime"))\
                .withColumn("month",month("datetime"))\
                .withColumn("year",year("datetime"))\
                .withColumn("weekday",dayofweek("datetime"))\
                .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    song_df = spark.read.format("json").load(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(
        song_df, 
        (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 
        'left_outer')\
            .select(
                df.start_time,
                col("userId").alias('user_id'),
                df.level,
                song_df.song_id,
                song_df.artist_id,
                col("sessionId").alias("session_id"),
                df.location,
                col("useragent").alias("user_agent"),
                year('start_time').alias('year'),
                month('start_time').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"), mode='overwrite', partitionBy=["year","month"])


def main():
    """
    This procedure executes COPY into the staging tables.

    INPUTS:
    * cur the cursor variable
    * conn the database connection
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = "data/"
    output_data = "data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

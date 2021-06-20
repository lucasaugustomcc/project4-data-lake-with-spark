# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Project Data Lake with Spark and S3

# %%
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, desc
import os
import configparser


# %%
config = configparser.ConfigParser()

config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['default']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['default']['AWS_SECRET_ACCESS_KEY']

# %% [markdown]
# # Create spark session with hadoop-aws package

# %%
configure = SparkConf().setAppName("app name").setMaster("local")
sc = SparkContext(conf = configure)
spark = SparkSession.builder\
                .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
                .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.sql.crossJoin.enabled","true") \
                .appName("app name")\
                .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
                .getOrCreate()                      

# %% [markdown]
# # Analysis

# %%
output_data = "s3a://data-engineering-nd-2021/output/"
song_df = spark.read        .format("parquet")        .option("basePath", os.path.join(output_data, "songs/"))        .load(os.path.join(output_data, "songs/*/*/"))


# %%
song_df.printSchema()
song_df.show(3)


# %%
artists_df = spark.read        .format("parquet")        .option("basePath", os.path.join(output_data, "artists/"))        .load(os.path.join(output_data, "artists/*"))


# %%
artists_df.printSchema()
artists_df.show(3)


# %%
songplays_df = spark.read        .format("parquet")        .option("basePath", os.path.join(output_data, "songplays/"))        .load(os.path.join(output_data, "songplays/*"))
songplays_df.printSchema()
songplays_df.show(3)


# %%
song_df = song_df.alias("s")
artists_df = artists_df.alias("a")
songplays_df = songplays_df.alias("sp")
sparkify_table = songplays_df.join(
        song_df, 
        songplays_df.song_id == song_df.song_id, 
        'left_outer')\
        .join(
            artists_df,
            artists_df.artist_id == artists_df.artist_id, 'left_outer')
        #.select(col('s.year').alias('s_year'))


# %%
sparkify_table.printSchema()
sparkify_table.show(1)


# %%
songs_in_hour = sparkify_table.filter(col('sp.year') == 2018).select(col('a.artist_name'))
songs_in_hour.show()


# %%
top_users = sparkify_table.groupby(col('sp.user_id')).count().orderBy(desc('count'))
top_users.show()


# %%
users_df = spark.read        .format("parquet")        .option("basePath", os.path.join(output_data, "users/"))        .load(os.path.join(output_data, "users/*"))
users_df.printSchema()
users_df.show(3)


# %%
top_user_data = top_users.join(
    users_df,
    users_df.user_id == top_users.user_id,
    'inner'
)


# %%
top_user_data.select('sp.user_id','first_name','last_name',col('count').alias('song_plays')).dropDuplicates().orderBy(desc('count')).show()



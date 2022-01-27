# importing all the needed libraries
import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# getting all the configuration
config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

#--------------------------------
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
#--------------------------------

# data source in and out in a bucket
S3_Data_Bucket = config['S3']['S3_Data_Bucket']
Destination_S3_Bucket = config['S3']['Destination_S3_Bucket']

# creating a spark-session
def create_spark_session():
    """
    Creates a new or uses the existing spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

#===========================================================================================
def process_song_data(spark, input_data, output_data):
    """"
        Description:
        This function loads song_data table from S3 and then
        extracts specific data-columns for song_table and artist_table.
        Does some invariable transformation and
        Load the tables back to S3 as parquet file.
        
        Parameters:
            spark:       Spark Session
            input_data:  path/location of "song_data.json" file to load back here
            output_data: location in S3 bucket where transformed file will be saved as a parquet file. 
        Return: None
    """  
    # get filepath to song data file
    print("Getting song_data path from json file")
    
    #song_data = input_data + 'song_data/*/*/*/*.json' 
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
       
    # creating a confirming schema for song_data table with specific columns
    song_data_schema = StructType([
        StructField("num_songs", IntegerType(),True),
        StructField("title", StringType(),True),
        StructField("artist_id", StringType(),True),
        StructField("year", IntegerType(),True),
        StructField("duration", DoubleType(),True),
        StructField("artist_name", StringType(),True),
        StructField("artist_location", StringType(),True),
        StructField("artist_latitude", DoubleType(),True),
        StructField("artist_longitude", StringType(),True),       
    ])
    
    # read song data file with schema asserted
    song_df = spark.read.json(song_data, schema = song_data_schema)
        
    # extract columns from 'song_data_table' view
    songs_table = song_df.select("title", "artist_id", "year", "duration").dropDuplicates() \
                    .withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing Songs_table to S3_bucket after processing")
    songs_table.write.mode("overwrite").pratitionBy("year", "artist_id").format("parquet").save(output_data + "songs")

#--------Extracting artist_table from 'song_df' table
    artists_table = song_df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()
    # writing the artist_table back to S3 bucket as a parquet-file
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')
    
       
#====================================================================================================
def process_log_data(spark, input_data, output_data):
    """
    Description:
    This functions imports/loads "log_data.json" file from S3 bucket 
    Creating two tables user and time tables
    and after some transformations send it back to S3 bucket as a parquet file.
    
    Parameters:
              spark:      Spark Session
              input_data:  path/location of "log_data.json" files to load back here
              output_data: location in S3 bucket where transformed file will be saved as a parquet file format 
    Return: None
    """
    # get filepath to log data file
    log_data_path = os.path.join(input_data, 'log_data/*/*/*.json')
    
    # read log_data file
    log_df = spark.read.json(log_data_path)
      
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong') 
    #log_df = log_df.where(df.page = 'NextSong') 
    
    # -----------USERS TABLE-------------------------------------
    # selecting columns from log_df file to create 'users_table'
    # extract columns for users table                                  
    users_table = log_df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
        
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')
    #users_table.write.parquet(output_data + "users_parq_file.parquet", mode="overwrite")
                     
                    
    #==========================TIME TABLE CREATION ==========================================   
    get_timestmap = udf(lambda x: datetime.utcfromttimestamp(int(x)/1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestampt("ts"))\
    
    # Extract columns to create time table
    log_df = log_df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time"))
                    
    time_table = log_df.select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()
                  

    # Write time table to parquet files partitioned by year and month
    print("Writing Time_table back to S3 bucket after processing")  
    time_table.write.parquet(os.path.join(output_data, "time"), mode='overwrite', \
                             partitionBy=["year","month"])
    
    # time_table.write.mode('overwrite').partitionBy('year', 'month')\
    #         .parquet(path=output_data + 'time') 

#===============================================================================================
    #------------------ Reading tables back from "Parquet" files -----------------------------------
    # reading back from 'songs.parquet' file    
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    
    # 1.join ( log_df & songs ) table
    # Extract columns from joined song and log_df datasets to create songplays table               
    song_log_table = log_df.join(song_df, log_df.song == song_df.title, how='inner')              
                    
    # reading back from 'atrists.parquet' file
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    
    # 2. join ( songs & artists ) table
    # joining song_df and artist table
    artists_songs_logs = song_log_table.join(artists_df, song_log_table.artist == artists_df.name, how='inner')
    
    # 3. join ( artists_songs_logs & time ) table
    # joinng with artists_songs_logs & time_table
    songplays = artists_songs_logs.join(time_table, artists_songs_logs.ts == time_table.ts, how='inner')

    # selecting columns needed for the songplays table
    songplays_table = songplays.select(monotonically_increasing_id().alias("songplay_id"), \
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).drop_duplicates()

    
    #-------------------------------back to S3 bucket as a parquet file
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays"), \
                                  mode="overwrite", partitionBy=["year","month"])

    #========================Main Function========================================   
def main():
    """
    Reading from input data bucket
    Sending to output bucket
    Calling two functions
    """
    # spark session + input_data + output_data
    spark = create_spark_session()
    input_data = S3_Data_Bucket
    output_data = Destination_S3_Bucket
    
    # calling both functions
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
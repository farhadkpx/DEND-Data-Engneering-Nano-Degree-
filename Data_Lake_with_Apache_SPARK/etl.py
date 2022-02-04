# importing all the needed libraries
import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf, col, year, month, dayofweek, hour, weekofyear, dayofmonth
from pyspark.sql.functions import monotonically_increasing_id, from_unixtime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# getting all the configuration
config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

#----------------------------------------------------------------------------------
#os.environ[AWS_ACCESS_KEY_ID]=config[AWS][AWS_ACCESS_KEY_ID]
#os.environ[AWS_SECRET_ACCESS_KEY]=config[AWS][AWS_SECRET_ACCESS_KEY]

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

#----------------------------------------------------------------------------------
# data source in and out in a bucket (Not used)
# S3_Data_Bucket = config['S3']['S3_Data_Bucket']
# Destination_S3_Bucket = config['S3']['Destination_S3_Bucket']

# creating a spark-session
#----------------------------------------------------------------------------------------
def create_spark_session():
    """
    creating a spark session.
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
    # get filepath to song data file with reduced song_data table size 
    # song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')    
    
    # The reduced size 'song_data.json' file
    song_data = os.path.join(input_data, 'song_data/A/A/*/*.json')
       
    # creating a confirming schema for song_data table with specific columns and data type
    song_data_schema = StructType([
        StructField("song_id", StringType(),True),
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
    
    # quick view of my song_data table file
    print('Viewing original song_data table schema: ')
    song_df.printSchema()
    
    # creating song_table with selection
    songs_table = song_df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()    
    
    # creating a temp view of song data for songplays table to join
    songs_table.createOrReplaceTempView("song_data_view")
    
    print('Viewing selected songs_table columns Schema:')
    songs_table.printSchema()
    
    # write songs_table to parquet files partitioned by year and artist
    print("Writing songs_table back to > Results < folder in Workspace after processing.")
    print('\n')
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").format("parquet").save(output_data + "songs/")

    # These code works for sending 'songs_table' back to my S3 bucket'
    # print("Sending files back to my S3 bucket")
    # songs_table \
    #    .write \
    #    .partitionBy('year', 'artist_id') \
    #    .parquet(('s3a://sparkifybucket/songs/'), 'overwrite')
    # print("Sending song_data file back to S3 is completed.")
    
    # -- Extracting artist_table columns from 'song_df' table
    artists_table = song_df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()
   
    print('Viewing selected artists_table columns Schema:')   
    artists_table.printSchema()
    
    # writing the artist_table back to S3 bucket as a parquet-file
    print("Writing 'artists_table' back to > Results < folder in Workspace after processing.")
    artists_table.write.mode("overwrite").format("parquet").save(output_data + "artists/")
    
    # sending artists_table back to my S3 bucket
    # artists_table.write.parquet(('s3a://sparkifybucket/artists/'), 'overwrite')
       
 #================================== LOG DATA ==================================================================
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
    # get filepath for log_data file
    log_data_path = os.path.join(input_data, 'log_data/*/*/*.json')
    
    # read log_data file through the path
    log_df = spark.read.json(log_data_path)
      
    # filter only by actions pages for song plays
    log_df = log_df.filter(log_df.page == 'NextSong') 
    # log_df = log_df.where(df.page = 'NextSong') 
    
    print("Viewing original log_df table Schema:")
    log_df.printSchema()
    
    # -----------USERS TABLE--------------------------------------------------------------------------
    # selecting columns from log_df file to create 'users_table'                             
    users_table = log_df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates()
    print('\n')
    print('Viewing selected users_table columns Schema:')   
    users_table.printSchema()
    
    # write users table to parquet files
    print("Writing 'users_table' back to > Results < folder in Workspace after selecting.")
    users_table.write.mode("overwrite").format("parquet").save(output_data + "users/")
    
    # sending users_table back to my S3 bucket
    # users_table.write.parquet(('s3a://sparkifybucket/users/'), 'overwrite')
                        
                    
    #==========================TIME TABLE CREATION ==========================================
    print("Converting 'ts' column from 'log_df' into segmented 'timestamp' form:\n")
    
    # create timestamp column from original 'ts' column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))

    # create 'start_time' column from 'timestamp' column with datetime format
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))
    
    # transforming 'start_time' into five new separate column added to 'log_df' table
    log_df = log_df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time"))
    
    print("Viewing log_df table after time segmented insertion:")
    log_df.printSchema()
    
    # selecting columns from the 'log_df' table to create a time table               
    time_table = log_df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"]).drop_duplicates()
       
    print('Viewing selected time_table columns schema:')   
    time_table.printSchema()
                  
    print('\n')
    # Write time table to parquet files partitioned by year and month
    print("Writing Time_table back to > Results < folder in workspace after processing.")  
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', \
                            partitionBy=["year","month"])
    
    # sending time_table back to my S3-sparkifybucket bucket in Amazon
    # time_table.write.partitionBy('year', 'month')\
    #                        .parquet(('s3a://sparkifybucket/time/'), 'overwrite'
   
 #===============================================================================================
    # Read in song data to use for songplays table
    songs_df2 = spark.sql("SELECT * FROM song_data_view")
        
    # creating 'songplays_table' by joining with'song_df' table
    songplays_table = log_df.join(songs_df2, log_df.song == songs_df2.title, how='inner') \
                        .select(monotonically_increasing_id().alias("songplay_id"), \
                                col("start_time"), \
                                col("userId").alias("user_id"), \
                                "level","song_id","artist_id", \
                                col("sessionId").alias("session_id"), \
                                "location", \
                                col("userAgent").alias("user_agent"),\
                                log_df['year'].alias('year'),\
                                log_df['month'].alias('month'))
                            

    print('Final "songplay_table" selected column Schema:')
    songplays_table.printSchema()    
       
    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays_table back to >> Results folder in Workspace.')   
    songplays_table.write.mode("overwrite").partitionBy("year", "month").format("parquet").save(output_data + "songplays/")
    
    # sending data back to my Amazon S3 bucket
    # songplays_table.write.partitionBy('year', 'month')\
    #                       .parquet(('s3a://sparkifybucket/songplays/'), 'overwrite')
    
    print('Final songplays table-dimension with row-colum: \n')
    print(songplays_table.toPandas().shape)
            
    #======================== MAIN FUNCTION ========================================   
def main():
    """
    Reading from input data bucket
    Sending to output bucket
    Calling two functions
    """
    # spark session + input_data + output_data
    spark = create_spark_session()
    
    #input_data = S3_Data_Bucket
    #output_data = Destination_S3_Bucket
    
    input_data = 's3a://udacity-dend/'
    output_data = './Results/'
    
    # only for sending data back to Amazon S3 bucket
    # input_data = 's3a://udacity-dend/'
    # output_data ='https://s3.console.aws.amazon.com/s3/buckets/sparkifybucket?region=us-west-2&tab=objects'
    
    # calling both functions
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

# implementing main() function
if __name__ == "__main__":
    main()

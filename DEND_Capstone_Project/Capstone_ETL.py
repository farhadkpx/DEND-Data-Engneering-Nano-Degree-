# Getting all the needed packages
import configparser
import os
import logging

import pandas as pd
import pyspark
from pyspark.sql.functions import asc, desc

import datetime as dt
from datetime import datetime
from pyspark.sql.functions import udf

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, DoubleType

from pyspark.sql.functions import udf, split, col, lit, year, month, upper, lower, to_date
from pyspark.sql.functions import datediff, isnan, when, count, desc, avg, round,sum

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)


#====================================================================================================
#.............................KentHsu............CLOUD ISSUES

# AWS configuration
#config = configparser.ConfigParser()
#config.read('capstone.cfg', encoding='utf-8-sig')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
#DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

#=====================================Rlyer==============================================
#get config data by reading from dl.cfg
#config = configparser.ConfigParser()
#config.read('/home/workspace/dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
#os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

#os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"

#os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
#os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

#create the spark session
#def create_spark_session():
#    """Return a SparkSession object."""
    
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
#        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
#        .enableHiveSupport() \
#        .getOrCreate()
#    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
#    return spark


#=============================CREATING SPARK SESSION==================================================
# create the spark session
#def create_spark_session():
#    spark = SparkSession.builder\
#        .config("spark.jars.packages",\
#                "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
#        .enableHiveSupport().getOrCreate()
#    return spark
#---------------------------------------------------------------------
logging.info("Start creating Spark session: ")
def create_spark_session():
    spark = SparkSession \
               .builder \
               .appName("Capstone - Non_Cloud_Application") \
               .getOrCreate()
    return spark

#-------------------------------------------------------------------------
#def create_spark_session():
#    """Return a SparkSession object."""
    
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
#        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
#        .enableHiveSupport() \
#        .getOrCreate()
#    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2").........???
#    return spark

#========================================================================================================
#--------------------------------------Need work-----------------------------------------------
#def SAS_to_date(date):
#    if date is not None:
#        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
#SAS_to_date_udf = udf(SAS_to_date, DateType())



#------------------------------------------------REVISE IT-------------------------------------------------
# Immigration data processing function
def Immigration_data_processing(spark, output_data):
    """
       Description:
       Processing immigration data to create a fact_immigration, 
       dim_Individual_Immigrants_Records and dim_Dated_Arrival_Departure tables.
       Arguments:
            spark: A SparkSession object.
            input_data: Source of immigration data.
            output_data: Target location to send processed fact & dimension tables.
       Returns:  None
    """

    logging.info("Redefining date column, renaming columns and data type change: \n")
    #---------------------------------------------------------------------------------------
    # read immigration data file
    #immi_data = os.path.join(input_data + 'immigration/18-83510-I94-Data-2016/*.sas7bdat')
    #df = spark.read.format('com.github.saurfang.sas.spark').load(immi_data)
    #--------------------------------------1-----------------------------------------------
    # read immigration data
    # df=spark.read.parquet("sas_data")
    #-------------------------------------
    immigration_file_path = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/immigration_data_sample.csv'
    df_immigration = spark.read.format('csv').options(header=True, delimiter=',').load(immigration_file_path)
    df_immigration.show(3)
    
    print("Counting number of NULL values by each column with Immigration table: ")
    df_immigration.select([count(when(col(c).isNull(), c))\
                         .alias(c) for c in df_immigration.columns]).show(n=2, truncate=False, vertical=False)
    
    # UDF > converts SAS date fields to date type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    
    # applying the user defined function to convert SAS date and rename the given date column
    df_immigration = df_immigration.withColumn("arrival_Date", get_date(df_immigration.arrdate))
    df_immigration = df_immigration.withColumn("departure_Date", get_date(df_immigration.depdate))
    
    # converting date from SAS format to pySpark date format
    df_immigration = df_immigration.withColumn("Arrival_Date", to_date(col("arrival_Date").cast('date')))
    df_immigration = df_immigration.withColumn("Departure_Date", to_date(col("departure_Date").cast('date')))
    
    # creating a view for SQL query
    df_immigration.createOrReplaceTempView("Immigration_Data")
    
    # Read, rename & redefine columns with Fact Table
    Immigration_fact = spark.sql('''
                    SELECT int(cicid)     Immigration_Id,\
                        int(i94yr)     Immigration_Year,\
                        int(i94mon)    Immigration_Month,\
                        int(i94cit)    Citizenship_Country,\
                        int(i94res)    Residency_Country,\
                        i94port        Port_Code,\
                        Arrival_Date             ,\
                        int(i94mode)   Travel_Mode,\
                        i94addr        Current_State,\
                        Departure_Date              ,\
                        int(i94bir)    Immigrants_Age,\
                        int(i94visa)   Visa_Code,\
                        matflag        Match_Flag,\
                        int(biryear)   Birth_Year,\
                        gender         Gender,\
                        airline        Airline_Code,\
                        bigint(admnum) Admission_Num,\
                        fltno          Flight_Num,\
                        visatype       Visa_Type                        
                    FROM Immigration_Data
                    WHERE (i94yr) >= 2016                     
                ''')
    
    logging.info("Displaying Immigration_fact table data after transformation: ")
    Immigration_fact.show(3)
    
    #-----------------------------------------------------------------------------------------------------------
    # write to partitioned parquet files 
    # Immigration_fact.mode('overwrite').parquet(os.path.join(output_data,'Capstone/Immigration_fact.parquet'))
    
    # Immigration_fact.write.mode("overwrite")\
                  # .parquet(path=output_data + 'Immigration_fact/')
        
    #----------------------------Individual immigration Record----------2-------------------------------------    
    
    logging.info("Devising individual immigrants Dimension table: \n")
    # selecting columns relevant to individual immigrants records
    Individual_Immigrants_Records = Immigration_fact.selectExpr('Admission_Num as Entry_Num','Immigration_Id','Arrival_Date', 'Citizenship_Country',\
                          'Immigrants_Age', 'Gender','Departure_Date','Visa_Type', 'Match_Flag').distinct()
    
    logging.info("Displaying individual immigrants DIMENSION TABLE: ")
    Individual_Immigrants_Records.show(3)
    Individual_Immigrants_Records.printSchema()
    #-----------------------------------------------------------------------------------------
    # sending writing the file back to cloud
    # dim_Individual_Immigrants_Records.mode('overwrite').\
    # parquet(os.path.join(output_data,'Capstone/dim_Individual_Immigrants_Records.parquet'))
     
    #---------------------Dated transformation of arrival-departure columns----------3---------------------------
    
    logging.info("Creating a Date intensive immigration Dimension table: \n")
    
    # creating temp view for a detail dated information        
    Immigration_fact.createOrReplaceTempView("Immigration_Time_Line")
    
    # delving into detail dated arrival and departure columns
    Dated_Arrival_Departure = spark.sql('''
                     select Arrival_Date AS Entry_Date, \
                            Admission_Num,\
                            Citizenship_Country, \
                            date_format(date(Arrival_Date),'yyyy') Arrival_Year,\
                            date_format(date(Arrival_Date),'MMMM') Arrival_Month,\
                            date_format(date(Arrival_Date),'dd') Arrival_Day,\
                            departure_date AS Departure_Date, \
                            date_format(date(Departure_Date),'yyyy') Depart_Year,\
                            date_format(date(Departure_Date),'MMMM') Depart_Month,\
                            date_format(date(Departure_Date),'dd') Depart_Day, \
                            Visa_Type, Port_Code
                     from Immigration_Time_Line
                     WHERE year(Arrival_Date) >= 2016               
                  ''')
    
    logging.info("Displaying immigrants date detail DIMENSION TABLE: ")
    Dated_Arrival_Departure.show(3)
    Dated_Arrival_Departure.printSchema()
    #------------------------------------------------------------------
    # sending the file back to cloud
    # dim_Dated_Arrival_Departure.mode('overwrite').\
    # parquet(os.path.join(output_data,'Capstone/dim_Dated_Arrival_Departure.parquet'))
       
    #---------------------Travel modes of Immigrants-----------4---------------------------------------------
    
    logging.info("Redefining Travel mode column from immigration table: \n")
    Mode_of_Travel = Immigration_fact.select(['Immigration_Id','Citizenship_Country', 'Current_State', 'Visa_Type', 'Travel_Mode']).distinct()
    
    # transforming mode of travel to an understandable form
    Mode_of_Travel = Mode_of_Travel.withColumn("Means_of_Travel", \
                   when((Mode_of_Travel.Travel_Mode == 1), lit("Air")) \
                     .when((Mode_of_Travel.Travel_Mode == 2), lit("Sea")) \
                        .when((Mode_of_Travel.Travel_Mode == 3), lit("Land")) \
                          .when((Mode_of_Travel.Travel_Mode == 9), lit("Not Reported")))
    
    logging.info("Displaying immigrants Mode of Travel data: ")
    Mode_of_Travel.show(3)
    
    #--------------------------------------------------------------------------------------
    # sending 'Mode_of_Travel' ( Not a dimension table ) back to cloud
    # Mode_of_Travel.mode('overwrite').parquet(os.path.join(output_data,'Capstone/Mode_of_Travel.parquet'))
    
    #----------------------Visa Type category------------------5-----------------------------------------
    
    logging.info("Visa purpose categorization of immigrants: \n")
    Visa_Purpose_Type = Immigration_fact.select(['Immigration_Id','Citizenship_Country', 'Current_State',                                                'Residency_Country','Visa_Code']).distinct()
    
    # transforming 'visa category type' to understandable form
    Visa_Purpose_Type = Visa_Purpose_Type.withColumn("Visa_Purpose", \
                   when((Visa_Purpose_Type.Visa_Code == 1), lit("Business")) \
                     .when((Visa_Purpose_Type.Visa_Code == 2), lit("Pleasure")) \
                        .when((Visa_Purpose_Type.Visa_Code == 3), lit("Student")))  
    
    # joing 'Mode_of_Travel' and 'Visa_Purpose_Type' by removing duplicated colummns
    Mode_Visa_Purpose = Mode_of_Travel.join(Visa_Purpose_Type, ["Immigration_Id", 'Citizenship_Country', \
                             'Current_State']).drop(Mode_of_Travel.Travel_Mode) 
    
    logging.info("Displaying 'Mode_Visa_Purpose' table data: ")
    Mode_Visa_Purpose.show(3)
    
    #--------------------------------------------------------------------------------------
    # sending 'Mode Visa Category' table file back to cloud ( Not a dimension table )
    # Mode_Visa_Purpose.mode('overwrite').\
    # parquet(os.path.join(output_data,'Capstone/Mode_Visa_Purpose.parquet'))
    
    #----------------Joining with Fact Immigration table for a final Table----------6------------------------ 
    # Joining by removing duplicated columns
    Fact_Immigration_Table = Immigration_fact.join(Mode_Visa_Purpose, ["Immigration_Id", 'Citizenship_Country', 'Residency_Country', 'Current_State', 'Visa_Type']).drop('Travel_Mode', 'Visa_Code')
    
    logging.info("Displaying Immigration Fact Table with inclusive data: \n")
    Fact_Immigration_Table.show(3)
    
    Fact_Immigration_Table.printSchema()
    #-------------------------------------------------------------------------------
    # sending 'Fact_Immigration_Inclusive' table file back to cloud ( The Fact Table )
    #  Fact_Immigration_Inclusive.mode('overwrite').\
    #  parquet(os.path.join(output_data,'Capstone/Fact_Immigration_Inclusive.parquet'))
     
#=============================================DONE============================================================
def Airports_data_processing(spark, output_data):
    """
       Description:
       Processing the Airport Codes data and after some data transformations,
       I'll write it as a parquet file back to workspace cloud folder.
    
       Arguments:
       Spark: SparkSession object as ceated.
       Input_data: Source of the Airport table.
       Output_data: path to write/send out the parquet files back to cloud.    
       Returns: None    
    """
    
    #read the airports codes csv file
    fname_path = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/airport-codes_csv.csv'
    Airport_df = spark.read.options(delimiter=",").csv(fname_path, header=True)
    
    #Airport_df = spark.read.options(delimiter=",").csv("data/airport-codes_csv.csv",header=True)
    
    # splitting existing 'coordinates' column into two longitude and lattitude columns
    Airport_df = Airport_df.withColumn('latitude', split(Airport_df['coordinates'], ',').getItem(0)) \
       .withColumn('longitude', split(Airport_df['coordinates'], ',').getItem(1)) 
      
    # A quick view of airport data
    logging.info("A quick view of Airport data: \n")
    Airport_df.show(5)
    
    print("Number of NULL values by each column with Airport_Codes_df Table: \n")
    Airport_df.select([count(when(col(c).isNull(), c))\
                         .alias(c) for c in Airport_df.columns]).show(n=2, truncate=False, vertical=True)
        
    # creating a temporary view of Airports_Table for SQL based transformations
    Airport_df.createOrReplaceTempView("Airport_Table")
    
    # SQL based transformations with conditions
    Airport_Details = spark.sql('''
                SELECT distinct  \
                         ident        AS Airport_Code, \
                         type         AS Airport_Type, \
                         name         AS Airport_Name, \
                         double(elevation_ft) AS Elevation_Ft, \
                         continent    AS Continent, \
                         iso_country  AS Country, \
                         iso_region   AS Region, \
                         municipality AS Municipality, \
                         gps_code     AS Gps_Code, \
                         iata_code    AS Iata_Code,\
                         local_code   AS Local_Code, \
                         latitude     AS Latitude,\
                         longitude    AS Longitude \
                         FROM Airport_Table \
                WHERE (iata_code is NOT NULL) AND (iata_code != '0')\
                AND (type != 'closed')
                ''')
    
    logging.info("Airport data after Renaming columns and Conditional data selection: \n")
    Airport_Details.show(3)
    
    # write to parquet
    logging.info("Sending Airport table back to cloud: ")
    #--------------------------------------------------------------------------------
    # Airport_Details.write.mode('overwrite').\
    # parquet(os.path.join(output_data,'Capstone_Files/Airport_Details.parquet'))
 
    # Airport_Details.write.mode("overwrite")\
    #                .parquet(path=output_data + 'Airport_Details/')
    
#========================================= DONE ==========================================
def Processing_label_descriptions_file(spark, output_data):
    """ 
       Description:
       parsing  SAS-label desctiption file to get codes of port, city, country, state
       Arguments:
            spark: SparkSession object.
            input_data: input source of data table.
            output_data: Target to send the processed data-table
       Returns: None
    """
        
    #-----------COUNTRY CODES------------------1--------------------
    logging.info("Start processing SAS Label Descriptions file: \n")
    #label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    
    label_file = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/I94_SAS_Labels_Descriptions.SAS'
    # reading the SAS file
    with open(label_file) as f:
        Label_Contents = f.readlines()
    
    # slicing rows involved only with valid country codes
    Label_Contents = [country.strip() for country in Label_Contents]
    countries = Label_Contents[10:245]
    
    # splitting & stripping codes for cleaning
    Splitted_country = [country.split("=") for country in countries]

    # replacing and splitting inverted commas from > Splitted_country
    Country_Code = [country[0].replace("'","").strip() for country in Splitted_country]
    Country_Name = [country[1].replace("'","").strip() for country in Splitted_country]
    
    # creating data frame with country codes
    Country_Codes = pd.DataFrame({"Country_Codes" : Country_Code, \
                                  "Country_Names": Country_Name})
    
    # converting country_codes into a > Spark data frame
    Country_Codes = spark.createDataFrame(Country_Codes)    
    
    # casting > 'Country_Codes' into integer data type
    Country_Codes = Country_Codes.withColumn("Country_Codes", col("Country_Codes").cast(IntegerType()))
    
    # Counting numer of rows and display dim_Country_Codes table
    logging.info("Counting number of rows with Country table: "+ str(Country_Codes.count()))
    
    logging.info("Displaying country code and name DIMENSION TABLE: \n")
    Country_Codes.show()
    
    #------------------------------------------------------------------
    # sending 'Country_Codes' back to parquet files 
    # spark.createDataFrame(dim_Country_Codes.items(), ['Country_Codes', 'Country_Names'])\
    #         .write.mode("overwrite")\
    #         .parquet(path=output_data + 'dim_Country_Codes/')
    
    
    #---------------STATE CODE AND NAMES--------------------------------------2-----------------
    # slicing the needed state codes from 'Label_Contents'
    Label_Contents = [state.strip() for state in Label_Contents]
    States = Label_Contents[981:1036]
    
    # splitting and stripping row values
    Splitted_States = [state.split("=") for state in States]
    
    # replacing inverted commas and then stripping empty spaces from Splitted_Sates
    State_Codes = [state[0].replace("'","").strip() for state in Splitted_States]
    State_Names = [state[1].replace("'","").strip() for state in Splitted_States]
    
    # creating data frame with state codes and names
    State_Codes = pd.DataFrame({"State_Codes" : State_Codes, "State_Names": State_Names})
    
    # converting State_codes into a pySpark data frame
    State_Codes = spark.createDataFrame(State_Codes)
    
    # Counting numer of rows and display dim_State_Codes table
    logging.info("Counting number of rows with State table: "+ str(State_Codes.count()))
    
    logging.info("Displaying State code and name DIMENSION TABLE: \n")
    State_Codes.show(5)
    
    #---------------------------------------------------------------------------
    # sending the state codes back to parquet file
    # spark.createDataFrame(dim_State_Codes.items(), ['State_Codes', 'State_Names'])\
    #     .write.mode("overwrite")\
    #     .parquet(path=output_data + 'dim_State_Codes')

    #---------------PORT, CITY & STATE CODE SEGMENTATION---------------------3---------------
    # selecting rows with embedded port and city codes
    Label_Contents = [ports.strip() for ports in Label_Contents]
    Port_locations = Label_Contents[302:894]
    
    # splitting rows with '=' from port_locations
    Splitted_ports = [port.split("=") for port in Port_locations]
    
    # stripping & replacing inverted commas from splitted_ports
    Port_Codes = [ports[0].replace("'","").strip() for ports in Splitted_ports]
    Port_locations = [ports[1].replace("''", "").strip() for ports in Splitted_ports]
    Port_locations = [ports[1].replace("'","").strip() for ports in Splitted_ports]

    # splitting values from port_locations by commas
    Port_Cities = [ports.split(",")[0] for ports in Port_locations]
    Port_States = [ports.split(",")[-1] for ports in Port_locations]
    
    # placing all the separted values into a data frame
    Port_locations = pd.DataFrame({"Port_Codes" : Port_Codes, "Port_Citys": Port_Cities, "Port_States":
                                      Port_States})
    
    # converting dim_Port_locations into a > Spark data frame
    Port_locations = spark.createDataFrame(Port_locations)
    
    # Counting numer of rows and display Port_locations_df table
    logging.info("Displaying Port_locations row COUNT: "+ str(Port_locations.count()))
    
    logging.info("Globalizing 'dim_Port_locations' table for subsequent table-join use: \n")
    logging.info("Displaying Port Code, City and State columns DIMENSION TABLE: \n")    
    
    global Port_table_df
    Port_table_df = Port_locations
    Port_table_df.show(5)
    Port_table_df.printSchema()
   
    #-------------------------------------------------------------------------------
    # sending the state codes back to parquet file
    # spark.createDataFrame(dim_Port_locations.items(), ['Port_Code', 'Port_City','Port_State'])\
    #     .write.mode("overwrite")\
    #     .parquet(path=output_data + 'dim_Port_locations')

    
#============================Temperature data processing=======================DONE============================
def Temperature_data_processing(spark,  output_data):
    """ 
       Descriptions:
       processing temperature data to get dim_temperature table
       Arguments:
            Spark: SparkSession object.
            Input_data: Input source of Temperature table.
            Output_data: Storage locations to send processed data.
       Returns: None
    """

    logging.info("Start processing Global temperature table: \n")
    
    # read temperature data file
    #tempe_data = os.path.join(input_data + 'temperature/GlobalLandTemperaturesByCity.csv')
    
    #df = spark.read.csv(tempe_data, header=True)
    
    #------ Reading option 1 -------------------------
    #temp_df = 'GlobalLandTemperaturesByCity.csv'
    #temp_df = spark.read.format('csv').options(header=True, delimiter=',').load(temp_df)
    #temp_df.show(3)
       
    #-------Reading option 2 -----------------------
    # Read the temperature csv file
    #fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    #temp_df = spark.read.options(delimiter=",").csv(fname,header=True)
    
    Temp_file_path = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.options(delimiter=",").csv(Temp_file_path, header=True)
    
    # creating a temporary view for reading temperature file
    temp_df.createOrReplaceTempView("World_City_Temp")
    
    # World temperature columns selection with SQL query transformation
    World_City_Temperature = spark.sql('''
                        select date(dt) Date_Records,
                                   date_format(date(dt),'yyyy') Year,
                                    date_format(date(dt),'MMMM') Month_Name,
                                      date_format(date(dt),'MM') Month_Num,
                                       date_format(date(dt),'dd') Day,
                                        round(float(AverageTemperature),2) Avg_Temp,
                                          upper(City) City, 
                                           upper(Country) Country
                        from World_City_Temp
                        where to_date(dt,'yyyy-MM-dd') >= '2005-01-01' 
                        AND AverageTemperature IS NOT NULL                        
                  ''')
    
    logging.info("Displaying World temperature with transformations: \n")
    World_City_Temperature.show(7)
    World_City_Temperature.printSchema()
    
    logging.info("Sending World City & Country temperature to cloud:")
    
    # write dim_temperature table to parquet files (Not a dimension table)
    #World_City_Temperature.write.mode("overwrite")\
    #                    .parquet(path=output_data + 'World_City_Temperature/')
    
    #---------------------- YEARLY -------------------------------------------------------------
    logging.info("Yearly World City & Country temperature data after pivot transformations: \n")
    World_City_Temp_Yearly = World_City_Temperature.groupBy(["City","Country"]).pivot("Year")\
                                              .avg("Avg_Temp")
    
    logging.info("Displaying Pivoted Yearly World City & Country temperature: \n")
    World_City_Temp_Yearly.show(5)
    
    # write dim_temperature table to parquet files 
    logging.info("Sending Yearly World City & Country temperature to cloud:")
    
    # World_City_Temp_Yearly.write.mode("overwrite")\
    #                    .parquet(path=output_data + 'World_City_Temp_Yearly/')
    
    #------------------------ US City Temperature -----------------------------------------------
    # separating US City temperature data with some data transformation and selection
    US_Cities_Temperature = spark.sql('''
                                    SELECT date(dt) Date_Records,
                                           date_format(date(dt),'yyyy') Year,
                                           date_format(date(dt),'MMMM') Month_Name,
                                           int(date_format(date(dt),'MM')) Month_Num,
                                           int(date_format(date(dt),'dd')) Day,
                                           round(float(AverageTemperature),3) Avg_Temp,
                                           upper(City) US_City, 
                                           upper(Country) Country
                                     FROM World_City_Temp
                                     WHERE to_date(dt,'yyyy-MM-dd') >= '2005-01-01' 
                                     AND Country = 'United States' 
                                     AND AverageTemperature IS NOT NULL
                                     AND City IS NOT NULL
                                                          ''')
    
    logging.info("US city temperature transformational table here: \n")
    US_Cities_Temperature.show(7)
    
    # write dim_temperature table to parquet files 
    logging.info("Sending Yearly US City & Country temperature to cloud:")
    # US_Cities_Temperature.write.mode("overwrite")\
    #                .parquet(path=output_data + 'Capstone_Files/US_Cities_Temperature/')
    
    #------------------------Yearly US City Temperature -----------------------------
    # Pivoting the City temperature by year to get average temp in a single row for each row 
    Yearly_US_City_Tempr = US_Cities_Temperature.groupBy(["US_City","Country"]).pivot("Year")\
                                              .avg("Avg_Temp")
    
    logging.info("Yearly US city temperature data displayed here: \n")
    Yearly_US_City_Tempr.show(3)
    
    logging.info("Sending Yearly US City & Country temperature to cloud:")
    # Yearly_US_Tempr.write.mode("overwrite")\
    #                .parquet(path=output_data + 'Capstone_Files/Yearly_US_Tempr/')
    
    #----------------------- Monthly US City temperature ------------------------------
    # Pivoting US City temperature by Month to get average temp in a single row for each row 
    Monthly_US_City_Tempr = US_Cities_Temperature.groupBy(["US_City","Country","Year"]).pivot("Month_name")\
                                              .avg("Avg_Temp")
    
    logging.info("Monthly US city temperature data processed here: ")
    Monthly_US_City_Tempr.show(7)
    Monthly_US_City_Tempr.printSchema()
    
    logging.info("Sending Monthly US City & Country temperature to cloud:")
    # Monthly_US_Tempr.write.mode("overwrite")\
    #                .parquet(path=output_data + 'Capstone_Files/Monthly_US_Tempr/')
    
    
    #-------------------------DIMENSION TABLE ------------------------------------
    # creating a temperature dimension table by joining with Port_table
    logging.info("Creating US City dimension table: ") 
    US_Cities_Tempr = US_Cities_Temperature.join(Port_table_df,\
                               (US_Cities_Temperature.US_City == Port_table_df.Port_Citys), how='inner')
    
    # renaming Port_Code and dropping Port_City column
    US_Cities_Tempr = US_Cities_Tempr.withColumnRenamed("Port_Codes","US_Port")
    US_Cities_Tempr = US_Cities_Tempr.drop(Port_table_df.Port_Citys)
    
    dim_US_Citys_Temperature = US_Cities_Tempr.alias('dim_US_Citys_Temperature')
    
    logging.info("Displaying dim_US_Cities_Temperature DIMENSION TABLE: \n")
    dim_US_Citys_Temperature.show(5)
    dim_US_Citys_Temperature.printSchema()
    
    # write dimension table to the cloud source
    logging.info("Sending Monthly US City & Country temperature to cloud:")
    
    # US_Cities_Tempr.write.mode('overwrite').\
    # parquet(os.path.join(output_data,'Capstone_Files/US_Cities_Tempr.parquet'))
    
    # US_Cities_Tempr.write.mode("overwrite")\
    #                .parquet(path=output_data + 'Capstone_Files/US_Cities_Tempr.parquet/')
    

#=====================================DONE=========================================================
def Processing_US_demography_data(spark, output_data):
    """
       Description:
       Processing demograpy data to create a dimension table. 
       Arguments:
            Spark: SparkSession object
            Input_data: Source of input data
            Output_data: Target location of processed table
       Returns: None
    """

    logging.info("Start processing US demographic data: \n")
    
    # read demography data file
    #demog_data = os.path.join(input_data + 'demography/us-cities-demographics.csv')
    #df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_data)
    
    #Demog_data = 'us-cities-demographics.csv'
    #Demog_df = spark.read.format('csv').options(header=True, delimiter=';').load(Demog_data )
   
    # get filepath to us cities demographics data file
    logging.info("Start processing demography Table: \n")
    Demog_file_path = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/us-cities-demographics.csv'
    demog_df = spark.read.options(delimiter=";").csv(Demog_file_path,header=True)
    
    #Create Temporary view for Demography table
    demog_df.createOrReplaceTempView("Demography_table")
    
    # Rename columms with some datatypes transformations
    US_City_Demog_data = spark.sql('''
                         SELECT upper(City) Demog_City, \
                             State State_Name, \
                             float(`Median Age`) Median_Age, \
                             int(`Male Population`) Male_Population, \
                             int(`Female Population`) Female_Population , \
                             int(`Total Population`) Total_Population, \
                             int(`Number of Veterans`) Number_Of_Veterans, \
                             int(`Foreign-Born`) Foreign_Born, \
                             float(`Average Household Size`) Average_Household_Size, \
                             string(Race) Race, \
                             int(Count) Count
                          FROM Demography_table
                                                ''')

    logging.info("Displaying demography data: \n")
    US_City_Demog_data.show(3)
    
    # write >> Transf_Demogrphy_data table >> to parquet files--------> 1
    logging.info("Sending US City demography data to the cloud: ")
    # US_City_Demog_data.write.mode("overwrite")\
    #                    .parquet(path=output_data + 'Capstone_Files/Transf_Demogrphy_data/')
    
    #--------------------------------Creating RACE based population distribution---------------------------
    # Pivot the dataframe by Race to get count in a single row
    US_Race_distrbution = US_City_Demog_data.groupBy(["Demog_City","State_Name"]).pivot("Race").sum("Count")
        
    # Race columns renamed for clarity
    US_Race_distrbution = US_Race_distrbution.withColumnRenamed("American Indian and Alaska Native","American_Indian_Alaska_Native") \
                                        .withColumnRenamed("Asian","Asian_Population") \
                                        .withColumnRenamed("Black or African-American","Black_African_American") \
                                        .withColumnRenamed("Hispanic or Latino", "Hispanic_Latino") \
                                        .withColumnRenamed("White", "White_Population")
    
    # joining the two datasets to create a single combined final dataset and drop duplicates
    US_Demog_N_Race = US_City_Demog_data.join(US_Race_distrbution,["Demog_City","State_Name"])\
                                                    .drop("Race","Count").dropDuplicates()
    
    logging.info("Displaying demography data with Race distributed: \n")
    US_Demog_N_Race.show(3)
    
    # joining Demography_Race table with port_table to create a dimension table
    US_City_Demog_N_Race = US_Demog_N_Race.join(Port_table_df,\
                                US_Demog_N_Race.Demog_City == Port_table_df.Port_Citys, how='inner')
    
    # renaming and dropping columns
    US_City_Demog_N_Race = US_City_Demog_N_Race.withColumnRenamed("Port_Codes","US_Port")
    US_City_Demog_N_Race = US_City_Demog_N_Race.drop(Port_table_df.Port_Citys)
    
    dim_US_City_Race_N_Demog = US_City_Demog_N_Race.alias('dim_US_City_Race_N_Demog')
    
    # displaying dim_US_City_Demog_Race table
    logging.info("Displaying dim_US_City_demography DIMENSION TABLE: ")
    dim_US_City_Race_N_Demog.show(5)     
    dim_US_City_Race_N_Demog.printSchema()
    
    logging.info("Displaying US_City_demography row COUNT: "+ str(dim_US_City_Race_N_Demog.count()))
    
    # write dim_demog_population table to parquet files -----> 2
    logging.info("Sending US City demography data to the cloud. ")
        
    # write to parquet file on S3
         #US_City_Demog_N_Race.write.mode('overwrite').parquet(os.path.join(output_data,'Capstone/US_City_Demog_N_Race.parquet'))
    
    # US_City_Demog_N_Race.write.mode("overwrite")\
    #                    .parquet(path=output_data + 'Capstone_Files/US_City_Demog_N_Race/')

#=========================================================================================================   
def main():
    """
       Descriptons:
       Main funtion that initiates the spark session and 
            Input source: locations from to read data to using spark.
            Output: Showing source where to send the processed files.
    """
    logging.info("Start Processing Data pipeline: \n")
    spark = create_spark_session()
    
    #output_data = "s3a://Farhad-capstone/"
    output_data = 'C:/Users/paralax11/Desktop/Data_Engineering_Udacity_21/CAPSTONE_PROJECT/PROJECT_CAPSTONE_FILES/Practice_Capstone'
    #input_data = SOURCE_S3_BUCKET
    #output_data = DEST_S3_BUCKET
    
    Immigration_data_processing(spark, output_data)  # DONE **
    
    Airports_data_processing(spark, output_data)        # DONE **
    
    Processing_label_descriptions_file(spark, output_data)   # DONE **
    
    Temperature_data_processing(spark, output_data)  # DONE  **
    
    Processing_US_demography_data(spark, output_data)   # DONE   **
    
    logging.info("Data Pipeline Processing is completed.")


if __name__ == "__main__":
    main()
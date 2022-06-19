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

#==================== SENDING TO S3 BUCKET (IF) =====================================================================
# AWS configuration
# config = configparser.ConfigParser()
# config.read('capstone.cfg', encoding='utf-8-sig')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
# DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

#============================= CREATING SPARK SESSION (IF NEEDED )=====================================================
# create the spark session
#def create_spark_session():
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
#        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
#        .enableHiveSupport() \
#        .getOrCreate()
#    return spark


#def create_spark_session():
#    spark = SparkSession.builder\
#        .config("spark.jars.packages",\
#                "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
#        .enableHiveSupport().getOrCreate()
#    return spark


#------------------------- My Spark Session --------------------------------------------------------------------------
# considering I' m using 'immigration sample data'..
logging.info("Start creating Spark session: ")
def create_spark_session():
    spark = SparkSession \
               .builder \
               .appName("Capstone - PyScript") \
               .getOrCreate()
    return spark


#================================ IMMIGRATION DATA PROCESSING ===============================================
# Immigration data processing function
def Immigration_data_processing(spark, output_data):
    """
       Description:
       Processing immigration data to create a fact_immigration table. 
       Creating two dimension table 'dim_Individual_Immigrants_Records' and 'dim_Dated_Arrival_Departure'.
       Arguments:
            spark: A SparkSession object.
            data_input: Reading from workspace Source.
            output_data: Path location of processed table either Workspace or S3 bucket.
       Returns:  None
    """

    logging.info("Redefining date column, renaming columns and data type change: \n")
    
    # reading the 'Sample immigraiton table' here    
    Immig_Sample_Data = 'immigration_data_sample.csv'
    df_immigration = spark.read.format('csv').options(header=True, delimiter=',').load(Immig_Sample_Data)
    
    df_immigration.show(5) 
    
    df_immigration.printSchema()
    
    print("Counting number of NULL values by each column with Immigration table: ")
    
    df_immigration.select([count(when(col(c).isNull(), c))\
                         .alias(c) for c in df_immigration.columns]).show(n=2, truncate=False, vertical=True)
    
    # UDF > converts SAS date fields to date type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    
    # applying the user defined function to convert SAS date and rename the given date column
    df_immigration = df_immigration.withColumn("arrival_Date", get_date(df_immigration.arrdate))
    df_immigration = df_immigration.withColumn("departure_Date", get_date(df_immigration.depdate))
    
    # converting & casting date from SAS format to pySpark date format
    df_immigration = df_immigration.withColumn("Arrival_Date", to_date(col("arrival_Date").cast('date')))
    df_immigration = df_immigration.withColumn("Departure_Date", to_date(col("departure_Date").cast('date')))
    
    # creating a view for SQL query
    df_immigration.createOrReplaceTempView("Immigration_Data")
    
    # Read, rename & redefine columns with Fact Table
    Fact_Immigration = spark.sql('''
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
    
    logging.info("Displaying Fact_Immigration table data after transformation: \n")
    
    Fact_Immigration.show(3)
    
    Fact_Immigration.printSchema()
    
    #------------------------- Sending file to Workspace folder OR Amazon S3 ------------------------------------------------
    # if we send this table to Amazon S3 bucket as parquet file** 
    # Fact_Immigration.write.mode('overwrite').parquet(os.path.join(output_data,'Fact_Immigration/'))
   
    Fact_Immigration.write.mode("overwrite").parquet(path=output_data + 'Fact_Immigration/')
    logging.info("**Sending Fact_Immigration table to Workspace folder is Complete.** ")   
    
    #---------------------------- Creating Individual immigrants Record  DIMENSION TABLE --------------------------    
    
    logging.info("Devising individual immigrants Dimension table: \n")
    # selecting columns relevant & necessary to individual immigrants records nothing more
    dim_Individual_Immigrants_Records = Fact_Immigration.selectExpr('Admission_Num as Entry_Num','Immigration_Id','Arrival_Date', 'Citizenship_Country',\
                          'Immigrants_Age', 'Gender','Departure_Date','Visa_Type', 'Match_Flag').distinct()
    
    logging.info("Displaying Individual Immigrants DIMENSION TABLE: \n")
    
    dim_Individual_Immigrants_Records.show(3)
    
    dim_Individual_Immigrants_Records.printSchema()
    
    #-------------------------- Sending file to Workspace folder OR Amazon S3 ---------------------------------------------
    # sending writing the file back to Workspace
    # dim_Individual_Immigrants_Records.write.mode('overwrite').\
    #                              parquet(os.path.join(output_data,'dim_Individual_Immigrants_Records/'))
    
    dim_Individual_Immigrants_Records.write.mode("overwrite").parquet(path=output_data + 'dim_Individual_Immigrants_Records/')
    logging.info("**Sending Individual_Immigrants_Records table to Workspace folder is Complete.**")
    
    #--------------------- Creating Dated transformation of arrival-departure columns DIMENSION TABLE --------------
    
    logging.info("Creating a Date intensive immigration Dimension table: \n")
    
    # creating temp view for a detail dated information about an immigrant       
    Fact_Immigration.createOrReplaceTempView("Immigration_Time_Line")
    
    # delving into detail dated arrival and departure columns
    dim_Dated_Arrival_Departure = spark.sql('''
                   Select Admission_Num,\
                             Arrival_Date AS Entry_Date,\
                             Citizenship_Country, \
                             int(date_format(date(Arrival_Date),'yyyy')) Arrival_Year,\
                             date_format(date(Arrival_Date),'MMMM') Arrival_Month,\
                             int(date_format(date(Arrival_Date),'dd')) Arrival_Day,\
                             departure_date AS Departure_Date, \
                             int(date_format(date(Departure_Date),'yyyy')) Depart_Year,\
                             date_format(date(Departure_Date),'MMMM') Depart_Month,\
                             int(date_format(date(Departure_Date),'dd')) Depart_Day, \
                             Visa_Type, Port_Code
                      From Immigration_Time_Line
                      WHERE year(Arrival_Date) >= 2016                                      
                  ''')
    
    logging.info("Displaying immigrants date detail DIMENSION TABLE: \n")
    
    dim_Dated_Arrival_Departure.show(3)
    
    dim_Dated_Arrival_Departure.printSchema()
    
    #------------------------ Sending file to Workspace folder OR Amazon S3 --------------------------------------------
    # sending the file back to cloud
    # dim_Dated_Arrival_Departure.write.mode('overwrite').\
    #                                 parquet(os.path.join(output_data,'dim_Dated_Arrival_Departure/'))
    
    dim_Dated_Arrival_Departure.write.mode("overwrite").parquet(path=output_data + 'dim_Dated_Arrival_Departure/')
    logging.info("**Sending dim_Dated_Arrival_Departure table to Workspace folder is Complete.** ")
    
    #--------------------- Travel modes of Immigrants SUPPORTING TABLE ---------------------------------------   
    logging.info("Redefining Travel mode column from immigration table: \n")
    Mode_of_Travel = Fact_Immigration.select(['Immigration_Id','Citizenship_Country', 'Current_State', 'Visa_Type', 'Travel_Mode']).distinct()
    
    # transforming mode of travel to an understandable form
    Mode_of_Travel = Mode_of_Travel.withColumn("Means_of_Travel", \
                   when((Mode_of_Travel.Travel_Mode == 1), lit("Air")) \
                     .when((Mode_of_Travel.Travel_Mode == 2), lit("Sea")) \
                        .when((Mode_of_Travel.Travel_Mode == 3), lit("Land")) \
                          .when((Mode_of_Travel.Travel_Mode == 9), lit("Not Reported")))
    
    logging.info("Displaying immigrants Mode of Travel data: \n")
    
    Mode_of_Travel.show(3)
    
    #--------------------- Sending file to Workspace OR Amazon S3 ---------------------------------------
    # Mode_of_Travel.mode("overwrite").parquet(os.path.join(output_data,'Mode_of_Travel/'))
    
    Mode_of_Travel.write.mode("overwrite").parquet(path=output_data + 'Mode_of_Travel/') 
    
    #---------------------- Visa Type category SUPPORTING TABLE ------------------------------------------- 
    logging.info("Visa purpose categorization of immigrants: \n")
    Visa_Purpose_Type = Fact_Immigration.select(['Immigration_Id','Citizenship_Country', 'Current_State', 'Residency_Country','Visa_Code']).distinct()
    
    # transforming 'visa category type' to understandable form
    Visa_Purpose_Type = Visa_Purpose_Type.withColumn("Visa_Purpose", \
                   when((Visa_Purpose_Type.Visa_Code == 1), lit("Business")) \
                     .when((Visa_Purpose_Type.Visa_Code == 2), lit("Pleasure")) \
                        .when((Visa_Purpose_Type.Visa_Code == 3), lit("Student")))  
    
    # joing 'Mode_of_Travel' and 'Visa_Purpose_Type' by removing duplicated colummns
    Mode_Visa_Purpose = Mode_of_Travel.join(Visa_Purpose_Type, ["Immigration_Id", 'Citizenship_Country', \
                             'Current_State']).drop(Mode_of_Travel.Travel_Mode) 
    
    logging.info("Displaying 'Mode_Visa_Purpose' table data: \n")
    
    Mode_Visa_Purpose.show(3)
    
    #------------------------ Sending file to Workspace folder OR Amazon S3 ------------------------------------------------
    # Mode_Visa_Purpose.mode('overwrite').parquet(os.path.join(output_data,'Mode_Visa_Purpose.parquet'))
    
    Mode_Visa_Purpose.write.mode("overwrite").parquet(path=output_data + 'Mode_Visa_Purpose/') 
    
    #---------------- JOINING  with Fact Immigration table for a final Fact Table------------------------------------------ 
    # Joining by removing duplicated columns
    Fact_Immigration_Table = Fact_Immigration.join(Mode_Visa_Purpose, ["Immigration_Id", 'Citizenship_Country', 'Residency_Country', 'Current_State', 'Visa_Type']).drop('Travel_Mode', 'Visa_Code')
    
    logging.info("Displaying Immigration Fact Table with all inclusive data: \n")
    
    Fact_Immigration_Table.show(3)
    
    Fact_Immigration_Table.printSchema()
    
    #--------------------- Sending file to Workspace folder OR Amazon S3 -------------------------------------------------  
    #  Fact_Immigration_Table.mode('overwrite').parquet(os.path.join(output_data,'Fact_Immigration_Table'))
    
    Fact_Immigration_Table.write.mode("overwrite").parquet(path=output_data + 'Fact_Immigration_Table/')
    logging.info("**Sending Fact_Immigration_Table to Workspace folder is Complete.** ")

    
#============================ AIRPORT DATA Processing ========================================================
def Airports_data_processing(spark, output_data):
    """
       Description:
       Processing the Airport Codes data and after some data transformations.
    
       Arguments:
       Spark: SparkSession object as ceated.
       data_input:: Sourcing from workspace.
       Output_data: Path location of processed table either Workspace or S3 bucket.
       Returns: None    
    """
    
    #read the airports codes csv file
    Airport_df = spark.read.options(delimiter=",").csv("airport-codes.csv",header=True)
    
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
    
    #--------------------- Sending file to Workspace folder OR Amazon S3 -------------------------------  
    # Airport_Details.write.mode('overwrite').parquet(os.path.join(output_data,'Airport_Details/'))
 
    Airport_Details.write.mode("overwrite").parquet(path=output_data + 'Airport_Details/')
    logging.info("**Sending Airport_Details table to Workspace folder is Complete.** ")
     

#============== I94_SAS_LABEL DESCRIPTION FILE data processing ==========================================
def Processing_label_descriptions_file(spark, output_data):
    """ 
       Description:
       parsing  SAS-label desctiption file to get codes of port, city, country, states.
       Arguments:
            spark: SparkSession object.
            data_input: input source of data table (workspace source used).
            output_data: Path location of processed table either Workspace or S3 bucket.
       Returns: None
    """
        
    #----------- COUNTRY CODES DIMENSION TABLE CREATION ------------------------------
    logging.info("Start processing SAS Label Descriptions file: \n")
    # label_file = os.path.join(input_data + "I94_SAS_Labels_Descriptions.SAS")
    label_file = "I94_SAS_Labels_Descriptions.SAS"
       
    # reading the SAS file from workspace
    with open(label_file) as f:
        Label_Contents = f.readlines()
    
    logging.info("**Creating World Country Codes and Names table from SAS Label file. \n")
    
    # slicing rows involved only with valid country codes
    Label_Contents = [country.strip() for country in Label_Contents]
    countries = Label_Contents[10:245]
    
    # splitting & stripping codes for cleaning
    Splitted_country = [country.split("=") for country in countries]

    # replacing and splitting inverted commas from > Splitted_country
    Country_Code = [country[0].replace("'","").strip() for country in Splitted_country]
    Country_Name = [country[1].replace("'","").strip() for country in Splitted_country]
    
    # creating data frame with country codes
    dim_Country_Codes = pd.DataFrame({"Country_Codes" : Country_Code, \
                                  "Country_Names": Country_Name})
    
    # converting country_codes into a > Spark data frame
    dim_Country_Codes = spark.createDataFrame(dim_Country_Codes)    
    
    # casting > 'Country_Codes' into integer data type
    dim_Country_Codes = dim_Country_Codes.withColumn("Country_Codes", col("Country_Codes").cast(IntegerType()))
    
    # Counting numer of rows and display dim_Country_Codes table
    logging.info("Counting number of rows with Country table: "+ str(dim_Country_Codes.count()))
    
    logging.info("Displaying country codes and names DIMENSION TABLE: \n")
    dim_Country_Codes.show()
    
    #------------------------ Sending file to Workspace folder OR Amazon S3 ------------------------------------
    # dim_Country_Codes.write.mode("overwrite").parquet(path=output_data + 'dim_Country_Codes/')
    
    dim_Country_Codes.write.mode("overwrite").parquet(path=output_data + 'dim_Country_Codes/')   
    logging.info("**Sending dim_Country_Codes table to Workspace folder is Complete.** ")
    
    
    #--------------- STATE CODE AND NAMES DIMENSIONT TABLE -------------------------------------------------
    logging.info("**Creating US State Code and Names table from SAS Label file. \n")
    
    # slicing the needed state codes from 'Label_Contents'
    Label_Contents = [state.strip() for state in Label_Contents]
    States = Label_Contents[981:1036]
    
    # splitting and stripping row values
    Splitted_States = [state.split("=") for state in States]
    
    # replacing inverted commas and then stripping empty spaces from Splitted_Sates
    State_Codes = [state[0].replace("'","").strip() for state in Splitted_States]
    State_Names = [state[1].replace("'","").strip() for state in Splitted_States]
    
    # creating data frame with state codes and names
    dim_State_Codes = pd.DataFrame({"State_Codes" : State_Codes, "State_Names": State_Names})
    
    # converting State_codes into a pySpark data frame
    dim_State_Codes = spark.createDataFrame(dim_State_Codes)
    
    # Counting numer of rows and display dim_State_Codes table
    logging.info("Counting number of rows with State table: "+ str(dim_State_Codes.count()))
    
    logging.info("Displaying State code and name DIMENSION TABLE: \n")
    dim_State_Codes.show(5)
    
    #------------------- Sending file to Workspace folder OR Amazon S3 ----------------------------------------
    # dim_State_Codes.write.mode("overwrite").parquet(path=output_data + 'dim_State_Codes/')
    
    dim_State_Codes.write.mode("overwrite").parquet(path=output_data + 'dim_State_Codes/')
    
    logging.info("**Sending dim_State_Codes table to Workspace folder is Complete.**")
    
    
    #--------------- PORT, CITY & STATE CODE SEGMENTATION DIMENSION TABLE -----------------------------------
    logging.info("**Creating Port locations table from SAS Label file. \n")
    
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
    
    # Creating a global variable for resue
    global Port_table_df
    Port_table_df = Port_locations
    Port_table_df.show(5)
    Port_table_df.printSchema()
   
    #------------------- Sending file to Workspace folder OR Amazon S3 ----------------------------------
    # sending the state codes back to parquet file
    # Port_table_df.write.mode("overwrite").parquet(path=output_data + 'Port_table_df/')
           
    Port_table_df.write.mode("overwrite").parquet(path=output_data + 'dim_Port_table_df/')    
    logging.info("**Sending Port_locations table to Workspace folder is Complete.** ")


#====================== TEMPERATURE DATA PROCESSING ===================================================
def Temperature_data_processing(spark,  output_data):
    """ 
       Descriptions:
       processing temperature data.
       Arguments:
            Spark: SparkSession object.
            data_input: Input source of Temperature table(Workspace or S3).
            Output_data: Path location of processed table either Workspace or S3 bucket.
       Returns: None
    """

    logging.info("Start processing Global temperature table: \n")
    
    # read temperature data file from my bucket
    # tempe_df = os.path.join(input_data + 'GlobalLandTemperaturesByCity.csv')
        
    #------ Reading temperature file -------------------------
    temp_df = 'GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.format('csv').options(header=True, delimiter=',').load(temp_df)
    temp_df.show(3)
       
    # creating a temporary view for reading temperature file
    temp_df.createOrReplaceTempView("World_City_Temp")
    
    logging.info("Creating World City & Country temperature to cloud:")
    # World temperature columns selection with SQL query transformation
    World_City_Temperature = spark.sql('''
                        select date(dt) Date_Records,
                                   int(date_format(date(dt),'yyyy')) Year,
                                    date_format(date(dt),'MMMM') Month_Name,
                                      int(date_format(date(dt),'MM')) Month_Num,
                                       int(date_format(date(dt),'dd')) Day,
                                        round(float(AverageTemperature),2) Avg_Temp,
                                          upper(City) Global_City, 
                                           upper(Country) Country
                        from World_City_Temp
                        where to_date(dt,'yyyy-MM-dd') >= '2005-01-01' 
                        AND AverageTemperature IS NOT NULL                        
                  ''')
    
    logging.info("Displaying World City temperature with transformations: \n")
    World_City_Temperature.show(7)
    World_City_Temperature.printSchema()
      
    #------ Sending file to Workspace folder OR Amazon S3 -----------------------------------------------
    # World_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'World_City_Temperature/')
    
    World_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'World_City_Temperature/')
    logging.info("**Sending World_City_Temperature table to Workspace folder is Complete.** ")
    
    #---------------------- PIVOTING  YEARLY WORLD TEMPERATURE DATA --------------------------------------------
    logging.info("Yearly World City & Country temperature data after pivot transformations: \n")
    World_City_Temp_Yearly = World_City_Temperature.groupBy(["Global_City","Country"]).pivot("Year")\
                                              .avg("Avg_Temp")
    
    logging.info("Displaying Pivoted Yearly World City & Country temperature: \n")
    World_City_Temp_Yearly.show(5)
    
    #-------------- Sending file to Workspace folder OR Amazon S3 -----------------------------------------------
    # World_City_Temp_Yearly.write.mode("overwrite").parquet(path=output_data + 'World_City_Temp_Yearly/')
    World_City_Temp_Yearly.write.mode("overwrite").parquet(path=output_data + 'World_City_Temp_Yearly/')
    logging.info("**Sending World_City_Temp_Yearly table to Workspace folder is Complete.** ")
    
    #------------------------ US CITY TEMPERATURE DATA TABLE CREATION -------------------------------------------
    logging.info("Creating only US City temperature table:")
    # separating US City temperature data with some data transformation and selection
    US_Cities_Temperature = spark.sql('''
                                    SELECT date(dt) Date_Records,
                                           int(date_format(date(dt),'yyyy')) Year,
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
    
    logging.info("Displaying US city temperature transformational table here: \n")
    US_Cities_Temperature.show(7)
    
    #-------------- Sending file to Workspace folder OR Amazon S3 -----------------------------------------------
    # US_Cities_Temperature.write.mode("overwrite").parquet(path=output_data + 'US_Cities_Temperature/')
    US_Cities_Temperature.write.mode("overwrite").parquet(path=output_data + 'US_Cities_Temperature/')
    
    #------------------------ YEARLY US CITY TEMPERATURE TABLE -------------------------------------------------
    # Pivoting the City temperature by year to get average temp in a single row for each row 
    Yearly_US_City_Temperature = US_Cities_Temperature.groupBy(["US_City","Country"]).pivot("Year")\
                                              .avg("Avg_Temp")
    
    logging.info("Yearly US city temperature data displayed here: \n")
    Yearly_US_City_Temperature.show(3)
    
    #-------------- Sending file to Workspace folder OR Amazon S3 -------------------------------------------------
    # Yearly_US_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'Yearly_US_City_Temperature/')
    Yearly_US_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'Yearly_US_City_Temperature/')
    logging.info("Sending Yearly_US_City_Temperature to Workspace folder: ")
    
    
    
    #----------------------- MONTHLY US CITY TEMPERATURE TABLE -----------------------------------------------------
    # Pivoting US City temperature by Month to get average temp in a single row for each row 
    Monthly_US_City_Temperature = US_Cities_Temperature.groupBy(["US_City","Country","Year"]).pivot("Month_name")\
                                              .avg("Avg_Temp")
    
    logging.info("Monthly US city temperature data processed here: \n")
    Monthly_US_City_Temperature.show(7)
    Monthly_US_City_Temperature.printSchema()
    
    #-------------- Sending file to Workspace folder OR Amazon S3 --------------------------------------------------
    # Monthly_US_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'Monthly_US_City_Temperature/')
    Monthly_US_City_Temperature.write.mode("overwrite").parquet(path=output_data + 'Monthly_US_City_Temperature/')
    
    logging.info("Sending Monthly US City & Country temperature to Workspace folder is complete.")
    
    #------------------------- US CITY TEMPERATURE DIMENSION TABLE -------------------------------------------------
    # creating a temperature dimension table by joining with Port_table
    logging.info("Creating US City dimension table: ") 
    US_Citys_Temperature = US_Cities_Temperature.join(Port_table_df,\
                               (US_Cities_Temperature.US_City == Port_table_df.Port_Citys), how='inner')
    
    # renaming Port_Code and dropping Port_City column
    US_Citys_Temperature = US_Citys_Temperature.withColumnRenamed("Port_Codes","City_Port")
    US_Citys_Temperature = US_Citys_Temperature.drop(Port_table_df.Port_Citys)
    
    dim_US_Citys_Temperature = US_Citys_Temperature.alias('dim_US_Citys_Temperature')
    
    logging.info("Displaying dim_US_Cities_Temperature DIMENSION TABLE: \n")
    dim_US_Citys_Temperature.show(5)
    dim_US_Citys_Temperature.printSchema()
    
    #-------------- Sending file to Workspace folder OR Amazon S3 --------------------------------------------------
    # dim_US_Citys_Temperature.write.mode("overwrite").parquet(path=output_data + 'dim_US_Citys_Temperature/')
    
    dim_US_Citys_Temperature.write.mode("overwrite").parquet(path=output_data + 'dim_US_Citys_Temperature')
    
    # write dimension table to the cloud source
    logging.info("Sending US City & Country temperature dimension table to WorkSapce folder is complete.")


#==================== DEMOGRAPHY DATA PROCESSING =====================================================
def Processing_US_demography_data(spark, output_data):
    """
       Description:
       Processing demograpy data to create a dimension table. 
       Arguments:
            Spark: SparkSession object
            data_input: Source of input data (Workspace or S3 bucket)
            Output_data: Path location of processed table either Workspace or S3 bucket.
       Returns: None
    """

    logging.info("Start processing US demographic data: \n")
    
    # read demography data file
    logging.info("Start processing demography Table: \n")
   
    demog_data = 'us-cities-demographics.csv'
    demog_df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_data)
    demog_df.show(3)
    
    # Create Temporary view for Demography table
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
    
    #-------------Sending file to Workspace folder OR Amazon S3 ---------------------------------------
    # US_City_Demog_data.write.mode('overwrite').parquet(os.path.join(output_data,'US_City_Demog_data/'))
 
    US_City_Demog_data.write.mode("overwrite").parquet(path=output_data + 'US_City_Demog_data/')
    
    logging.info("Sending US City demography data to WorkSapce folder is complete.")
    
    #-------------------- Creating RACE based population distribution ------------------------------------
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
    
    #---- joining DEMOGRAPHY TABLE with PORT TABLE to create a DIMENSION TABLE-------
    US_City_Demog_N_Race = US_Demog_N_Race.join(Port_table_df,\
                                US_Demog_N_Race.Demog_City == Port_table_df.Port_Citys, how='inner')
    
    # renaming 'Port_Codes' and dropping repeated columns
    US_City_Demog_N_Race = US_City_Demog_N_Race.withColumnRenamed("Port_Codes","Demog_Port")
    US_City_Demog_N_Race = US_City_Demog_N_Race.drop(Port_table_df.Port_Citys)
    
    dim_US_City_Race_N_Demog = US_City_Demog_N_Race.alias('dim_US_City_Race_N_Demog')
    
    # displaying dim_US_City_Demog_Race table
    logging.info("Displaying dim_US_City_demography DIMENSION TABLE: \n")
    dim_US_City_Race_N_Demog.show(5)     
    dim_US_City_Race_N_Demog.printSchema()
    
    #------------- Sending file to Workspace folder OR Amazon S3 ---------------------------------------     
    # dim_US_City_Race_N_Demog.write.mode('overwrite').parquet(os.path.join(output_data,'dim_US_City_Race_N_Demog/'))
    
    dim_US_City_Race_N_Demog.write.mode("overwrite").parquet(path=output_data + 'dim_US_City_Race_N_Demog/')
    logging.info("Sending US City demography data to WorkSapce folder is complete: ")

    
#=========================== MAIN FUNCTION TABLE =========================================================================  
def main():
    """
       Descriptons:
       Main funtion that initiates the spark session and 
       Input source: locations from where to read data to using spark object.
       Output: Showing source path where to send the processed files.
    """
    logging.info("Start Processing Data pipeline: \n")
    spark = create_spark_session()
    
    #input_data = SOURCE_S3_BUCKET
    #output_data = DEST_S3_BUCKET
    
    # output tables goes ton workspace folders.
    output_data = './PyScript_Output_Results/'
    
    Immigration_data_processing(spark, output_data)          # Immigration table data processing **
    
    Airports_data_processing(spark, output_data)             # Airport table data processing **
    
    Processing_label_descriptions_file(spark, output_data)   # SAS Label data processing **
    
    Temperature_data_processing(spark, output_data)          # Temperature data processing  **
    
    Processing_US_demography_data(spark, output_data)        # US Demography data processing   **
    
    logging.info("Data Pipeline Processing is completed.")


if __name__ == "__main__":
    main()

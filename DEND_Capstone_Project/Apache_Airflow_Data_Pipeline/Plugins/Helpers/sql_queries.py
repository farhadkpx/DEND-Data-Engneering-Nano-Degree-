class SqlQueries:
    
    dim_Country_Codes_insert = ("""
          SELECT
                Country_Codes,
                Country_Names
          FROM Staging_Country_Codes   
""")
               
    dim_State_Codes_insert = ("""
          SELECT
                State_Codes,
                State_Names
          FROM  Staging_State_Codes  
""")
    
    
    dim_Port_Locations_insert = ("""
          SELECT
                Port_Codes,
                Port_Citys,
                Port_States
          FROM  Staging_Port_Locations  
""")


    
#-------------------------- US_Cities_Temperature ------------------------------
    dim_US_City_Temperature_insert = ("""
          SELECT
                Date_Records,
                US_City,
                US_Port,
                Port_State,
                Country,
                Year,
                Month_Name,
                Month_Num,
                Day,
                Avg_Temp
          FROM  Staging_US_City_Temperature uc
          INNER JOIN Fact_Immigration_Table ft ON uc.US_Port = ft.Port_Code
          INNER JOIN Fact_Immigration_Table ft ON uc.Port_State = ft.Current_State..??
          
""")

#-------------------US_City_Demog_Race-------------------------------
    dim_US_City_Demog_Race_insert = ("""
         SELECT
               Demog_City,
               State_Name,
               Male_Population,
               Female_Population,
               Total_Population,
               Median_Age,
               Number_Of_Veterans,
               Foreign_Born,
               Average_Household_Size,
               American_Indian_and_Alaska_Native,
               Asian_Population,
               Black_or_African_American,
               Hispanic_or_Latino,
               White_Population
               US_Port,
               Port_State
          FROM  Staging_US_City_Demog_Race cd
          INNER JOIN  Fact_Immigration_Table ft ON cd.US_Port = ft.Port_Code
          INNER JOIN  Fact_Immigration_Table ft ON cd.Port_State = ft.Current_State
""")

#----------------------------Fact_Immigration_Inclusive---------------------
    Fact_Immigration_Table_insert = ( """
              SELECT
                     Immigration_Id,
                     Immigrants_Age,
                     Citizenship_Country,
                     Residency_Country,
                     Current_State,
                     Visa_Type,
                     Immigration_Year,
                     Immigration_Month,
                     Port_Code,
                     Arrival_Date,
                     Departure_Date, 
                     Match_Flag, 
                     Birth_Year, 
                     Gender,  
                     Airline_Code,  
                     Admission_Num,
                     Flight_Num,  
                     Means_of_Travel,
                     Visa_Purpose
                FROM  Staging_Fact_Immigration_Table im
                LEFT JOIN  dim_Country_Codes AS cc  ON  im.Citizenship_Country = cc.Country_Codes
                LEFT JOIN  dim_Country_Codes AS cr  ON  im.Residency_Country = cr.Country_Codes
                LEFT JOIN  dim_State_Codes AS st    ON  im.Current_State = st.State_Codes
                LEFT JOIN  dim_Port_Locations AS pl ON  im.Current_State = pl.Port_States
                LEFT JOIN  dim_Port_Locations AS pc ON  im.Port_Code = pc.Port_Codes
                LEFT JOIN  dim_Individual_Immigrants_Records ir  ON  im.Admission_Num = ir.Entry_Num
                LEFT JOIN  dim_Dated_Arrival_Departure dt  ON im.Arrival_Date = dt.Entry_Date
                LEFT JOIN  dim_US_City_Temperature uc ON im.Port_Code = uc.US_Port
                LEFT JOIN  dim_US_City_Demog_Race  ur ON im.Port_Code = ur.US_Por
                """)
    
#----------------------Individual_Immigrants_Records---------------------------
    dim_Individual_Immigrants_Records_insert = ("""
            SELECT
                 Entry_Num,
                 Immigration_Id,
                 Arrival_Date,
                 Citizenship_Country,
                 Immigrants_Age,
                 Gender,
                 Departure_Date,
                 Visa_Type,
                 Match_Flag
           FROM  Staging_Individual_Immigrants_Records ir
           INNER JOIN Fact_Immigration_Table ft ON ir.Entry_Num = ft.Admission_Num
           """)
    
#--------------------Dated_Arrival_Departure------------------------------
    dim_Dated_Arrival_Departure_insert = (""" 
           SELECT
                 Entry_Date,
                 Admission_Num,
                 Citizenship_Country,
                 Arrival_Year,
                 Arrival_Month,
                 Arrival_Day,
                 Departure_Date,
                 Depart_Year,
                 Depart_Month,
                 Depart_Day,
                 Visa_Type,
                 Port_Code
            FROM  Staging_Dated_Arrival_Departure dt 
            INNER JOIN Fact_Immigration_Table ft ON dt.Entry_Date = ft.Arrival_Date
""")
    


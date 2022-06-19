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
                uc.Date_Records,
                uc.Year,
                uc.Month_Name,
                uc.Month_Num,
                uc.Day,
                uc.Avg_Temp,
                uc.US_City,
                uc.Country,
                uc.City_Port,
                uc.Port_States
          FROM  Staging_US_City_Temperature uc
          
          
""")

#-------------------US_City_Demog_Race-----------------------------------------
    dim_US_City_Demog_Race_insert = ("""
         SELECT
               cd.Demog_City,
               cd.State_Name,
               cd.Male_Population,
               cd.Female_Population,
               cd.Total_Population,
               cd.Median_Age,
               cd.Number_Of_Veterans,
               cd.Foreign_Born,
               cd.Average_Household_Size,
               cd.American_Indian_Alaska_Native,
               cd.Asian_Population,
               cd.Black_African_American,
               cd.Hispanic_Latino,
               cd.White_Population,
               cd.Demog_Port,
               cd.Port_States
          FROM  Staging_US_City_Demog_Race cd
          
""")

#----------------------------Fact_Immigration_Inclusive----------------------------------
    Fact_Immigration_Table_insert = ( """
              SELECT
                    im.Immigration_Id,
                    im.Citizenship_Country,
                    im.Residency_Country,
                    im.Current_State,
                    im.Visa_Type,
                    im.Immigration_Year,
                    im.Immigration_Month,
                    im.Port_Code,
                    im.Arrival_Date,
                    im.Departure_Date,
                    im.Immigrants_Age,
                    im.Match_Flag,
                    im.Birth_Year,
                    im.Gender,
                    im.Airline_Code,
                    im.Admission_Num,
                    im.Flight_Num,
                    im.Means_of_Travel,
                    im.Visa_Purpose
              FROM  Staging_Immigration_Fact_Table im
              LEFT JOIN  dim_Country_Codes AS cc  ON  im.Citizenship_Country = cc.Country_Codes
              LEFT JOIN  dim_Country_Codes AS cr  ON  im.Residency_Country = cr.Country_Codes
              LEFT JOIN  dim_State_Codes AS st    ON  im.Current_State = st.State_Codes
              LEFT JOIN  dim_Port_Locations AS pl ON  im.Current_State = pl.Port_States
              LEFT JOIN  dim_Port_Locations AS pc ON  im.Port_Code = pc.Port_Codes
              LEFT JOIN  dim_Individual_Immigrants_Records ir  ON  im.Admission_Num = ir.Entry_Num
              LEFT JOIN  dim_Dated_Arrival_Departure dt  ON im.Arrival_Date = dt.Entry_Date
              LEFT JOIN  dim_US_City_Temperature uc ON im.Port_Code = uc.City_Port
              LEFT JOIN  dim_US_City_Demog_Race  ur ON im.Port_Code = ur.Demog_Port
                """)
    
#----------------------Individual_Immigrants_Records---------------------------------------------
    dim_Individual_Immigrants_Records_insert = ("""
            SELECT
                 ir.Entry_Num,
                 ir.Immigration_Id,
                 ir.Arrival_Date,
                 ir.Citizenship_Country,
                 ir.Immigrants_Age,
                 ir.Gender,
                 ir.Departure_Date,
                 ir.Visa_Type,
                 ir.Match_Flag
           FROM  Staging_Individual_Immigrants_Records ir
           """)
    
#--------------------Dated_Arrival_Departure-------------------------------------------------------
    dim_Dated_Arrival_Departure_insert = (""" 
           SELECT
                 dt.Entry_Date,
                 dt.Admission_Num,
                 dt.Citizenship_Country,
                 dt.Arrival_Year,
                 dt.Arrival_Month,
                 dt.Arrival_Day,
                 dt.Departure_Date,
                 dt.Depart_Year,
                 dt.Depart_Month,
                 dt.Depart_Day,
                 dt.Visa_Type,
                 dt.Port_Code
            FROM  Staging_Dated_Arrival_Departure dt 
            
""")
    



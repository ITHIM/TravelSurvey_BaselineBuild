

#prepare for SQL processing
library(sqldf)
library(dplyr)


####################### STARTS BUILD PROCESS OF BASELINE ###############################

# make files thinner for inner_join
trip2014 <- trip2014[, c('SurveyYear', 'TripID', 'DayID', 'IndividualID', 'HouseholdID', 
                           'PSUID', 'W5', 'W5xHH', 'TravDay', 'SeriesCall_B01ID', 'ShortWalkTrip_B01ID', 
                           'NumStages', 'MainMode_B03ID', 'MainMode_B04ID', 'MainMode_B11ID', 'TripTotalTime', 
                           'TripTravTime', 'TripDisIncSW', 'TripDisExSW', 'JJXSC', 'JOTXSC', 'JTTXSC', 
                           'JD')]
ind2014 <- ind2014[   ,  c('IndividualID', 'Age_B01ID',  'Sex_B01ID', 'NSSec_B03ID',  'CarAccess_B01ID','HouseholdID',
                           'NSSec_B03ID', 'IndIncome2002_B02ID', 'EthGroupTS_B02ID' )]

household2014  <- household2014[ ,c('HouseholdID', 'HHoldGOR_B02ID')]


# filter days/stages
day2014 <-  day2014[ ,]
stage2014 <- stage2014[ ,c('SurveyYear', 'StageID', 'TripID', 'DayID', 
                           'IndividualID', 'HouseholdID', 'PSUID', 'VehicleID', 
                           'IndTicketID', 'PersNo', 'TravDay', 'JourSeq', 'StageMode_B03ID',
                           'StageMode_B04ID','StageMode_B11ID','StageSeq', 'StageDistance',
                           'StageDistance_B01ID', 'StageTime', 
                           'StageTime_B01ID', 'StageMain_B01ID', 'SSXSC', 'STTXSC', 'SD')]


# create initial baseline (no mMETs , filtered to >=18 y.o., English regions 1-10)

household2014$HHoldGOR_B02ID = as.character(household2014$HHoldGOR_B02ID)

str_sql <- 'SELECT T2.Age_B01ID, T2.Sex_B01ID, T3.HHoldGOR_B02ID,
T2.CarAccess_B01ID, T2.NSSec_B03ID, T2.IndIncome2002_B02ID, T2.EthGroupTS_B02ID, 
T1.SurveyYear, T1.TripID, T1.DayID, T1.IndividualID, T1.HouseholdID, 
T1.PSUID, T1.W5, T1.W5xHH, T1.TravDay, T1.SeriesCall_B01ID, T1.ShortWalkTrip_B01ID, 
T1.NumStages, T1.MainMode_B03ID, T1.MainMode_B04ID, T1.MainMode_B11ID, T1.TripTotalTime, 
T1.TripTravTime, T1.TripDisIncSW, T1.TripDisExSW, T1.JJXSC, T1.JOTXSC, T1.JTTXSC, 
T1.JD, 0 AS Pcyc, 0 AS now_cycle, T3.HHoldGOR_B02ID 

FROM (trip2014 as T1 INNER JOIN ind2014 as T2 ON T1.IndividualID = T2.IndividualID) 
INNER JOIN household2014 as T3 ON T2.HouseholdID = T3.HouseholdID  

WHERE (((T2.Age_B01ID)>=8) AND ((T3.HHoldGOR_B02ID)<10))
ORDER BY T3.HHoldGOR_B02ID '


bl2014 <-sqldf(x=str_sql)
names(bl2014)

rm(day2014, household2014, ind2014, stage2014, trip2014)

#add extra variables
bl2014$Age = bl2014$Sex =NULL
bl2014$Age[bl2014$Age_B01ID<16] <- '16.59'
bl2014$Age[bl2014$Age_B01ID>=16] <- '60plus'
bl2014$Sex[bl2014$Sex_B01ID==1] <- 'Male'
bl2014$Sex[bl2014$Sex_B01ID==2] <- 'Female'
bl2014$agesex <- paste0(bl2014$Age,bl2014$Sex)


################ CYCLABLE TRIPS  ##################

#create cyclable trips  
str_sql <- 'SELECT T1.SurveyYear, T1.TripID, T1.IndividualID, T2.TravDay, 
T1.W5, T1.W5xHH, T1.SeriesCall_B01ID, T1.ShortWalkTrip_B01ID,
T1.NumStages, T1.MainMode_B03ID, T1.TripTotalTime, T1.TripTravTime,  
T1.TripDisIncSW, T1.TripDisExSW, T1.JJXSC, T1.JOTXSC, T1.JTTXSC, T1.JD, 
0 AS Cycl_Impossible, T3.Age_B01ID, 
T3.Sex_B01ID, T3.NSSec_B03ID, T3.CarAccess_B01ID

FROM (trip2014 as T1 INNER JOIN day2014 as T2 ON T1.DayID = T2.DayID)
INNER JOIN ind2014 as T3 ON T1.IndividualID = T3.IndividualID

WHERE (((T3.Age_B01ID)>=8)) '

tripscyclable <-sqldf(x=str_sql)


############### WALKABLE TRIPS  (= trips w. walked stages)

str_sql <- 'SELECT T1.TripID, T2.StageID, T4.TravDay, T2.IndividualID, T2.StageSeq, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, T2.StageMode_B11ID, 
T2.StageShortWalk_B01ID, T2.W5, T2.W5xHH, T2.SSXSC, T2.STTXSC, T2.SD, 
T3.Age_B01ID 

FROM ((trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID = T2.TripID) 
INNER JOIN ind2014 AS T3 ON T2.IndividualID = T3.IndividualID) 
INNER JOIN day2014 AS T4 ON T2.DayID = T4.DayID

WHERE (((T2.StageMode_B04ID)=1) AND ((T3.Age_B01ID)>=8))

ORDER BY T1.TripID '

# columns not in dataset:    T2.SD_woW5, T2.SSXSC_woW5, T2.STTXSC_woW5, 

tripswalkstages <- sqldf(x=str_sql)


#####################  COMBINE: CYCLABLE trips + WALKABLE stages:

sql_str <- 'SELECT T1.SurveyYear, T1.TripID, T1.NumStages, 
T1.IndividualID, T1.TravDay, T1.SeriesCall_B01ID, 
T1.ShortWalkTrip_B01ID, T1.MainMode_B03ID, T1.TripTotalTime, 
T1.TripDisIncSW, T1.TripDisExSW, T2.StageID, 
T2.StageMode_B04ID, T2.StageDistance, T2.StageTime, 
T2.W5, T2.W5xHH, T2.SSXSC, T2.STTXSC, 
T2.SD

FROM tripscyclable AS T1 LEFT JOIN tripswalkstages AS T2 ON T1.TripID = T2.TripID

ORDER BY T1.SurveyYear, T1.NumStages DESC , T2.StageID '

#NON-EXISTENT COLUMNS:  , T2.SD_woW5, T2.SSXSC_woW5, T2.STTXSC_woW5
wc <- sqldf(x=sql_str)    #cyclable trips w. walked stages added



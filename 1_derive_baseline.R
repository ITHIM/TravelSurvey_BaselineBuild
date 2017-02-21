
#prepare for SQL processing
library(sqldf)
library(dplyr)


####################### STARTS BUILD PROCESS OF BASELINE ###############################

# make files thinner for inner_join 
trip2014 <- trip2014[, c('SurveyYear', 'TripID', 'DayID', 'IndividualID', 'HouseholdID', 
                           'PSUID', 'W5', 'W5xHH', 'TravDay', 'SeriesCall_B01ID', 'ShortWalkTrip_B01ID', 
                           'NumStages', 'MainMode_B03ID', 'MainMode_B04ID', 'MainMode_B11ID',
                           'TripPurpose_B01ID', 'TripPurpose_B02ID', 
                           'TripTotalTime', 'TripTravTime', 'TripDisIncSW', 'TripDisExSW',
                           'JJXSC', 'JOTXSC', 'JTTXSC', 'JD')]

ind2014 <- ind2014[   ,  c('IndividualID', 'Age_B01ID',  'Sex_B01ID', 'NSSec_B03ID',  'CarAccess_B01ID','HouseholdID',
                           'NSSec_B03ID', 'IndIncome2002_B02ID', 'EthGroupTS_B02ID' )]

household2014  <- household2014[ ,c('HouseholdID', 'HHoldGOR_B02ID')]


# filter days/stages
#day2014 <-  day2014[ ,]
stage2014 <- stage2014[ ,c('SurveyYear', 'StageID', 'TripID', 'DayID', 
                           'IndividualID', 'HouseholdID', 'PSUID', 'VehicleID', 'StageShortWalk_B01ID',
                           'IndTicketID', 'PersNo', 'TravDay', 'JourSeq', 'StageMode_B03ID',
                           'StageMode_B04ID','StageMode_B11ID','StageSeq', 'StageDistance',
                           'StageDistance_B01ID', 'StageTime', 
                           'StageTime_B01ID', 'StageMain_B01ID', 
                            'W5', 'W5xHH','SSXSC', 'STTXSC', 'SD')]

# T2.W5, T2.W5xHH, T2.SSXSC, T2.STTXSC, T2.SD,    additional fields for extra precision


#### Create initial baseline = no mMETs , filtered to >=18 y.o., English regions 1..9)

#household2014$HHoldGOR_B02ID = as.character(household2014$HHoldGOR_B02ID)

str_sql <- 'SELECT T2.Age_B01ID, T2.Sex_B01ID, T2.CarAccess_B01ID, T2.NSSec_B03ID, 
T2.IndIncome2002_B02ID, T2.EthGroupTS_B02ID, 
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

#add extra variables
bl2014$Age = bl2014$Sex =NA
bl2014$Age[bl2014$Age_B01ID<16] <- '16.59'
bl2014$Age[bl2014$Age_B01ID>=16] <- '60plus'
bl2014$Sex[bl2014$Sex_B01ID==1] <- 'Male'
bl2014$Sex[bl2014$Sex_B01ID==2] <- 'Female'
bl2014$agesex <- paste0(bl2014$Age,bl2014$Sex)


############### TRIPS w. P.A.  (= trips w. WALKING/CYCLING stages)

str_sql <- 'SELECT T1.TripID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID = T2.TripID) 

WHERE   T2.StageMode_B04ID =1   

ORDER BY T1.TripID '

walktrips <- sqldf(x=str_sql)

str_sql <- 'SELECT T1.TripID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID = T2.TripID) 

WHERE   T2.StageMode_B04ID =2   

ORDER BY T1.TripID '

cycletrips <- sqldf(x=str_sql)

###### calculate  TIMES/DISTANCES (for METs stages)
str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumofWStageDistance, 
Sum(T1.StageTime) AS SumOfWStageTime

FROM walktrips AS T1 GROUP BY T1.TripID   '

walktrips1 <- sqldf(x= str_sql)    # walked trips

str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumofCStageDistance, 
Sum(T1.StageTime) AS SumOfCStageTime

FROM cycletrips AS T1 GROUP BY T1.TripID   ' 

cycletrips1 <- sqldf(x= str_sql)    # cycled trips

rm(walktrips, cycletrips)

#####################  COMBINE: bl trips <>     W/C stages METs times:

bl2014 = left_join(bl2014, walktrips1, by="TripID")
bl2014 = left_join(bl2014, cycletrips1, by="TripID")

bl2014$SumofWStageDistance[is.na(bl2014$SumofWStageDistance)]=0
bl2014$SumOfWStageTime[is.na(bl2014$SumOfWStageTime)]= 0

bl2014$SumofCStageDistance[is.na(bl2014$SumofCStageDistance) ] = 0
bl2014$SumOfCStageTime[is.na(bl2014$SumOfCStageTime) ] = 0


################## MATCHING BASELINE <>  A.P.S.


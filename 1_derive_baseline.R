
#prepare for SQL processing
library(sqldf)
library(dplyr)
library(data.table)

options("scipen" = 20)
memory.size(max = T)

datapath = './data/'

####################### STARTS BUILD PROCESS OF BASELINE ###############################

# make files thinner for inner_join 
trip2014 <- trip2014[, c('SurveyYear', 'TripID', 'DayID', 'IndividualID', 'HouseholdID', 
                           'PSUID', 'W5', 'W5xHH', 'TravDay', 'SeriesCall_B01ID', 'ShortWalkTrip_B01ID', 
                           'NumStages', 'MainMode_B03ID', 'MainMode_B04ID', 'MainMode_B11ID',
                           'TripPurpose_B01ID', 'TripPurpose_B02ID', 
                           'TripTotalTime', 'TripTravTime', 'TripDisIncSW', 'TripDisExSW',
                           'JJXSC', 'JOTXSC', 'JTTXSC', 'JD')]

ind2014 <- ind2014[   ,  c('IndividualID', 'HouseholdID', 'Age_B01ID', 'Sex_B01ID',
                           'CarAccess_B01ID', 'BicycleFreq_B01ID', 'WalkFreq_B01ID',
                           'Cycle12_B01ID', 'NSSec_B03ID', 'IndIncome2002_B02ID',
                           'EthGroupTS_B02ID' )]

household2014  <- household2014[ ,c('HouseholdID', 'HHoldGOR_B02ID')]


stage2014 <- stage2014[ ,c('SurveyYear', 'StageID', 'TripID', 'DayID', 
                           'IndividualID', 'HouseholdID', 'PSUID', 'VehicleID', 'StageShortWalk_B01ID',
                           'IndTicketID', 'PersNo', 'TravDay', 'JourSeq', 'StageMode_B03ID',
                           'StageMode_B04ID','StageMode_B11ID','StageSeq', 'StageDistance',
                           'StageDistance_B01ID', 'StageTime', 
                           'StageTime_B01ID', 'StageMain_B01ID', 
                            'W5', 'W5xHH','SSXSC', 'STTXSC', 'SD')]

# T2.W5, T2.W5xHH, T2.SSXSC, T2.STTXSC, T2.SD,    additional fields for extra precision


#### Create INITIAL BASELINE  = no mMETs , filtered to >=18 y.o., English regions 1..9)

# build baseline= trips <> ind <> household
str_sql <- 'SELECT T2.Age_B01ID, T2.Sex_B01ID, T2.CarAccess_B01ID, T2.NSSec_B03ID, 
T2.IndIncome2002_B02ID, T2.EthGroupTS_B02ID, 

T1.SurveyYear, T1.TripID, T1.DayID, T1.IndividualID, T1.HouseholdID, 
T1.PSUID, T1.W5, T1.W5xHH, T1.TravDay, T1.SeriesCall_B01ID, T1.ShortWalkTrip_B01ID, 
T1.NumStages, T1.MainMode_B03ID, T1.MainMode_B04ID, T1.MainMode_B11ID, T1.TripTotalTime, 
T1.TripTravTime, T1.TripDisIncSW, T1.TripDisExSW, T1.JJXSC, T1.JOTXSC, T1.JTTXSC, 
T1.JD, T3.HHoldGOR_B02ID 

FROM (trip2014 as T1 INNER JOIN ind2014 as T2 ON T1.IndividualID  = T2.IndividualID) 
INNER JOIN household2014 as T3 ON T1.HouseholdID  = T3.HouseholdID  

WHERE  T3.HHoldGOR_B02ID<10
ORDER BY T1.TripID, T3.HHoldGOR_B02ID '    

# ages 18-84 already filtered in ind2014

bl2014 <- sqldf(x =str_sql)
names(bl2014)


# process short walks
df <- bl2014[bl2014$MainMode_B03ID==1,]
shortwalks <- data.frame()

for (i in 1:6) {shortwalks <- rbind(shortwalks,df)}
bl2014 <- rbind(bl2014,shortwalks)

rm(df, shortwalks)


#cycling related vars
bl2014$Cycled = bl2014$Pcyc = bl2014$now_cycle = 0
sel = ( bl2014$MainMode_B03ID == 3 )
bl2014$Cycled[sel] = 1

bl2014 = setDT(bl2014)[, cyclist := max(Cycled == 1), by = IndividualID]  #flag actual cyclists
bl2014 = as.data.frame(bl2014)  

#add extra variables for scenarios build
bl2014$Age  = bl2014$Sex  =NA
bl2014$Age[bl2014$Age_B01ID< 16] <- '16.59'
bl2014$Age[bl2014$Age_B01ID>= 16] <- '60plus'
bl2014$Sex[bl2014$Sex_B01ID== 1] <- 'Male'
bl2014$Sex[bl2014$Sex_B01ID== 2] <- 'Female'
bl2014$agesex <- paste0(bl2014$Age, bl2014$Sex)


########        CALCULATE P.A. = NTS  + APS recreational  (WALKING + CYCLING)

## 1: NTS WALKING:  stages time/distance, as per agreed rules 

# utility walking:
str_sql <- 'SELECT T1.TripID, T1.TripPurpose_B01ID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID  = T2.TripID) 

WHERE   (T1.[TripPurpose_B01ID]<>17 AND T2.[StageMode_B04ID]= 1  AND T2.StageTime > 10 )

ORDER BY T1.TripID '

utilwalkstages <- sqldf(x= str_sql)

#  all walking:
str_sql <- 'SELECT T1.TripID, T1.TripPurpose_B01ID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID  = T2.TripID) 

WHERE   T2.[StageMode_B04ID]= 1  

ORDER BY T1.TripID '

walkstages <- sqldf(x= str_sql)

## 2: NTS CYCLING:  stages time/distance 

str_sql <- 'SELECT T1.TripID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID  = T2.TripID) 

WHERE   T2.StageMode_B04ID = 2 

ORDER BY T1.TripID '

cyclestages <- sqldf(x=str_sql)

###### calculate NTS TIMES/DISTANCES per trip (NTS)
str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumWStageDistance, 
Sum(T1.StageTime) AS SumWStageTime

FROM walkstages AS T1 GROUP BY T1.TripID   '

walktrips <- sqldf(x= str_sql)    # walked trips

str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumutilWStageDistance, 
Sum(T1.StageTime) AS SumutilWStageTime

FROM utilwalkstages AS T1 GROUP BY T1.TripID   '

utilwalktrips <- sqldf(x= str_sql)    # utility walk trips

######### same for CYCLING

str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumCStageDistance, 
Sum(T1.StageTime) AS SumCStageTime

FROM cyclestages AS T1 GROUP BY T1.TripID   ' 

cycletrips <- sqldf(x= str_sql)    # cycled trips

rm(walkstages, utilwalkstages, cyclestages)

#####################  COMBINE: add W/C stages dist/times to bl trips (NTS)

bl2014  = left_join(bl2014, walktrips, by ="TripID")
bl2014  = left_join(bl2014, utilwalktrips, by ="TripID")
bl2014  = left_join(bl2014, cycletrips, by ="TripID")
rm(walktrips, cycletrips, utilwalktrips)

bl2014$SumWStageDistance[is.na(bl2014$SumWStageDistance)] = 0
bl2014$SumWStageTime[is.na(bl2014$SumWStageTime)] = 0

bl2014$SumutilWStageDistance[is.na(bl2014$SumutilWStageDistance)] = 0
bl2014$SumutilWStageTime[is.na(bl2014$SumutilWStageTime)] = 0


bl2014$SumCStageDistance[is.na(bl2014$SumCStageDistance) ]  = 0
bl2014$SumCStageTime[is.na(bl2014$SumCStageTime) ]  = 0

# CHECK: create a unique TripID 
bl2014$TripID <-  1:nrow(bl2014)

saveRDS(object = bl2014, file.path(datapath, 'bl2014_APS.Rds'))  #save final baseline
save.image()

################## MATCHING individuals <>  A.P.S. 
#### prepare APS for the matching

datapath = './data/'

#latest *v4* file from Anna (adds the variable dur_walk10_all_wk’)
aps  = readRDS(paste0(datapath,'aps_proc_v4.Rds')) 

#eliminate all NAs from APS file
aps = aps[! (is.na(aps[ ,'id']) | is.na(aps[ ,'nonwhite']) |  is.na(aps[ ,'age'])  ) , ]
sum(is.na(aps))   #check no NAs left

#recode region-sex-age bands - walk duration
aps$region = as.character(aps$region)
aps$region  = recode(.x  = aps$region,  "East" = 6, "East Midlands" = 4, "London" = 7, 
                    "North East" = 1, "North West" =2, "South East" = 8, "South West" = 9,
                    "West Midlands" = 5, "Yorkshire" = 3)


# categs. as in NTS: 1 =male, 2 =female
aps$male  = recode(aps$male, '1' =1, '0' =2)
aps$nonwhite  = recode(aps$nonwhite, '0' =1, '1' =2)

#NTS baseline criteria: 18y.o or older
aps  = aps[aps$age>17 & aps$age<85, ]

#recode age
aps$ageband <- cut(aps$age, breaks  = c(18:20, 21, 26, 30, 40, 50, 60, 65, 70, 75, 80, 85, Inf),
labels  = c(8:21), right  = F)
aps$ageband = as.integer(levels(aps$ageband))[aps$ageband]

#recode duration of walking for 10+ min bouts
aps$dur_walk10_all_wk[ is.na(aps$dur_walk10_all_wk) ] = 0
aps$bin.dur_walk10_all_wk  = cut(aps$dur_walk10_all_wk, breaks = c(-Inf, 2.5, 5, 10, Inf), 
                                     labels =c(1:4), right = F)

# aps$dur_walk10_utility_wk[is.na(aps$dur_walk10_utility_wk)] = 0
# aps$bin.dur_walk10_utility_wk  = cut(aps$dur_walk10_utility_wk, breaks  = c(0, 1, 3, 6, Inf), 
#                                 labels =c(0:3), right = F)

#binary variable for total cycling
aps$bin.days_cycle_all_wk  = 0
sel = (aps$days_cycle_all_wk > 0.75) 
aps$bin.days_cycle_all_wk [ sel ]  = 1

#############  PROCESS INDIVIDUALS
# add region & subset to England
ind2014 = inner_join(ind2014, household2014, by = "HouseholdID")
ind2014 = ind2014[ind2014$HHoldGOR_B02ID<10, ]   # 137,393 indiv.

# impute cycling in ind2014
ind2014$BicycleFreq_B01ID = recode(ind2014$BicycleFreq_B01ID, '-10'= 0, '-9'= 0, '-8'= 0,
                               '1'= 1, '2'=1, '3'= 0, '4'= 0, '5'= 0, '6'= 0, '7'= 0)

sel = ( ind2014$Cycle12_B01ID== 2 | ind2014$Cycle12_B01ID== 3)
ind2014$BicycleFreq_B01ID[ sel ] = 0


# add WC times per individual
str_sql='select T1.*, sum(T2.SumWStageTime) AS WalkTime, 
                      sum(T2.SumutilWStageTime) AS utilWalkTime,
                      sum(T2.SumCStageTime) AS CycleTime

        FROM ind2014 AS T1 
        LEFT JOIN bl2014 AS T2 
        ON T1.IndividualID=T2.IndividualID
        GROUP BY T1.IndividualID    '

indiv.MET = sqldf(str_sql)      

indiv.MET$WalkTime[is.na(indiv.MET$WalkTime)] = indiv.MET$CycleTime[is.na(indiv.MET$CycleTime)] = 
    indiv.MET$utilWalkTime[is.na(indiv.MET$utilWalkTime)] = 0

indiv.MET$WalkTime.h = round(indiv.MET$WalkTime/60, digits = 1)
indiv.MET$utilWalkTime.h = round(indiv.MET$utilWalkTime/60, digits = 1)
indiv.MET$CycleTime.h = round(indiv.MET$CycleTime/60, digits = 1)

# indiv.MET$bin.WalkTime.h = cut(x = indiv.MET$utilWalkTime.h, breaks  = c(0, 1, 3, 6, Inf),
#                                labels = c(0:3), right = F)

indiv.MET$bin.WalkTime.h = cut(x = indiv.MET$utilWalkTime.h, breaks  = c(-Inf, 0, 2, 4, Inf),
                               labels = c(1:4), right = T)


#create separate file of people w/o trips
indiv.notrips = indiv.MET[ !(indiv.MET$IndividualID %in% bl2014$IndividualID), ]
saveRDS(object = indiv.notrips, file.path(datapath, 'indiv.notrips.Rds'))

### MATCHING APS vs. NTS individuals 
### initially on 6 [7] variables: age-sex-region-ethnicity-walking-cycling - [SES]
#aps$dur_walk10_healthrec_wk = aps$dur_walk10_healthrec_wk/2

# 6 vars (use either method depending on memory size)
str_sql  = "SELECT T1.id, T1.weightla_truepop, T1.mets_sport_wk,
            (T1.dur_walk10_healthrec_wk/2) AS walkAPS, T1.dur_cycle_rec_wk AS cycleAPS,
            T2.IndividualID, T2.Age_B01ID, T2.Sex_B01ID, T2.HHoldGOR_B02ID,
            T2.EthGroupTS_B02ID, T2.WalkTime, T2.CycleTime

           FROM aps AS T1 INNER JOIN [indiv.MET] AS T2

           ON (T1.ageband = T2.Age_B01ID)
           AND (T1.male  = T2.Sex_B01ID)
           AND (T1.region = T2.HHoldGOR_B02ID)
           AND (T1.nonwhite = T2.EthGroupTS_B02ID)
           AND (T1.[bin.dur_walk10_utility_wk] =  T2.[bin.WalkTime.h] )
           AND (T1.[bin.days_cycle_all_wk] = T2.[BicycleFreq_B01ID]  ) "

           #    APS variables   <>  NTS variables

nts.aps  = sqldf(x =str_sql)

nts.aps = inner_join(indiv.MET[,],  aps[,], 
                          
                          by=c( "Age_B01ID" = "ageband" , 
                                "Sex_B01ID" = "male",
                                "HHoldGOR_B02ID" = "region", 
                                "EthGroupTS_B02ID" = "nonwhite" ,
                                "bin.WalkTime.h"  = "bin.dur_walk10_utility_wk",
                                "BicycleFreq_B01ID"  = "bin.days_cycle_all_wk"  )  )                               


sum(!indiv.MET$IndividualID %in% nts.aps$IndividualID)
## match results: 137,240 matched | 153 unmatched  

# rest: 4 vars
indiv.MET1 = indiv.MET [! indiv.MET$IndividualID %in% nts.aps$IndividualID,  ]
str_sql1  = "SELECT T1.id, T1.weightla_truepop, T1.mets_sport_wk,
            (T1.dur_walk10_healthrec_wk/2) AS walkAPS, T1.dur_cycle_rec_wk AS cycleAPS,

            T2.IndividualID, T2.Age_B01ID, T2.Sex_B01ID, T2.HHoldGOR_B02ID,
            T2.EthGroupTS_B02ID, T2.WalkTime, T2.CycleTime

            FROM [aps] AS T1 INNER JOIN [indiv.MET1] AS T2

           ON (T1.ageband = T2.Age_B01ID)
           AND (T1.male  = T2.Sex_B01ID)
           AND (T1.region = T2.HHoldGOR_B02ID)
           AND (T1.[bin.dur_walk10_utility_wk] =  T2.[bin.WalkTime.h]  ) "

#    APS variables   <>  NTS variables

nts.aps1  = sqldf(x = str_sql1)

nts.aps1 = inner_join(indiv.MET1[,], aps[, ], 
             by=c("Age_B01ID"  = "ageband" ,
                  "Sex_B01ID"  = "male",
                  "HHoldGOR_B02ID" = "region",
                  "bin.WalkTime.h" = "bin.dur_walk10_utility_wk")         )


sum(!indiv.MET1$IndividualID %in% nts.aps1$IndividualID)  # if =0 => ALL MATCHED !!

#####################

#samples 1 individual per match, probabilistic extraction
nts.aps <- setDT(nts.aps)[,if(.N<1) .SD 
                          else .SD[sample(.N,1, replace=F, prob = weightla_truepop)], by=IndividualID]

nts.aps1 <- setDT(nts.aps1)[,if(.N<1) .SD 
                          else .SD[sample(.N,1, replace=F, prob = weightla_truepop)], by=IndividualID]

nts.aps= rbind(nts.aps, nts.aps1)
nts.aps = as.data.frame(nts.aps)   #convert to DF, otherwise problems in ICT

saveRDS(object = nts.aps, file.path(datapath, 'nts.aps.Rds'))



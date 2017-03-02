
#prepare for SQL processing
library(sqldf)
library(dplyr)
library(data.table)

####################### STARTS BUILD PROCESS OF BASELINE ###############################

# make files thinner for inner_join 
trip2014 <- trip2014[, c('SurveyYear', 'TripID', 'DayID', 'IndividualID', 'HouseholdID', 
                           'PSUID', 'W5', 'W5xHH', 'TravDay', 'SeriesCall_B01ID', 'ShortWalkTrip_B01ID', 
                           'NumStages', 'MainMode_B03ID', 'MainMode_B04ID', 'MainMode_B11ID',
                           'TripPurpose_B01ID', 'TripPurpose_B02ID', 
                           'TripTotalTime', 'TripTravTime', 'TripDisIncSW', 'TripDisExSW',
                           'JJXSC', 'JOTXSC', 'JTTXSC', 'JD')]

ind2014 <- ind2014[   ,  c('IndividualID', 'Age_B01ID',  'Sex_B01ID', 'NSSec_B03ID',
                           'CarAccess_B01ID', 'BicycleFreq_B01ID', 'WalkFreq_B01ID',
                           'Cycle12_B01ID', 'NSSec_B03ID', 'IndIncome2002_B02ID',
                           'EthGroupTS_B02ID' )]

household2014  <- household2014[ ,c('HouseholdID', 'HHoldGOR_B02ID')]


# filter days/stages
stage2014 <- stage2014[ ,c('SurveyYear', 'StageID', 'TripID', 'DayID', 
                           'IndividualID', 'HouseholdID', 'PSUID', 'VehicleID', 'StageShortWalk_B01ID',
                           'IndTicketID', 'PersNo', 'TravDay', 'JourSeq', 'StageMode_B03ID',
                           'StageMode_B04ID','StageMode_B11ID','StageSeq', 'StageDistance',
                           'StageDistance_B01ID', 'StageTime', 
                           'StageTime_B01ID', 'StageMain_B01ID', 
                            'W5', 'W5xHH','SSXSC', 'STTXSC', 'SD')]

# T2.W5, T2.W5xHH, T2.SSXSC, T2.STTXSC, T2.SD,    additional fields for extra precision


#### Create INITIAL BASELINE  = no mMETs , filtered to >=18 y.o., English regions 1..9)

str_sql <- 'SELECT T2.Age_B01ID, T2.Sex_B01ID, T2.CarAccess_B01ID, T2.NSSec_B03ID, 
T2.IndIncome2002_B02ID, T2.EthGroupTS_B02ID, 
T1.SurveyYear, T1.TripID, T1.DayID, T1.IndividualID, T1.HouseholdID, 
T1.PSUID, T1.W5, T1.W5xHH, T1.TravDay, T1.SeriesCall_B01ID, T1.ShortWalkTrip_B01ID, 
T1.NumStages, T1.MainMode_B03ID, T1.MainMode_B04ID, T1.MainMode_B11ID, T1.TripTotalTime, 
T1.TripTravTime, T1.TripDisIncSW, T1.TripDisExSW, T1.JJXSC, T1.JOTXSC, T1.JTTXSC, 
T1.JD, 0 AS Pcyc, 0 AS now_cycle, T3.HHoldGOR_B02ID 

FROM (trip2014 as T1 INNER JOIN ind2014 as T2 ON T1.IndividualID  = T2.IndividualID) 
INNER JOIN household2014 as T3 ON T1.HouseholdID  = T3.HouseholdID  

WHERE (((T2.Age_B01ID)>=8) AND ((T3.HHoldGOR_B02ID)<10))
ORDER BY T3.HHoldGOR_B02ID '


bl2014 <-sqldf(x =str_sql)
names(bl2014)

#add extra variables for scenarios build
bl2014$Age  = bl2014$Sex  =NA
bl2014$Age[bl2014$Age_B01ID< 16] <- '16.59'
bl2014$Age[bl2014$Age_B01ID>= 16] <- '60plus'
bl2014$Sex[bl2014$Sex_B01ID== 1] <- 'Male'
bl2014$Sex[bl2014$Sex_B01ID== 2] <- 'Female'
bl2014$agesex <- paste0(bl2014$Age, bl2014$Sex)


########        CALCULATE TOTAL P.A. = NTS  + APS recreational  (WALKING + CYCLING)

## 1: NTS WALKING:  stages time/distance, as per agreed rules 

str_sql <- 'SELECT T1.TripID, T1.TripPurpose_B01ID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID  = T2.TripID) 

WHERE   (T1.TripPurpose_B01ID<>17 AND T2.StageMode_B04ID= 1  AND T2.StageTime > 10 )

ORDER BY T1.TripID '

walktrips <- sqldf(x= str_sql)


## 2: NTS CYCLING:  stages time/distance 

str_sql <- 'SELECT T1.TripID, T2.StageID, T2.IndividualID, T2.StageDistance, 
T2.StageTime, T2.StageTime_B01ID, T2.StageMode_B03ID, T2.StageMode_B04ID, 
T2.StageShortWalk_B01ID, T2.SD, T2.STTXSC

FROM (trip2014 AS T1 INNER JOIN stage2014 AS T2 ON T1.TripID  = T2.TripID) 

WHERE   T2.StageMode_B04ID  =2 

ORDER BY T1.TripID '

cycletrips <- sqldf(x=str_sql)

###### calculate NTS TIMES/DISTANCES per trip (NTS)
str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumofWStageDistance, 
Sum(T1.StageTime) AS SumOfWStageTime

FROM walktrips AS T1 GROUP BY T1.TripID   '

walktrips1 <- sqldf(x= str_sql)    # walked trips

str_sql <- 'SELECT T1.TripID, Sum(T1.StageDistance) AS SumofCStageDistance, 
Sum(T1.StageTime) AS SumOfCStageTime

FROM cycletrips AS T1 GROUP BY T1.TripID   ' 

cycletrips1 <- sqldf(x= str_sql)    # cycled trips

rm(walktrips, cycletrips)

#####################  COMBINE: add W/C stages METs times to bl trips (NTS)

bl2014  = left_join(bl2014, walktrips1, by ="TripID")
bl2014  = left_join(bl2014, cycletrips1, by ="TripID")
rm(walktrips1, cycletrips1)

bl2014$SumofWStageDistance[is.na(bl2014$SumofWStageDistance)] = 0
bl2014$SumOfWStageTime[is.na(bl2014$SumOfWStageTime)] = 0

bl2014$SumofCStageDistance[is.na(bl2014$SumofCStageDistance) ]  = 0
bl2014$SumOfCStageTime[is.na(bl2014$SumOfCStageTime) ]  = 0


################## MATCHING BASELINE <>  A.P.S.

datapath = './data/'
aps  = readRDS(paste0(datapath,'aps_proc.Rds')) #latest v3 file from Anna
aps$region = as.character(aps$region)

#recode region-sex-age bands - walk duration
aps$region  = recode(.x  = aps$region,  "East" = 6, "East Midlands" = 4, "London" = 7, 
                    "North East" = 1, "North West" =2, "South East" = 8, "South West" = 9,
                    "West Midlands" = 5, "Yorkshire" = 3)


# categs. as in NTS: 1 =male, 2 =female
aps$male  = recode(aps$male, '1' =1, '0' =2)

#NTS baseline criteria: 18y.o or older
aps  = aps[aps$age>17, ]

aps$ageband <- cut(aps$age, breaks  = c(18:20, 21, 26, 30, 40, 50, 60, 65, 70, 75, 80, 85, Inf),
labels  = c(8:21), right  = F)

#recode duration of walking for 10+ min bouts
aps$dur_walk10_utility_wk[is.na(aps$dur_walk10_utility_wk)] = 0
aps$bin.dur_walk10_utility_wk  = cut(aps$dur_walk10_utility_wk, breaks  = c(0, 1, 3, 6, Inf), 
                                labels =c(0:3), right = F)

#binary variable for total cycling
aps$bin.days_cycle_all_wk  = 0
sel = (aps$days_cycle_all_wk > 0.75) 
aps$bin.days_cycle_all_wk [ sel ]  = 1

# impute cycling in ind2014
ind2014$BicycleFreq_B01ID = recode(ind2014$BicycleFreq_B01ID, '-10'= 0, '-9'= 0, '-8'= 0,
                                   '1'= 1, '2'=1, '3'= 0, '4'= 0, '5'= 0, '6'= 0, '7'= 0)

sel = ( ind2014$Cycle12_B01ID== 2 | ind2014$Cycle12_B01ID== 3)
ind2014$BicycleFreq_B01ID[ sel ] = 0


### MATCHING APS <> baseline individuals 
### initially on 6 [7] variables: age-sex-region-ethnicity-walking-cycling - [SES]

# group bl2014
str_sql='select IndividualID, HouseholdID, sum(SumOfWStageTime) as WalkTime,
                              sum(SumOfCStageTime) as CycleTime
                             from bl2014 GROUP BY individualID '

indiv.MET = sqldf(str_sql)      # 135K people w trips, 42,441 w/o
indiv.MET$WalkTime.h = round(indiv.MET$WalkTime/60, digits = 1)
indiv.MET$CycleTime.h = round(indiv.MET$CycleTime/60, digits = 1)

indiv.MET$bin.WalkTime.h = cut(x = indiv.MET$WalkTime.h, breaks  = c(0, 1, 3, 6, Inf),
                               labels = c(0:3), right = F)

#add region & rest of vars for matching
indiv.MET = inner_join(indiv.MET, household2014, by="HouseholdID")
indiv.MET  = inner_join(indiv.MET, ind2014, by ="IndividualID")

#match 6 vars
str_sql  = "SELECT T1.id, T1.weightla_truepop, T1.mets_sport_wk,
            (T1.dur_walk10_healthrec_wk/2) AS walkAPS, T1.dur_cycle_rec_wk AS cycleAPS,
            T2.IndividualID, T2.WalkTime, T2.CycleTime

           FROM [aps.sel] AS T1 INNER JOIN [indiv.MET] AS T2

           ON (T1.ageband = T2.Age_B01ID) 
           AND (T1.male  = T2.Sex_B01ID) 
           AND (T1.region = T2.HHoldGOR_B02ID) 
           AND (T1.nonwhite = T2.EthGroupTS_B02ID)  
           AND (T1.[bin.dur_walk10_utility_wk] =  T2.[bin.WalkTime.h]  ) 
           AND (T1.[bin.days_cycle_all_wk] = T2.[BicycleFreq_B01ID]  ) "

                #    APS variables   <>  NTS variables

nts.aps.match  = sqldf(x =str_sql)
sum(!indiv.MET$IndividualID %in% nts.aps.match$IndividualID)
## individuals with/w.o match: 120,738 matched, 14,262 unmatched  

# rest: match 4 vars
indiv.MET1 = indiv.MET [! indiv.MET$IndividualID %in% nts.aps.match$IndividualID,  ]
str_sql1  = "SELECT T1.id, T1.weightla_truepop, T1.mets_sport_wk,
            (T1.dur_walk10_healthrec_wk/2) AS walkAPS, T1.dur_cycle_rec_wk AS cycleAPS,
            T2.IndividualID, T2.WalkTime, T2.CycleTime 

           FROM [aps.sel] AS T1 INNER JOIN [indiv.MET1] AS T2

           ON (T1.ageband = T2.Age_B01ID) 
           AND (T1.male  = T2.Sex_B01ID) 
           AND (T1.region = T2.HHoldGOR_B02ID) 

           AND (T1.[bin.dur_walk10_utility_wk] =  T2.[bin.WalkTime.h]  ) "

#    APS variables   <>  NTS variables

nts.aps.match1  = sqldf(x =str_sql1)
sum(!indiv.MET1$IndividualID %in% nts.aps.match1$IndividualID)  # =0 => ALL MATCHED !!

#####################

nts.aps = rbind(nts.aps.match, nts.aps.match1) ; rm(nts.aps.match, nts.aps.match1)

#samples 1 individual per match, probabilistic extraction
nts.aps <- setDT(nts.aps)[,if(.N<1) .SD 
                          else .SD[sample(.N,1,replace=F, prob = weightla_truepop)], by=IndividualID]


saveRDS(object = nts.aps, file.path(datapath, 'nts.aps.Rds'))




####################### REQUIREMENTS ###############################
#  a 'data' folder with all the sources from the specific Travel Survey
#  in .tab format (for .csv,  .Rds,  .xls.....  replace reading commands)
# Scripts subsets to years 2004-2014 *ONLY* when using the latest release NTS2014

#rm(list=ls())

# define data path
datapath ='data'   #V:/Studies/MOVED/HealthImpact/Data/National_Travel_Survey_2014/2014/tab/'

ficheros <-  dir(path = datapath,  pattern = '.tab')

#reads all key NTS files with data 2004-2014

#days the trips were made
day2014 <- read.table(file.path(datapath,  "day.tab"),  sep = "\t",  header=T)
colnames(day2014)
dim(day2014)
day2014 <- subset(day2014, subset= SurveyYear>=2004)  #include years 2004-2014
#saveRDS(day2014,  file.path(datapath,  "./Rds.format/day2014.Rds") )

#individuals making the trips
ind2014<- read.table(file.path(datapath,  "individual.tab"),  sep = "\t",  header=T)
colnames(ind2014)
dim(ind2014)
ind2014<- subset(ind2014, SurveyYear>=2004)
#saveRDS(ind2014, file.path(datapath,  "./Rds.format/ind2014.Rds") )

#stages per trip
stage2014<- read.table(file.path(datapath, "stage.tab"),  sep = "\t",  header=T) 
colnames(stage2014)
dim(stage2014)
stage2014<- subset(stage2014, SurveyYear>=2004)
#saveRDS(stage2014, file.path(datapath,  "./Rds.format/stage2014.Rds") )


# households the individuals belong to
household2014 <- read.table(file.path(datapath, "household.tab"),  sep = "\t",  header=T,  as.is = T)
household2014 <- subset(household2014, SurveyYear>=2004)
#saveRDS(household2014, file.path(datapath,  "./Rds.format/household2014.Rds"))

# trips (this will make the core of the baseline)
trip2014 <- read.table(file.path(datapath, "trip.tab"),  header = T,   sep = "\t", colClasses=c(rep("numeric", 63)))
colnames(trip2014)
dim(trip2014)
trip2014<- subset(trip2014, SurveyYear>=2004)
#saveRDS(trip2014, file="./Rds.format/trip2014.Rds")

#vehicle
vehicle2014 <- read.table(file.path(datapath, "vehicle.tab"),  header=T, sep="\t")
vehicle2014 <-  subset(vehicle2014, SurveyYear>=2004)
#saveRDS(vehicle2014, file='./Rds.format/vehicle2014.Rds')

#########################    FILTERING PROCESS: 

#subset to individuals >18y.o
ind2014<- subset(ind2014, Age_B01ID>=8)

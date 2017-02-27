
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
day2014 <- read.table(file.path(datapath,  "day.tab"),  sep = "\t",  header = T)
colnames(day2014)
dim(day2014)
day2014 <- subset(day2014, subset = SurveyYear>=2004)  #include years 2004-2014


#individuals making the trips
ind2014<- read.table(file.path(datapath,  "individual.tab"),  sep = "\t",  header = T)
ind2014<- subset(ind2014, SurveyYear >= 2004)

#stages per trip
stage2014<- read.table(file.path(datapath, "stage.tab"),  sep = "\t",  header = T) 
stage2014<- subset(stage2014, SurveyYear >= 2004)


# households the individuals belong to
household2014 <- read.table(file.path(datapath, "household.tab"),  sep = "\t",  header = T,  as.is = T)
household2014 <- subset(household2014, SurveyYear >= 2004)

# trips (this will make the core of the baseline)
trip2014 <- read.table(file.path(datapath, "trip.tab"),  header = T,   sep = "\t", colClasses=c(rep("numeric", 63)))
trip2014<- subset(trip2014, SurveyYear >= 2004)

#vehicle
# vehicle2014 <- read.table(file.path(datapath, "vehicle.tab"),  header = T, sep = "\t")
# vehicle2014 <-  subset(vehicle2014, SurveyYear >= 2004)


#########################    FILTERING PROCESS: 

#subset to individuals >18y.o
ind2014<- subset(ind2014, Age_B01ID >= 8)

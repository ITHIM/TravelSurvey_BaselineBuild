
####################### REQUIREMENTS ###############################
#  a 'data' folder with all the sources from the specific Travel Survey
#  in .tab format (for .csv,  .Rds,  .xls.....  replace reading commands)
# Script subsets to years 2004-2014 *ONLY* when using the latest release NTS2014

#rm(list=ls())

# define data path
# Read data from v drive
# datapath ='V:/Studies/MOVED/HealthImpact/Data/National_Travel_Survey_2014/2014/tab/'

# Read from local dir
datapath ='data/National_Travel_Survey_2014/2014/tab/'

#see all files
ficheros <-  dir(path = datapath,  pattern = '.tab')

#reads all key NTS files with data 2004-2014

#days the trips were made
day2014 <- read.table(file.path(datapath,  "day.tab"),  sep = "\t",  header = T)
day2014 <- subset(day2014, subset = SurveyYear>=2004)  #include years 2004-2014


#individuals making the trips
ind2014<- read.table(file.path(datapath,  "individual.tab"),  sep = "\t",  header = T)

#18-84 y.o. + with travel diary
ind2014<- subset(ind2014, subset = Age_B01ID >= 8 & Age_B01ID < 21 & SurveyYear>=2004 & W1==1 )  

#stages per trip
stage2014<- read.table(file.path(datapath, "stage.tab"),  sep = "\t",  header = T) 
stage2014<- subset(stage2014, SurveyYear >= 2004)


# households the individuals belong to
household2014 <- read.table(file.path(datapath, "household.tab"),  sep = "\t",  header = T,  as.is = T)
household2014 <- subset(household2014, SurveyYear >= 2004)

 



# Setup -------------------------------------------------------------------
library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)
library(arrow)
library(lubridate)

synLogin()

setwd('./parquet/')
system('synapse get -r syn51406699')
subfolders <- list.dirs(recursive = FALSE)
for (subfolder in subfolders) {
  file_path <- file.path(subfolder, "SYNAPSE_METADATA_MANIFEST.tsv")
  
  # Check if the file exists in the subfolder
  if (file.exists(file_path)) {
    file.remove(file_path)
    cat("File removed:", file_path, "\n")
  } else {
    cat("File not found in:", subfolder, "\n")
  }
}
setwd('~/recover-pipeline-qc/')

# files <- synapserutils::syncFromSynapse('syn51406699')

fitbitdailydata <- 
  open_dataset('./parquet/dataset_fitbitdailydata/') %>% 
  collect() %>% 
  mutate(HeartRateIntradayMinuteCount = as.numeric(HeartRateIntradayMinuteCount),
         Steps = as.numeric(Steps))

enrolledparticipants <- 
  open_dataset('./parquet/dataset_enrolledparticipants/') %>% 
  collect()

# Fitbit HeartRate HeartRateIntradayMinuteCount ---------------------------
num_records_total <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  nrow()

num_records_complete <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  drop_na() %>% 
  nrow()

num_records_complete_nonzero <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  filter(HeartRateIntradayMinuteCount!=0) %>% 
  drop_na() %>% 
  nrow()

num_participants_with_records <- 
  fitbitdailydata$ParticipantIdentifier[
    !is.na(fitbitdailydata$HeartRateIntradayMinuteCount)
    ] %>% 
  unique() %>% 
  length()

num_participants_with_records_nonzero <- 
  fitbitdailydata$ParticipantIdentifier[
    !is.na(fitbitdailydata$HeartRateIntradayMinuteCount) & 
      fitbitdailydata$HeartRateIntradayMinuteCount!=0
    ] %>% 
  unique() %>% 
  length()

days_present_nonzero_per_participant <-
  fitbitdailydata %>% 
  select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount) %>% 
  mutate(Date = as_date(Date)) %>% 
  drop_na() %>% 
  filter(HeartRateIntradayMinuteCount!=0) %>% 
  group_by(ParticipantIdentifier) %>% 
  distinct(Date, .keep_all = T) %>% 
  summarise(days_present_nonzero = n()) %>% 
  ungroup()

avg_days_present_nonzero <- mean(days_present_nonzero_per_participant$days_present_nonzero)

kd <- density(days_present_nonzero_per_participant$days_present_nonzero, bw = "SJ")
plot(kd, main = "Number of Days of Data per Participant")
polygon(kd, col='lightblue', border='black')

wear_time_enrollment_df <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount), 
             y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(HeartRateIntradayMinuteCount!=0)

avg_wear_time_since_enrollment_per_participant <- 
  wear_time_enrollment_df %>% 
  group_by(ParticipantIdentifier) %>% 
  filter(Date>=EnrollmentDate) %>% 
  summarise(average_wear_time = mean(HeartRateIntradayMinuteCount)) %>% 
  mutate(average_wear_time_percent = average_wear_time/1439)

kd2 <- density(avg_wear_time_since_enrollment_per_participant$average_wear_time_percent, bw = "SJ")
plot(kd2, main = "Adherence from Average Wear Time")
polygon(kd2, col='lightblue', border='black')

insights <- 
  data.frame(
    "device" = "fitbit",
    "category" = "heartrate",
    "measure" = "HeartRateIntradayMinuteCount",
    "n_records" = num_records_total,
    "n_complete_records" = num_records_complete,
    "n_complete_nonzero_records" = num_records_complete_nonzero,
    "n_participants_with_complete_records" = num_participants_with_records,
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero,
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero)

# Fitbit Activity tracker_steps -------------------------------------------



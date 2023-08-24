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

# Functions ---------------------------------------------------------------
num_records_total <- function(df, measure) {
  df %>% 
    select(all_of(measure)) %>% 
    nrow()
}

num_records_complete <- function(df, measure) {
  df %>% 
    select(all_of(measure)) %>% 
    drop_na() %>% 
    nrow()
}

num_records_complete_nonzero <- function(df, measure) {
  df %>%
    select(all_of(measure)) %>%
    filter(.[[measure]]!=0) %>%
    drop_na() %>%
    nrow()
}

num_participants_with_records <- function(df, measure) {
  df$ParticipantIdentifier[
    !is.na(df[[measure]])
  ] %>%
    unique() %>%
    length()
}

num_participants_with_records_nonzero <- function(df, measure) {
  df$ParticipantIdentifier[
    !is.na(df[[measure]]) &
      df[[measure]]!=0
  ] %>%
    unique() %>%
    length()
}

days_present_nonzero_per_participant <- function(df, measure) {
  fitbitdailydata %>% 
    select(ParticipantIdentifier, Date, measure) %>% 
    mutate(Date = as_date(Date)) %>% 
    drop_na() %>% 
    filter(.[[measure]]!=0) %>% 
    group_by(ParticipantIdentifier) %>% 
    distinct(Date, .keep_all = T) %>% 
    summarise(days_present_nonzero = n()) %>% 
    ungroup()
}

avg_days_present_nonzero <- function(df, col) {
  mean(df[[col]])
}

# Fitbit HeartRate HeartRateIntradayMinuteCount ---------------------------
days_present_nonzero_per_participant_hr_df <- days_present_nonzero_per_participant(fitbitdailydata, "HeartRateIntradayMinuteCount")

kd <- density(days_present_nonzero_per_participant_hr_df$days_present_nonzero, bw = "SJ")
plot(kd, main = "Number of Days of Data per Participant")
polygon(kd, col='lightblue', border='black')

wear_time_enrollment_df_hr <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount), 
             y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(HeartRateIntradayMinuteCount!=0)

avg_wear_time_since_enrollment_per_participant <- 
  wear_time_enrollment_df_hr %>% 
  group_by(ParticipantIdentifier) %>% 
  filter(Date>=EnrollmentDate) %>% 
  summarise(average_wear_time = mean(HeartRateIntradayMinuteCount)) %>% 
  mutate(average_wear_time_percent = average_wear_time/1439)

kd2 <- density(avg_wear_time_since_enrollment_per_participant$average_wear_time_percent, bw = "SJ")
plot(kd2, main = "Average Wear Time")
polygon(kd2, col='lightblue', border='black')

insights <- 
  data.frame(
    "device" = "fitbit",
    "category" = "heartrate",
    "measure" = "HeartRateIntradayMinuteCount",
    "n_records" = num_records_total(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_complete_records" = num_records_complete(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant_hr_df, "days_present_nonzero")
    )

# Fitbit Activity steps -------------------------------------------
days_present_nonzero_per_participant_steps_df <- days_present_nonzero_per_participant(fitbitdailydata, "Steps")

kd3 <- density(days_present_nonzero_per_participant_steps_df$days_present_nonzero, bw = "SJ")
plot(kd3, main = "Number of Days of Data per Participant")
polygon(kd3, col='lightblue', border='black')

days_enrollment_df_steps <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, Steps), 
        y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(Steps!=0)

# avg_proportion_days_since_enrollment_per_participant <- 
#   days_enrollment_df_steps %>% 
#   group_by(ParticipantIdentifier) %>% 
#   filter(Date>=EnrollmentDate) %>% 
#   summarise(average_proportion = mean()) %>% 
#   mutate(average_wear_time_percent = average_wear_time/1439)

insights <- 
  bind_rows(
    insights,
    data.frame(
      "device" = "fitbit",
      "category" = "activity",
      "measure" = "steps",
      "n_records" = num_records_total(fitbitdailydata, "Steps"),
      "n_complete_records" = num_records_complete(fitbitdailydata, "Steps"),
      "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "Steps"),
      "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "Steps"),
      "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "Steps"),
      "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant(fitbitdailydata, "Steps"), "days_present_nonzero")
    )
  )

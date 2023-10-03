# Setup -------------------------------------------------------------------
library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)
library(arrow)
library(lubridate)

synLogin()

# token <- synapser::synGetStsStorageToken(
#   entity = 'syn51406699',
#   permission = "read_only",
#   output_format = "json")
# 
# PARQUET_BUCKET <- 'recover-processed-data'
# PARQUET_BUCKET_BASE_KEY <- 'main/parquet/'
# 
# if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
#   base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
# } else {
#   base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
# }
# 
# # configure the environment with AWS token
# Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
#            'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
#            'AWS_SESSION_TOKEN'=token$sessionToken)
# 
# AWS_PARQUET_DOWNLOAD_LOCATION <- '~/recover-pipeline-qc/parquet/'
# unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
# sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
# system(sync_cmd)

fitbitdailydata <- 
  open_dataset('./parquet/dataset_fitbitdailydata/') %>% 
  collect() %>% 
  mutate(HeartRateIntradayMinuteCount = as.numeric(HeartRateIntradayMinuteCount),
         Steps = as.numeric(Steps),
         SpO2_Avg = as.numeric(SpO2_Avg),
         Hrv_DailyRmssd = as.numeric(Hrv_DailyRmssd)) %>% 
  rename(spo2_avg = SpO2_Avg,
         hrv = Hrv_DailyRmssd)

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

days_present_nonzero_per_participant <- function(df, measure, enrollment_df) {
  df %>% 
    select(ParticipantIdentifier, Date, measure) %>% 
    mutate(Date = as_date(Date)) %>% 
    drop_na() %>% 
    filter(.[[measure]]!=0) %>% 
    group_by(ParticipantIdentifier) %>% 
    distinct(Date, .keep_all = T) %>% 
    # merge(y = (enrollment_df %>% select(ParticipantIdentifier, EnrollmentDate))) %>% 
    # mutate(EnrollmentDate=as_date(EnrollmentDate)) %>% 
    # filter(Date>=EnrollmentDate) %>% 
    # group_by(ParticipantIdentifier) %>% 
    summarise(days_present_nonzero = n()) %>% 
    ungroup()
}

avg_days_present_nonzero <- function(df, col) {
  mean(df[[col]])
}

# Fitbit HeartRate HeartRateIntradayMinuteCount ---------------------------
days_present_nonzero_per_participant_hr_df <- 
  days_present_nonzero_per_participant(fitbitdailydata, "HeartRateIntradayMinuteCount")

kd <- density(days_present_nonzero_per_participant_hr_df$days_present_nonzero, bw = "SJ")
plot(kd, main = "Number of Days of Data per Participant", sub = "HeartRate: HeartRateIntradayMinuteCount")
polygon(kd, col='lightblue', border='black')

wear_time_enrollment_df_hr <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount), 
             y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na()

avg_nonzero_wear_time_since_enrollment_per_participant <- 
  wear_time_enrollment_df_hr %>% 
  filter(HeartRateIntradayMinuteCount!=0) %>% 
  group_by(ParticipantIdentifier) %>% 
  filter(Date>=EnrollmentDate) %>% 
  summarise(average_wear_time = mean(HeartRateIntradayMinuteCount)) %>% 
  mutate(average_wear_time_percent = average_wear_time/1439)

avg_total_wear_time_since_enrollment_per_participant <- 
  wear_time_enrollment_df_hr %>% 
  group_by(ParticipantIdentifier) %>% 
  filter(Date>=EnrollmentDate) %>% 
  summarise(average_wear_time = mean(HeartRateIntradayMinuteCount)) %>% 
  mutate(average_wear_time_percent = average_wear_time/1439)

kd2_1 <- density(avg_nonzero_wear_time_since_enrollment_per_participant$average_wear_time_percent, bw = "SJ")
plot(kd2_1, main = "Average Non-Zero Wear Time Since Enrollment", sub = "HeartRate: HeartRateIntradayMinuteCount")
polygon(kd2_1, col='lightblue', border='black')

kd2_2 <- density(avg_total_wear_time_since_enrollment_per_participant$average_wear_time_percent, bw = "SJ")
plot(kd2_2, main = "Average Total Wear Time Since Enrollment", sub = "HeartRate: HeartRateIntradayMinuteCount")
polygon(kd2_2, col='lightblue', border='black')

days_enrollment_df_hr <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount), 
        y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(HeartRateIntradayMinuteCount!=0)

proportion_days_since_enrollment_per_participant_hr <-
  days_enrollment_df_hr %>%
  group_by(ParticipantIdentifier) %>%
  filter(Date>=EnrollmentDate) %>%
  summarise(n_days = n(),
            dt = as.numeric(max(Date)-min(EnrollmentDate)+1)) %>% 
  mutate(adherence_proportion = n_days/dt)

insights_hr <- 
  data.frame(
    "device" = "fitbit",
    "category" = "heartrate",
    "measure" = "HeartRateIntradayMinuteCount",
    "n_records" = num_records_total(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_complete_records" = num_records_complete(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "HeartRateIntradayMinuteCount"),
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant_hr_df, "days_present_nonzero"),
    "avg_nonzero_wear_time_since_enrollment_all_participants" = mean(avg_nonzero_wear_time_since_enrollment_per_participant$average_wear_time),
    "avg_nonzero_wear_time_proportion_since_enrollment_all_participants" = mean(avg_nonzero_wear_time_since_enrollment_per_participant$average_wear_time_percent),
    "avg_total_wear_time_since_enrollment_per_participant" = mean(avg_total_wear_time_since_enrollment_per_participant$average_wear_time),
    "avg_total_wear_time_proportion_since_enrollment_all_participants" = mean(avg_total_wear_time_since_enrollment_per_participant$average_wear_time_percent),
    "avg_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_hr$n_days),
    "avg_proportion_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_hr$adherence_proportion)
    )

# Fitbit Activity steps -------------------------------------------
days_present_nonzero_per_participant_steps_df <- days_present_nonzero_per_participant(fitbitdailydata, "Steps")

kd3 <- density(days_present_nonzero_per_participant_steps_df$days_present_nonzero, bw = "SJ")
plot(kd3, main = "Number of Days of Data per Participant", sub = "Activity: Steps")
polygon(kd3, col='lightblue', border='black')

days_enrollment_df_steps <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, Steps), 
        y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(Steps!=0)

proportion_days_since_enrollment_per_participant_steps <-
  days_enrollment_df_steps %>%
  group_by(ParticipantIdentifier) %>%
  filter(Date>=EnrollmentDate) %>%
  summarise(n_days = n(),
            dt = as.numeric(max(Date)-min(EnrollmentDate)+1)) %>% 
  mutate(adherence_proportion = n_days/dt)

kd4 <- density(proportion_days_since_enrollment_per_participant_steps$adherence_proportion, bw = "ucv")
plot(kd4, main = "Proportion of Number of Days of Data Since Enrollment per Participant", sub = "Activity: Steps")
polygon(kd4, col='lightblue', border='black')

insights_steps <- 
  data.frame(
    "device" = "fitbit",
    "category" = "activity",
    "measure" = "steps",
    "n_records" = num_records_total(fitbitdailydata, "Steps"),
    "n_complete_records" = num_records_complete(fitbitdailydata, "Steps"),
    "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "Steps"),
    "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "Steps"),
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "Steps"),
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant(fitbitdailydata, "Steps"), "days_present_nonzero"),
    "avg_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_steps$n_days),
    "avg_proportion_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_steps$adherence_proportion)
  )

# Fitbit spo2 spo2 --------------------------------------------------------
days_present_nonzero_per_participant_spo2_df <- days_present_nonzero_per_participant(fitbitdailydata, "spo2_avg")

kd5 <- density(days_present_nonzero_per_participant_spo2_df$days_present_nonzero, bw = "SJ")
plot(kd5, main = "Number of Days of Data per Participant", sub = "spo2")
polygon(kd5, col='lightblue', border='black')

days_enrollment_df_spo2 <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, spo2_avg), 
        y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(spo2_avg!=0)

proportion_days_since_enrollment_per_participant_spo2 <-
  days_enrollment_df_spo2 %>%
  group_by(ParticipantIdentifier) %>%
  filter(Date>=EnrollmentDate) %>%
  summarise(n_days = n(),
            dt = as.numeric(max(Date)-min(EnrollmentDate)+1)) %>% 
  mutate(adherence_proportion = n_days/dt)

kd6 <- density(proportion_days_since_enrollment_per_participant_spo2$adherence_proportion, bw = "ucv")
plot(kd6, main = "Proportion of Number of Days of Data Since Enrollment per Participant", sub = "spo2")
polygon(kd6, col='lightblue', border='black')

insights_spo2 <- 
  data.frame(
    "device" = "fitbit",
    "category" = "spo2",
    "measure" = "spo2",
    "n_records" = num_records_total(fitbitdailydata, "spo2_avg"),
    "n_complete_records" = num_records_complete(fitbitdailydata, "spo2_avg"),
    "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "spo2_avg"),
    "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "spo2_avg"),
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "spo2_avg"),
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant(fitbitdailydata, "spo2_avg"), "days_present_nonzero"),
    "avg_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_spo2$n_days),
    "avg_proportion_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_spo2$adherence_proportion)
  )

# Fitbit hrv hrv ----------------------------------------------------------
days_present_nonzero_per_participant_hrv_df <- days_present_nonzero_per_participant(fitbitdailydata, "hrv")

kd7 <- density(days_present_nonzero_per_participant_hrv_df$days_present_nonzero, bw = "SJ")
plot(kd7, main = "Number of Days of Data per Participant", sub = "hrv")
polygon(kd7, col='lightblue', border='black')

days_enrollment_df_hrv <- 
  merge(x = fitbitdailydata %>% select(ParticipantIdentifier, Date, hrv), 
        y = enrolledparticipants %>% select(ParticipantIdentifier, EnrollmentDate)) %>% 
  mutate(Date = as_date(Date),
         EnrollmentDate = as_date(EnrollmentDate)) %>% 
  drop_na() %>% 
  filter(hrv!=0)

proportion_days_since_enrollment_per_participant_hrv <-
  days_enrollment_df_hrv %>%
  group_by(ParticipantIdentifier) %>%
  filter(Date>=EnrollmentDate) %>%
  summarise(n_days = n(),
            dt = as.numeric(max(Date)-min(EnrollmentDate)+1)) %>% 
  mutate(adherence_proportion = n_days/dt)

kd8 <- density(proportion_days_since_enrollment_per_participant_hrv$adherence_proportion, bw = "ucv")
plot(kd8, main = "Proportion of Number of Days of Data Since Enrollment per Participant", sub = "hrv")
polygon(kd8, col='lightblue', border='black')

insights_hrv <- 
  data.frame(
    "device" = "fitbit",
    "category" = "hrv",
    "measure" = "hrv",
    "n_records" = num_records_total(fitbitdailydata, "hrv"),
    "n_complete_records" = num_records_complete(fitbitdailydata, "hrv"),
    "n_complete_nonzero_records" = num_records_complete_nonzero(fitbitdailydata, "hrv"),
    "n_participants_with_complete_records" = num_participants_with_records(fitbitdailydata, "hrv"),
    "n_participants_with_complete_nonzero_records" = num_participants_with_records_nonzero(fitbitdailydata, "hrv"),
    "avg_days_of_complete_nonzero_data" = avg_days_present_nonzero(days_present_nonzero_per_participant(fitbitdailydata, "hrv"), "days_present_nonzero"),
    "avg_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_hrv$n_days),
    "avg_proportion_n_days_since_enrollment_all_participants" = mean(proportion_days_since_enrollment_per_participant_hrv$adherence_proportion)
  )

# Insights df -------------------------------------------------------------
insights <- bind_rows(insights_hr, insights_steps, insights_spo2, insights_hrv)

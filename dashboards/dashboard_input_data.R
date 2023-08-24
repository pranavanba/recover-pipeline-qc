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

#### Fitbit HeartRate HeartRateIntradayMinuteCount ####
fitbitdailydata <- open_dataset('./parquet/dataset_fitbitdailydata/') %>% collect()
hr_insights <- list()

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

avg_days_present_nonzero_per_participant <-
  fitbitdailydata %>% 
  select(ParticipantIdentifier, Date, HeartRateIntradayMinuteCount) %>% 
  mutate(Date = as_date(Date)) %>% 
  drop_na() %>% 
  filter(HeartRateIntradayMinuteCount!=0) %>% 
  group_by(ParticipantIdentifier) %>% 
  distinct(Date, .keep_all = T) %>% 
  summarise(days_present_nonzero = n()) %>% 
  ungroup()

library(synapser)
library(synapserutils)
library(tidyr)
library(dplyr)
library(arrow)

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

#### Fitbit Heart Rate ####
fitbitdailydata <- open_dataset('./parquet/dataset_fitbitdailydata/') %>% collect()
hr_insights <- list()

num_records_total <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  nrow()

num_records_unique <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  unique() %>% 
  nrow()

num_records_unique_complete <- 
  fitbitdailydata %>% 
  select(HeartRateIntradayMinuteCount) %>% 
  unique() %>% 
  filter(HeartRateIntradayMinuteCount!=0) %>% 
  drop_na() %>% 
  nrow()

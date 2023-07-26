library(synapser)
library(jsonlite)
library(dplyr)

synLogin()

system('synapse get -r syn51406698')

data <- read.delim('./ce-input-data/SYNAPSE_METADATA_MANIFEST.tsv')

adult_i <- 1
pediatric_i <- 1

for (p in data$path) {
  if (grepl('adult', p)) {
    unzip(p, exdir = paste0('adult/', adult_i))
    adult_i <- adult_i + 1
  } else if (grepl('pediatric', p)) {
    unzip(p, exdir = paste0('pediatric/', pediatric_i))
    pediatric_i <- pediatric_i + 1
  }
}

get_unique_participant_ids <- function(folder_path) {
  unique_participant_ids <- character(0)
  
  enrolled_files <- list.files(folder_path, pattern = "EnrolledParticipants", full.names = TRUE)
  
  for (file_path in enrolled_files) {
    data <- stream_in(con = file(file_path), flatten = TRUE)
    unique_participant_ids <- union(unique_participant_ids, unique(data$ParticipantIdentifier))
  }
  
  return(unique_participant_ids)
}

get_num_rows_per_file <- function(folder_path, manifest_path) {
  manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
  files_with_num_rows <- list()
  
  json_files <- list.files(folder_path, pattern = "\\.json$", full.names = TRUE)
  
  for (file_path in json_files) {
    file_basename <- sub("_[^_]*$", "", basename(file_path))
    num_rows <- manifest[[file_basename]]
    files_with_num_rows[[file_basename]] <- num_rows
  }
  
  out <- 
    files_with_num_rows %>% 
    tibble::enframe() %>% 
    mutate(value=as.double(value)) %>% 
    rename(file=name, num_records=value)
  
  return(out)
}

get_unique_fitbit_devices <- function(folder_path) {
  unique_fitbit_devices <- character(0)
  
  fitbit_files <- list.files(folder_path, pattern = "FitbitDevices", full.names = TRUE)
  
  for (file_path in fitbit_files) {
    data <- stream_in(con = file(file_path), flatten = TRUE)
    unique_fitbit_devices <- unique(data$Device)
  }
  
  return(unique_fitbit_devices)
}

pids <- get_unique_participant_ids('./adult/1/')
nrecs <- get_num_rows_per_file('./adult/1/', './adult/1/Manifest.csv')
devices <- get_unique_fitbit_devices('./adult/1/')

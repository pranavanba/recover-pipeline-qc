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
    unzip(p, exdir = paste0('input_data_unzipped/adult/', adult_i))
    adult_i <- adult_i + 1
  } else if (grepl('pediatric', p)) {
    unzip(p, exdir = paste0('input_data_unzipped/pediatric/', pediatric_i))
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

get_num_rows_per_file <- function(folder_path) {
  manifest_path <- file.path(folder_path, "Manifest.csv")
  manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
  
  exclude_columns <- c("ExportStartDate", "ExportEndDate", "ExportConfiguration", "ExcludedParticipantIdentifiers")
  manifest_subset <- manifest[, !(names(manifest) %in% exclude_columns)]
  
  out <- 
    manifest_subset %>% 
    t() %>% 
    tibble::enframe() %>% 
    rename(file = name, num_records = value)
  
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
nrecs <- get_num_rows_per_file('./adult/2/')
devices <- get_unique_fitbit_devices('./adult/1/')

apply_function_to_subdirectories <- function(parent_folder_path, fun) {
  subdirectories <- list.dirs(parent_folder_path, recursive = FALSE, full.names = TRUE)
  results <- lapply(subdirectories, function(subdir) {
    result <- fun(subdir)
    return(result)
  })
  names(results) <- basename(subdirectories)
  return(results)
}

parent_folder_path <- "./adult/"

pids_adults <- apply_function_to_subdirectories(parent_folder_path, get_unique_participant_ids)
nrecs_adults <- apply_function_to_subdirectories(parent_folder_path, get_num_rows_per_file)
devices_adults <- apply_function_to_subdirectories(parent_folder_path, get_unique_fitbit_devices)

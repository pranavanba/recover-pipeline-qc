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

get_info_from_folder <- function(folder_path, manifest_path) {
  manifest <- read.csv(manifest_path, stringsAsFactors = FALSE)
  
  unique_participant_ids <- character(0)
  files_with_num_rows <- list()
  unique_fitbit_devices <- character(0)
  
  json_files <- list.files(folder_path, pattern = "\\.json$", full.names = TRUE)
  
  for (file_path in json_files) {
    data <- stream_in(con=file(file_path), flatten = TRUE)
    
    file_basename <- sub("_[^_]*$", "", basename(file_path))
    
    unique_participant_ids <- union(unique_participant_ids, unique(data$ParticipantIdentifier))
    
    num_rows <- manifest[[file_basename]]
    files_with_num_rows[[file_basename]] <- num_rows
    
    if (file_basename == 'FitbitDevices') {
      unique_fitbit_devices <- unique(data$Device)
    }
  }
  
  result <- list(
    unique_participant_ids = unique_participant_ids,
    files_with_num_rows = files_with_num_rows,
    unique_fitbit_devices = unique_fitbit_devices
  )
  
  return(result)
}

folder_path <- './adult/1/'
manifest_path <- './adult/1/Manifest.csv'

result <- get_info_from_folder(folder_path, manifest_path)

# Extracting the results
unique_participant_ids <- result$unique_participant_ids
num_rows_per_file <- result$files_with_num_rows
unique_fitbit_devices <- result$unique_fitbit_devices

# Print the results
print("Unique Participant Identifiers:")
print(unique_participant_ids)

print("Number of Rows per File:")
print(num_rows_per_file %>% tibble::enframe())

print("Unique Fitbit Devices:")
print(unique_fitbit_devices)




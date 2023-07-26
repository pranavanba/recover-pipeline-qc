library(synapser)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(ggpubr)

synLogin()

system('synapse get -r syn51406698')

data <- read.delim('./ce-input-data/SYNAPSE_METADATA_MANIFEST.tsv')

adults_folder_path <- './input_data_unzipped/adult/'
pediatric_folder_path <- './input_data_unzipped/pediatric/'

adult_i <- 1
pediatric_i <- 1

for (p in data$path) {
  if (grepl('adult', p)) {
    unzip(p, exdir = paste0(adults_folder_path, adult_i))
    adult_i <- adult_i + 1
  } else if (grepl('pediatric', p)) {
    unzip(p, exdir = paste0(pediatric_folder_path, pediatric_i))
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

get_unique_participant_ids_enroll_date <- function(folder_path) {
  enrolled_files <- list.files(folder_path, pattern = "EnrolledParticipants.*\\.json$", full.names = TRUE)
  
  data_list <- lapply(enrolled_files, function(file_path) {
    data <- stream_in(file(file_path), flatten = TRUE)
    subset_data <- 
      data[, c("ParticipantIdentifier", "EnrollmentDate"), drop = FALSE] %>% 
      group_by(ParticipantIdentifier) %>% 
      distinct() %>% 
      ungroup()
    return(subset_data)
  })
  
  combined_data <- do.call(rbind, data_list)
  
  return(combined_data)
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

apply_function_to_subdirectories <- function(parent_folder_path, fun) {
  subdirectories <- list.dirs(parent_folder_path, recursive = FALSE, full.names = TRUE)
  results <- lapply(subdirectories, function(subdir) {
    result <- fun(subdir)
    return(result)
  })
  names(results) <- basename(subdirectories)
  return(results)
}

pids_adults <- apply_function_to_subdirectories(adults_folder_path, get_unique_participant_ids)
pids_enroll_adults <- 
  apply_function_to_subdirectories(adults_folder_path, get_unique_participant_ids_enroll_date) %>% 
  bind_rows(.id = "list_name")
nrecs_adults <- apply_function_to_subdirectories(adults_folder_path, get_num_rows_per_file)
devices_adults <- apply_function_to_subdirectories(adults_folder_path, get_unique_fitbit_devices)

pids_ped <- apply_function_to_subdirectories(pediatric_folder_path, get_unique_participant_ids)
pids_enroll_ped <- 
  apply_function_to_subdirectories(pediatric_folder_path, get_unique_participant_ids_enroll_date) %>% 
  bind_rows(.id = "list_name")
nrecs_ped <- apply_function_to_subdirectories(pediatric_folder_path, get_num_rows_per_file)
devices_ped <- apply_function_to_subdirectories(pediatric_folder_path, get_unique_fitbit_devices)

pid_cumulative_counts_adults <- 
  pids_enroll_adults %>% 
  group_by(ParticipantIdentifier, EnrollmentDate) %>% 
  distinct(ParticipantIdentifier, EnrollmentDate, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(EnrollmentDate=lubridate::date(EnrollmentDate)) %>% 
  group_by(EnrollmentDate) %>%
  summarise(count = n_distinct(ParticipantIdentifier)) %>%
  mutate(running_total = cumsum(count))

ggplot(pid_cumulative_counts_adults, aes(x = EnrollmentDate, y = running_total)) +
  geom_line() +
  geom_point() +
  labs(x = "Date", y = "Cumulative Count of Unique Participant IDs") +
  ggtitle("Enrollment Over Time")

ggline(data = pid_cumulative_counts_adults, 
       x = 'EnrollmentDate', 
       y = 'running_total',
       xlab = "Date",
       ylab = "Cumulative Count of Unique Participant IDs",
       title = "Enrollment Over Time") + grids()



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
  enrolled_files <- list.files(folder_path, pattern = "EnrolledParticipants", full.names = TRUE)
  
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
  
  exclude_columns <- c("ExportConfiguration", "ExcludedParticipantIdentifiers")
  manifest_subset <- manifest[, !(names(manifest) %in% exclude_columns)]
  
  out <- 
    manifest_subset %>% 
    t() %>% 
    tibble::enframe() %>% 
    rename(file = name, num_records = value) %>%
    mutate(num_records=as.numeric(num_records)) %>% 
    mutate(
      ExportStartDate = manifest_subset$ExportStartDate,
      ExportEndDate = manifest_subset$ExportEndDate
    ) %>% 
    filter(!(file %in% c('ExportStartDate', 'ExportEndDate'))) %>% 
    mutate(ExportStartDate=date(ExportStartDate),
           ExportEndDate=date(ExportEndDate))
  
  return(out)
}

get_unique_fitbit_devices <- function(folder_path) {
  fitbit_files <- list.files(folder_path, pattern = "FitbitDevices", full.names = TRUE)
  
  combined_result <- data.frame(stringsAsFactors = FALSE)
  
  for (file_path in fitbit_files) {
    data <- stream_in(con = file(file_path), flatten = TRUE)
    
    combined_result <- rbind(combined_result, data)
  }
  
  return(combined_result)
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
nrecs_date_adults <- 
  apply_function_to_subdirectories(adults_folder_path, get_num_rows_per_file) %>% 
  bind_rows(.id = "list_name")

devices_adults <- 
  apply_function_to_subdirectories(adults_folder_path, get_unique_fitbit_devices) %>% 
  tibble::enframe() %>% 
  tidyr::unnest(cols = c(value))

pids_ped <- apply_function_to_subdirectories(pediatric_folder_path, get_unique_participant_ids)
pids_enroll_ped <- 
  apply_function_to_subdirectories(pediatric_folder_path, get_unique_participant_ids_enroll_date) %>% 
  bind_rows(.id = "list_name")

nrecs_ped <- apply_function_to_subdirectories(pediatric_folder_path, get_num_rows_per_file)
nrecs_date_ped <- 
  apply_function_to_subdirectories(pediatric_folder_path, get_num_rows_per_file) %>% 
  bind_rows(.id = "list_name")

devices_ped <- 
  apply_function_to_subdirectories(pediatric_folder_path, get_unique_fitbit_devices) %>% 
  tibble::enframe() %>% 
  tidyr::unnest(cols = c(value))

#### Total counts ###
total_unique_pids_adult <- 
  pids_enroll_adults$ParticipantIdentifier %>% 
  n_distinct()
total_unique_pids_ped <- 
  pids_enroll_ped$ParticipantIdentifier %>% 
  n_distinct()

most_recent_enroll_date_adult <- 
  pids_enroll_adults$EnrollmentDate %>% 
  date() %>% 
  max()
most_recent_enroll_date_ped <- 
  pids_enroll_ped$EnrollmentDate %>% 
  date() %>% 
  max()

### Plots ###

# Particpants
pid_cumulative_counts_adults <- 
  pids_enroll_adults %>% 
  group_by(ParticipantIdentifier, EnrollmentDate) %>% 
  distinct(ParticipantIdentifier, EnrollmentDate, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(EnrollmentDate=lubridate::date(EnrollmentDate)) %>% 
  group_by(EnrollmentDate) %>%
  summarise(count = n_distinct(ParticipantIdentifier)) %>%
  mutate(running_total = cumsum(count))

pid_cumulative_counts_ped <- 
  pids_enroll_ped %>% 
  group_by(ParticipantIdentifier, EnrollmentDate) %>% 
  distinct(ParticipantIdentifier, EnrollmentDate, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(EnrollmentDate=lubridate::date(EnrollmentDate)) %>% 
  group_by(EnrollmentDate) %>%
  summarise(count = n_distinct(ParticipantIdentifier)) %>%
  mutate(running_total = cumsum(count))

a <- ggline(data = pid_cumulative_counts_adults, 
            x = 'EnrollmentDate', 
            y = 'running_total',
            xlab = "Date",
            ylab = "Cumulative Count of Unique Participant IDs",
            title = "Enrollment Over Time (Adults)") + 
  grids()
ggpar(a, caption = glue::glue('n = {total_unique_pids_adult} unique participants as of {most_recent_enroll_date_adult}'))

p <- ggline(data = pid_cumulative_counts_ped, 
            x = 'EnrollmentDate', 
            y = 'running_total',
            xlab = "Date",
            ylab = "Cumulative Count of Unique Participant IDs",
            title = "Enrollment Over Time (Pediatric)") + 
  grids()
ggpar(p, caption = glue::glue('n = {total_unique_pids_ped} unique participants as of {most_recent_enroll_date_ped}'))

# Number of records
# tmp <- 
#   nrecs_date_adults %>% 
#   group_by(ExportStartDate) %>% 
#   mutate(running_total = sum(num_records))
# 
# ggline(data = tmp, x = 'ExportStartDate', y = 'running_total')

pie_adult <- 
  nrecs_date_adults %>% 
  group_by(file) %>% 
  summarise(sum=sum(num_records)) %>% 
  ungroup() %>% 
  mutate(dataset = ifelse(grepl('HealthKit', file), 
                          'HealthKit', 
                          ifelse(grepl('Intraday', file), 
                                 'FitbitIntradayCombined', 
                                 ifelse(grepl('Enrolled', file),
                                        'EnrolledParticipants',
                                        'Fitbit_Other')))) %>% 
  group_by(dataset) %>% 
  summarise(dataset_sum = sum(sum)) %>% 
  ungroup() %>% 
  filter(dataset_sum>0)

labs_adult <- paste0((prop.table(pie_adult$dataset_sum)*100) %>% round(2))
a2 <- ggpie(pie_adult, 'dataset_sum', label = c('','','',''), 
            fill = 'dataset')
ggpar(a2, caption = glue::glue('n = {sum(pie_adult$dataset_sum)} records as of {max(nrecs_date_adults$ExportStartDate)}'), 
      title = "Proportion of Records from Different Datasets (Adults)")

pie_ped <- 
  nrecs_date_ped %>% 
  group_by(file) %>% 
  summarise(sum=sum(num_records)) %>% 
  ungroup() %>% 
  mutate(dataset = ifelse(grepl('HealthKit', file), 
                          'HealthKit', 
                          ifelse(grepl('Intraday', file), 
                                 'FitbitIntradayCombined', 
                                 ifelse(grepl('Enrolled', file),
                                        'EnrolledParticipants',
                                        'Fitbit_Other')))) %>% 
  group_by(dataset) %>% 
  summarise(dataset_sum = sum(sum)) %>% 
  ungroup() %>% 
  filter(dataset_sum>0)

labs_ped <- paste0(pie_ped$dataset, " (", (prop.table(pie_ped$dataset_sum)*100) %>% round(2), "%)")
p2 <- ggpie(pie_ped, 'dataset_sum', label = labs_ped, 
            fill = 'dataset')
ggpar(p2, caption = glue::glue('n = {sum(pie_ped$dataset_sum)} records as of {max(nrecs_date_ped$ExportStartDate)}'), 
      title = "Proportion of Records from Different Datasets (Pediatric)")

nrecs_cumsum_adult <- 
  nrecs_date_adults %>% 
  group_by(ExportStartDate) %>% 
  reframe(sum=sum(num_records)) %>% 
  reframe(ExportStartDate, cumsum=cumsum(sum))

a3 <- ggline(nrecs_cumsum_adult, 'ExportStartDate', 'cumsum',
             xlab = 'Export Date', ylab = '# of Records',
             title = '# of Records Across All Exports Over Time (Adults)') + 
  grids()
ggpar(a3, caption = glue::glue('n = {max(nrecs_cumsum_adult$cumsum)} records as of {max(nrecs_cumsum_adult$ExportStartDate)}'))

nrecs_cumsum_ped <- 
  nrecs_date_ped %>% 
  group_by(ExportStartDate) %>% 
  reframe(sum=sum(num_records)) %>% 
  reframe(ExportStartDate, cumsum=cumsum(sum))

p3 <- ggline(nrecs_cumsum_ped, 'ExportStartDate', 'cumsum',
             xlab = 'Export Date', ylab = '# of Records',
             title = '# of Records Across All Exports Over Time (Pediatric)') + 
  grids()
ggpar(p3, caption = glue::glue('n = {max(nrecs_cumsum_ped$cumsum)} records as of {max(nrecs_cumsum_ped$ExportStartDate)}'))

# file_cumsum_nrecs <- 
#   nrecs_date_adults %>% 
#   group_by(file, ExportStartDate) %>% 
#   reframe(sum=sum(num_records)) %>% 
#   reframe(file, ExportStartDate, cumsum=cumsum(sum))

# Devices
devices_summary_adult <- 
  devices_adults %>% 
  select(ParticipantIdentifier, Device) %>% 
  group_by(Device) %>% 
  summarise(n_participants = n_distinct(ParticipantIdentifier))

devices_ped$value %>% unique() %>% sort()

library(lubridate)
library(stringr)
library(dplyr)
library(arrow)
library(tidyr)
library(synapser)


# Get parquet datasets ----------------------------------------------------
synLogin()
token <- synapser::synGetStsStorageToken(
  entity = 'syn51406699',
  permission = "read_only",
  output_format = "json")

PARQUET_BUCKET <- 'recover-processed-data'
PARQUET_BUCKET_BASE_KEY <- 'main/parquet/'

if (PARQUET_BUCKET==token$bucket && PARQUET_BUCKET_BASE_KEY==token$baseKey) {
  base_s3_uri <- paste0('s3://', token$bucket, '/', token$baseKey)
} else {
  base_s3_uri <- paste0('s3://', PARQUET_BUCKET, '/', PARQUET_BUCKET_BASE_KEY)
}

# configure the environment with AWS token
Sys.setenv('AWS_ACCESS_KEY_ID'=token$accessKeyId,
           'AWS_SECRET_ACCESS_KEY'=token$secretAccessKey,
           'AWS_SESSION_TOKEN'=token$sessionToken)

AWS_PARQUET_DOWNLOAD_LOCATION <- '~/recover-pipeline-qc/parquet/'
unlink(AWS_PARQUET_DOWNLOAD_LOCATION, recursive = T, force = T)
sync_cmd <- glue::glue('aws s3 sync {base_s3_uri} {AWS_PARQUET_DOWNLOAD_LOCATION} --exclude "*owner.txt*" --exclude "*archive*"')
system(sync_cmd)


# Read parquet datasets to df ---------------------------------------------
enrolled <- arrow::open_dataset('./parquet/dataset_enrolledparticipants/') %>% as_tibble()
fitbit_actlogs <- arrow::open_dataset('./parquet/dataset_fitbitactivitylogs//') %>% as_tibble()
fitbit_dailydata <- arrow::open_dataset('./parquet/dataset_fitbitdailydata//') %>% as_tibble()
fitbit_devices <- arrow::open_dataset('./parquet/dataset_fitbitdevices/') %>% as_tibble()
fitbit_intracomb <- arrow::open_dataset('./parquet/dataset_fitbitintradaycombined/') %>% 
  select(ParticipantIdentifier) %>% 
  distinct() %>% 
  as_tibble()
fitbit_rhr <- arrow::open_dataset('./parquet/dataset_fitbitrestingheartrates/') %>% as_tibble()
fitbit_sleeplogs <- arrow::open_dataset('./parquet/dataset_fitbitsleeplogs/') %>% as_tibble()
googlefit <- arrow::open_dataset('./parquet/dataset_googlefitsamples/') %>% as_tibble()
hk_activitysummaries <- arrow::open_dataset('./parquet/dataset_healthkitv2activitysummaries/') %>% as_tibble() # No device vars
hk_ecg <- arrow::open_dataset('./parquet/dataset_healthkitv2electrocardiogram/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier (replace_na Apple Watch and iPhone), StartDate
hk_heartbeat <- arrow::open_dataset('./parquet/dataset_healthkitv2heartbeat/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (replace_na Apple Watch and iPhone), StartDate
hk_samples <- arrow::open_dataset('./parquet/dataset_healthkitv2samples/') %>% 
  select(ParticipantIdentifier, Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier, StartDate) %>% 
  as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (replace_na Apple Watch and iPhone), StartDate
hk_stats <- arrow::open_dataset('./parquet/dataset_healthkitv2statistics/') %>% as_tibble() # No device vars
hk_workouts <- arrow::open_dataset('./parquet/dataset_healthkitv2workouts/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (replace_na Apple Watch and iPhone), StartDate
symptomlog <- arrow::open_dataset('./parquet/dataset_symptomlog/') %>% as_tibble()

# Count number of records by counting number of rows across all parquet datasets
# pq_n_records <- tibble()
# for (dir in list.dirs('./parquet/', recursive = F)) {
#   rows <- open_dataset(dir) %>% nrow()
#   name <- basename(dir)
#   current <- tibble(dataset = name, n_records = rows)
#   pq_n_records <- bind_rows(pq_n_records, current)
# }


# Count of Unique Participants --------------------------------------------
# pq_enrolled <- 
#   enrolled %>% 
#   select(ParticipantIdentifier, EnrollmentDate) %>% 
#   mutate(EnrollmentDate = lubridate::date(EnrollmentDate))
# pq_enrolled

all_participants_by_dataset <- tibble(dataset = character(), ParticipantIdentifier = character())
all_participants_by_dataset <- 
  bind_rows(all_participants_by_dataset, 
            (enrolled %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'enrolledparticipants')),
            (fitbit_actlogs %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'fitbit')),
            (fitbit_dailydata %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'fitbit')),
            (fitbit_devices %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'fitbit')),
            (fitbit_intracomb %>%
               select(ParticipantIdentifier) %>% 
               distinct() %>% 
               mutate(dataset = 'fitbit')),
            (fitbit_rhr %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'fitbit')),
            (fitbit_sleeplogs %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'fitbit')),
            (googlefit %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'googlefit')),
            (hk_activitysummaries %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'healthkit')),
            (hk_ecg %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'healthkit')),
            (hk_heartbeat %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'healthkit')),
            (hk_samples %>%
               select(ParticipantIdentifier) %>% 
               distinct() %>% 
               mutate(dataset = 'healthkit')),
            (hk_stats %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'healthkit')),
            (hk_workouts %>% 
               select(ParticipantIdentifier) %>% 
               mutate(dataset = 'healthkit')),
            symptomlog %>% 
              select(ParticipantIdentifier) %>% 
              mutate(dataset = 'symptomlog')) %>% 
  group_by(dataset, ParticipantIdentifier) %>% 
  distinct() %>% 
  ungroup()


# Unique HealthKit Devices ------------------------------------------------
hk_devices <- bind_rows(
  (hk_ecg %>% 
     select(ParticipantIdentifier, Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier, StartDate) %>% 
     mutate(Source_Identifier = gsub('com\\.apple\\.health.*', 'com.apple.health', Source_Identifier))),
  (hk_heartbeat %>% 
     select(ParticipantIdentifier, Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier, StartDate) %>% 
     mutate(Source_Identifier = gsub('com\\.apple\\.health.*', 'com.apple.health', Source_Identifier))),
  (hk_samples %>%
     select(ParticipantIdentifier, Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier, StartDate) %>%
     mutate(Source_Identifier = gsub('com\\.apple\\.health.*', 'com.apple.health', Source_Identifier))),
  (hk_workouts %>% 
     select(ParticipantIdentifier, Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier, StartDate) %>% 
     mutate(Source_Identifier = gsub('com\\.apple\\.health.*', 'com.apple.health', Source_Identifier)) %>% 
     group_by(Device_Name, Device_Model, Device_HardwareVersion, Source_Identifier))
)


# Devices per Participant -------------------------------------------------
devices_per_pid_fitbit <- 
  fitbit_devices %>% 
  select(ParticipantIdentifier, Device, Type) %>% 
  group_by(ParticipantIdentifier) %>%
  summarise(n_devices_per_participant = n_distinct(Device)) %>% 
  group_by(n_devices_per_participant) %>% 
  count() %>% 
  ungroup() %>% 
  rename(n_participants = n) %>% 
  mutate(dataset = 'fitbit')

devices_per_pid_googlefit <- 
  googlefit %>% 
  select(ParticipantIdentifier, OriginalDataSourceDeviceModel, OriginalDataSourceDeviceType) %>% 
  rename(DeviceModel=OriginalDataSourceDeviceModel,
         DeviceType=OriginalDataSourceDeviceType) %>% 
  group_by(ParticipantIdentifier) %>% 
  summarise(n_devices_per_participant = n_distinct(DeviceModel)) %>% 
  group_by(n_devices_per_participant) %>% 
  count() %>% 
  ungroup() %>% 
  rename(n_participants = n) %>% 
  mutate(dataset = 'googlefit')

devices_per_pid_hk <- 
  hk_devices %>% 
  select(ParticipantIdentifier, Device_HardwareVersion) %>% 
  group_by(ParticipantIdentifier) %>% 
  summarise(n_devices_per_participant = n_distinct(Device_HardwareVersion)) %>% 
  group_by(n_devices_per_participant) %>% 
  count() %>% 
  ungroup() %>% 
  rename(n_participants = n) %>% 
  mutate(dataset = 'healthkit')


# Number of Participants Without Device Data Found in Non-Device Files ----
mismatch_pids_fitbit <- 
  (all_participants_by_dataset %>% 
     filter(dataset=='fitbit') %>% 
     pull(ParticipantIdentifier) %>% 
     unique() %>% 
     sort()
   )[which(!((all_participants_by_dataset %>% filter(dataset=='fitbit') %>% 
                pull(ParticipantIdentifier) %>% 
                unique() %>% 
                sort()
              ) %in% (fitbit_devices$ParticipantIdentifier %>% 
                        unique() %>% 
                        sort())
             )
           )
     ]

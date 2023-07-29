# setwd('./parquet/')
# system('synapse get -r syn51406699')
# setwd('~/recover-pipeline-qc/')

library(arrow)
library(tidyr)
library(dplyr)

enrolled <- arrow::open_dataset('./parquet/dataset_enrolledparticipants/') %>% as_tibble()
fitbit_actlogs <- arrow::open_dataset('./parquet/dataset_fitbitactivitylogs//') %>% as_tibble()
fitbit_dailydata <- arrow::open_dataset('./parquet/dataset_fitbitdailydata//') %>% as_tibble()
fitbit_devices <- arrow::open_dataset('./parquet/dataset_fitbitdevices/') %>% as_tibble()
fitbit_intracomb <- arrow::open_dataset('./parquet/dataset_fitbitintradaycombined/') %>% 
  select(ParticipantIdentifier, DateTime) %>% 
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
pq_n_records <- tibble()
for (dir in list.dirs('./parquet/', recursive = F)) {
  rows <- open_dataset(dir) %>% nrow()
  name <- basename(dir)
  current <- tibble(dataset = name, n_records = rows)
  pq_n_records <- bind_rows(pq_n_records, current)
}

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
  distinct()

# setwd('./parquet/')
# system('synapse get -r syn51406699')
# setwd('~/recover-pipeline-qc/')

library(arrow)
library(tidyr)
library(dplyr)

fitbitdevices <- arrow::open_dataset('./parquet/dataset_fitbitdevices/') %>% as_tibble()
enrolled <- arrow::open_dataset('./parquet/dataset_enrolledparticipants/') %>% as_tibble()
googlefit <- arrow::open_dataset('./parquet/dataset_googlefitsamples/') %>% as_tibble()
hk_activitysummaries <- arrow::open_dataset('./parquet/dataset_healthkitv2activitysummaries/') %>% as_tibble() # No device vars
hk_ecg <- arrow::open_dataset('./parquet/dataset_healthkitv2electrocardiogram/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (excluding Apple Watch and iPhone), StartDate
hk_heartbeat <- arrow::open_dataset('./parquet/dataset_healthkitv2heartbeat/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (excluding Apple Watch and iPhone), StartDate
hk_samples <- arrow::open_dataset('./parquet/dataset_healthkitv2samples/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (excluding Apple Watch and iPhone), StartDate
hk_stats <- arrow::open_dataset('./parquet/dataset_healthkitv2statistics/') %>% as_tibble() # No device vars
hk_workouts <- arrow::open_dataset('./parquet/dataset_healthkitv2workouts/') %>% as_tibble() # Device_Name, Device_Model, Device_HardwareVersion, Source_Name (excluding Apple Watch and iPhone), StartDate
symptomlog <- arrow::open_dataset('./parquet/dataset_symptomlog/') %>% as_tibble()

# Count number of records by counting number of rows across all parquet datasets
pq_n_records <- tibble()
for (dir in list.dirs('./parquet/', recursive = F)) {
  rows <- open_dataset(dir) %>% nrow()
  name <- basename(dir)
  current <- tibble(dataset = name, n_records = rows)
  pq_n_records <- bind_rows(pq_n_records, current)
}


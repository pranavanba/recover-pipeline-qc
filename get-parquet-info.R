# setwd('./parquet/')
# system('synapse get -r syn51406699')
# setwd('~/recover-pipeline-qc/')

library(arrow)
library(tidyr)
library(dplyr)

fitbitdevices <- arrow::open_dataset('./parquet/dataset_fitbitdevices/') %>% as.data.frame()
enrolled <- arrow::open_dataset('./parquet/dataset_enrolledparticipants/') %>% as.data.frame()
googlefit <- arrow::open_dataset('./parquet/dataset_googlefitsamples/') %>% as.data.frame()

# Count number of records by counting number of rows across all parquet datasets
pq_n_records <- tibble()
for (dir in list.dirs('./parquet/', recursive = F)) {
  rows <- open_dataset(dir) %>% nrow()
  name <- basename(dir)
  current <- tibble(dataset = name, n_records = rows)
  pq_n_records <- bind_rows(pq_n_records, current)
}


# setwd('./parquet/')
# system('synapse get -r syn51406699')
# setwd('~/recover-pipeline-qc/')

library(arrow)

fitbitdevices <- arrow::open_dataset('./parquet/dataset_fitbitdevices/') %>% as.data.frame()
enrolled <- arrow::open_dataset('./parquet/dataset_enrolledparticipants/') %>% as.data.frame()

library(synapser)
synLogin()

executed_url <- 'https://github.com/pranavanba/recover-pipeline-qc/blob/main/Parquet%20Metrics.qmd'

synStore(File('./Parquet Metrics.html', parent='syn52202326'),
         executed = executed_url)

file_names <- list.files('./rendered_dfs', pattern = '\\.csv$', full.names = TRUE)
for (file in file_names) {
  url <- paste0(executed_url, '?raw=true')
  synStore(File(file, parent='syn52203469'), executed=url)
}

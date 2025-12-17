library(dplyr)

datos_finales_camiones <- readr::read_csv(
  file = '../../Notebook/data/datos_idmod_2431.csv'
)

datos_finales_camiones %>%
  dplyr::arrange(time_empty)

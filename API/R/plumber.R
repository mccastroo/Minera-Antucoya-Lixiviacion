library(dplyr)
library(lubridate)
library(plumber)

#* @apiTitle Lixiviación Minera Antucoya API - R
#* @apiDescription Funciones que permiten trabajar variables para el proyecto de Lixiviación de Minera Antucoya
upsert_cosmos <- function(cosmos_container, df, id_str){
  datos_flujo <- lapply(1:nrow(df), function(r){
    
    fila_datos <- as.list(df[r, ])
    id <- fila_datos[[id_str]]
    
    print(stringr::str_glue('{r}/{nrow(df)}'))
    
    resultado_ingreso <- tryCatch(expr = {
      AzureCosmosR::create_document(cosmos_container, fila_datos)
      ingreso <- data.frame(id = id, insertado = "Insertado") 
      
      return(ingreso)
    }, 
    error = function(cond_1){
      error_detect <- 'Entity with the specified id already exists in the system'
      
      if (stringr::str_detect(cond_1[['message']], error_detect)) {
        tryCatch(expr = {
          AzureCosmosR::delete_document(cosmos_container, partition_key = id, id = id, confirm = FALSE)
          AzureCosmosR::create_document(cosmos_container, fila_datos)
          
          ingreso <- data.frame(id = id, insertado = "Reemplazado")
          return(ingreso)
        },
        error = function(cond_2){
          ingreso <- data.frame(id = id, insertado = "Error")
          return(ingreso)
        }
        )
      }
    })
  })
  return(datos_flujo)
}

grades_names <- c(
  'TON', 
  'CUT', 
  'CUS', 
  'NO3', 
  'CO3', 
  'MENA1', 
  'CAL1',	
  'IQS1', 
  'ACIDO', 
  'MENA2', 
  'MENA3', 
  'CAL2', 
  'CAL3', 
  'CAL4', 
  'IQS2', 
  'IQS3',
  'IQS4',	
  'UGM_NIR', 
  'SZO', 
  'CAO', 
  'PGL', 
  'CHL', 
  'SER', 
  'QZ', 
  'FEO', 
  'CAL', 
  'YES'	
)

folds <- 12

variablesNumericas <- c(
  'CUT', 
  'CUS', 
  'NO3', 
  'CO3',
  'SZO', 
  'CAO', 
  'PGL', 
  'CHL', 
  'ACIDO', 
  'SER', 
  'QZ', 
  'FEO', 
  'YES'
  )

#* Función que concluye las variables de despacho para el modelo predictivo
#* @parser json
#* @post /jigsaw_variables
#* @serializer unboxedJSON
function(req) {
  datos_finales_camiones <- req$body
  
  datos_finales_camiones <- 
    dplyr::bind_rows(datos_finales_camiones) %>%
    dplyr::arrange(time_empty) %>%
    dplyr::rename('UGM' = 'UGM_NIR') %>%
    dplyr::select(-notes) %>%
    dplyr::mutate(UGM = as.numeric(UGM)) %>%
    dplyr::mutate_at(variablesNumericas, .funs = ~as.numeric(.x)) %>%
    dplyr::mutate(
      MENA1 = as.numeric(MENA1), 
      MENA2 = as.numeric(MENA2), 
      MENA3 = as.numeric(MENA3)
      ) %>%
    # Hasta acá configuramos la nueva API que permita llevar los datos hasta Cosmos DB
    # Este último filtro lo consideramos para tener datos del 2020 y lo que cae en Chancado
    dplyr::filter(UGM %in% c(10, 20, 21, 24, 30, 40))

  id_mods_vector <- unique(datos_finales_camiones$id_mod)

  # Variables Númericas -----------------------------------------------------
  # Solo a nivel de módulo concluimos estadísticos
  
  ModuloVarsNumericos <-
    datos_finales_camiones %>%
    dplyr::select(id_mod, variablesNumericas) %>%
    tidyr::pivot_longer(
      cols = variablesNumericas, 
      names_to = 'Medicion', 
      values_to = 'Valor'
      ) %>%
    dplyr::filter(Valor >= 0) %>%
    dplyr::group_by(id_mod, Medicion) %>%
    dplyr::summarise_all(
      .funs = list(
        minimo = min,
        maximo = max,
        mediana = median,
        promedio = mean,
        desviacion = sd,
        varianza = var
      )
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = Medicion,
      names_sep = '_',
      values_from = c('minimo', 'promedio', 'mediana', 'desviacion', 'varianza', 'maximo')
    )

  # UGM ---------------------------------------------------------------------

  N_UGM_rellenar <-
    lapply(id_mods_vector, function(m){
      lapply(seq(1, folds), function(i){
        data.frame(id_mod = m, G = i, UGM = c(10, 20, 21, 24, 30, 40))
      }) %>%
        dplyr::bind_rows()
    }) %>%
    dplyr::bind_rows()
  
  UGM_modulo_general <-
    datos_finales_camiones %>%
    dplyr::group_by(id_mod, UGM) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::full_join(
      N_UGM_rellenar %>%
        dplyr::select(id_mod, UGM) %>%
        dplyr::distinct(), 
      by = c('id_mod', 'UGM')
    ) %>%
    dplyr::arrange(UGM) %>%
    tidyr::fill(id_mod) %>%
    base::replace(is.na(.), 0) %>%
    dplyr::group_by(id_mod) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(
      names_from = UGM, 
      values_from = P, 
      names_prefix = 'UGM_'
    ) %>%
    base::replace(is.na(.), 0)
  
  UGM_modulo_separacion <-
    datos_finales_camiones %>%
    dplyr::group_by(id_mod) %>%
    dplyr::mutate(ID = 1:dplyr::n()) %>%
    dplyr::mutate(G = ceiling(ID/dplyr::n()*folds), .after = id_mod) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(id_mod, G, UGM) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::ungroup() %>% 
    dplyr::full_join(N_UGM_rellenar, by = c('id_mod', 'G', 'UGM')) %>%
    dplyr::arrange(G, UGM) %>%
    tidyr::fill(id_mod) %>%
    base::replace(is.na(.), 0) %>%
    dplyr::group_by(id_mod, G) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(
      names_from = c('UGM', 'G'),  
      names_glue = '{G}_UGM_{UGM}', 
      values_from = 'P', 
      names_sort = TRUE
    )  %>%
    base::replace(is.na(.), 0)
    

  UGM_modulo_estadisticos <-
    UGM_modulo_separacion %>%
    tidyr::pivot_longer(cols = -id_mod, values_to = 'Porcentaje') %>%
    tidyr::separate(name, sep = "_", into = c("Fold", "H", "UGM")) %>%
    dplyr::select(-H) %>%
    dplyr::group_by(id_mod, UGM) %>%
    dplyr::summarise(
      minimo = min(Porcentaje),
      promedio = mean(Porcentaje),
      mediana = median(Porcentaje),
      desviacion = sd(Porcentaje),
      varianza = var(Porcentaje),
      maximo = max(Porcentaje)
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = UGM,
      names_sep = '_UGM_',
      values_from = c('minimo', 'promedio', 'mediana', 'desviacion', 'varianza', 'maximo')
    )

  
  # COMP ------------------------------------------------------------------------------


  N_COMP_rellenar <-
    lapply(id_mods_vector, function(m){
      lapply(seq(1, folds), function(i){
        data.frame(id_mod = m, G = i, COMP = c('OVN', 'SULF', 'LIM'))
      }) %>%
        dplyr::bind_rows()
    }) %>%
    dplyr::bind_rows()

  COMP_modulo_general <-
    datos_finales_camiones %>%
    dplyr::select(time_empty, id_mod, MENA1, MENA2, MENA3) %>%
    tidyr::pivot_longer(cols = c('MENA1', 'MENA2', 'MENA3')) %>%
    dplyr::group_by(time_empty, id_mod) %>%
    dplyr::filter(value != 0) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(name = stringr::str_remove(name, 'MENA')) %>%
    dplyr::mutate(
      COMP = dplyr::case_when(
        name == "1" ~ "OVN",
        name == "2" ~ "SULF",
        name == "3" ~ "LIM"
      )
    ) %>%
    dplyr::select(-value, -name, -time_empty) %>%
    dplyr::group_by(id_mod, COMP) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::full_join(
      N_COMP_rellenar %>%
        dplyr::select(id_mod, COMP) %>%
        dplyr::distinct(),
      by = c('id_mod', 'COMP')
    ) %>%
    dplyr::arrange(id_mod, COMP) %>%
    tidyr::fill(id_mod) %>%
    base::replace(is.na(.), 0) %>%
    dplyr::group_by(id_mod) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(
      names_from = COMP,
      values_from = P,
      names_prefix = 'COMP_'
    ) %>%
    base::replace(is.na(.), 0)
  
  
  COMP_modulo_separacion <-
    datos_finales_camiones %>%
    dplyr::select(time_empty, id_mod, MENA1, MENA2, MENA3) %>%
    tidyr::pivot_longer(cols = c('MENA1', 'MENA2', 'MENA3')) %>%
    dplyr::group_by(time_empty, id_mod) %>%
    dplyr::filter(value != 0) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(name = stringr::str_remove(name, 'MENA')) %>%
    dplyr::mutate(
      COMP = dplyr::case_when(
        name == "1" ~ "OVN",
        name == "2" ~ "SULF",
        name == "3" ~ "LIM"
      )
    ) %>%
    dplyr::select(-value, -name) %>%
    dplyr::group_by(id_mod) %>%
    dplyr::mutate(ID = 1:dplyr::n()) %>%
    dplyr::mutate(G = ceiling(ID/dplyr::n()*folds), .after = id_mod) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(id_mod, G, COMP) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::full_join(N_COMP_rellenar, by = c('id_mod', 'G', 'COMP')) %>%
    dplyr::arrange(G) %>%
    tidyr::fill(id_mod) %>%
    base::replace(is.na(.), 0) %>%
    dplyr::group_by(id_mod, G) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(
      names_from = c('COMP', 'G'),
      names_glue = '{G}_COMP_{COMP}',
      values_from = 'P',
      names_sort = TRUE
      )  %>%
    base::replace(is.na(.), 0)

  COMP_modulo_estadisticos <-
    COMP_modulo_separacion %>%
    tidyr::pivot_longer(
      cols = -id_mod,
      values_to = 'Porcentaje'
      ) %>%
    tidyr::separate(
      name,
      sep = "_",
      into = c("Fold", "H", "COMP")
      ) %>%
    dplyr::select(-H) %>%
    dplyr::group_by(id_mod, COMP) %>%
    dplyr::summarise(
      minimo = min(Porcentaje),
      promedio = mean(Porcentaje),
      mediana = median(Porcentaje),
      desviacion = sd(Porcentaje),
      varianza = var(Porcentaje),
      maximo = max(Porcentaje)
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = COMP,
      names_sep = '_COMP_',
      values_from = c(
        'minimo',
        'promedio',
        'mediana',
        'desviacion',
        'varianza',
        'maximo'
        )
    )

  
  # Variable retorno ------------------------------------------------------------------
  
  variablesJigsaw <-
    UGM_modulo_general %>%
    dplyr::inner_join(UGM_modulo_estadisticos, by = 'id_mod') %>%
    dplyr::inner_join(COMP_modulo_general, by = 'id_mod') %>%
    dplyr::inner_join(COMP_modulo_estadisticos, by = 'id_mod') %>%
    dplyr::inner_join(ModuloVarsNumericos, by = 'id_mod')
  
  return(variablesJigsaw)
}


#* Función que concluye los estadísticos a mostrar en la visualización de avances
#* @parser json
#* @post /jigsaw_avances
#* @serializer unboxedJSON

function(req){
  datos_finales_camiones_json <- req$body
  variables_numericas <- c(
    "CUT", 
    "CUS",
    "NO3", 
    "CO3", 
    "SZO", 
    "CAO", 
    "PGL", 
    "CHL",
    "SER", 
    "CAL",
    "MENA1",
    "MENA2",
    "MENA3"
  )
  
  datos_finales_camiones <- 
    dplyr::bind_rows(datos_finales_camiones_json) %>%
    dplyr::rename('UGM' = 'UGM_NIR') %>%
    dplyr::mutate(UGM = as.numeric(UGM)) %>%
    dplyr::mutate_at(variables_numericas, .funs = ~as.numeric(.x)) %>%
    dplyr::mutate(CUS_CUT = CUS/CUT)
  
  COMP_modulo_avances <-
    datos_finales_camiones %>%
    dplyr::select(time_empty, id_mod, Intervalo, MENA1, MENA2, MENA3) %>%
    tidyr::pivot_longer(cols = c('MENA1', 'MENA2', 'MENA3')) %>%
    dplyr::group_by(time_empty, id_mod) %>%
    dplyr::filter(value != 0) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(name = stringr::str_remove(name, 'MENA')) %>%
    dplyr::mutate(
      COMP = dplyr::case_when(
        name == "1" ~ "OVN",
        name == "2" ~ "SULF",
        name == "3" ~ "LIM"
      )
    ) %>%
    dplyr::select(-value, -name) %>%
    dplyr::group_by(id_mod, Intervalo, COMP) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::group_modify(
      .f = ~{
        .x %>%
          dplyr::full_join(
            data.frame(COMP = c('OVN', 'SULF', 'LIM')),
            by = c('COMP')
          ) %>%
          tidyr::replace_na(list(N = 0))
      }
    ) %>%
    dplyr::ungroup(COMP) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(names_from = COMP, values_from = P)

  UGM_modulo_avances <-
    datos_finales_camiones %>%
    dplyr::group_by(id_mod, Intervalo, UGM) %>%
    dplyr::summarise(N = dplyr::n()) %>%
    dplyr::group_modify(
      .f = ~{
        .x %>%
          dplyr::full_join(
            data.frame(UGM = c(10, 20, 21, 24, 30, 40)),
            by = c('UGM')
          ) %>%
          tidyr::replace_na(list(N = 0)) %>%
          dplyr::arrange(UGM)
      }
    ) %>%
    dplyr::ungroup(UGM) %>%
    dplyr::mutate(P = N/sum(N)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-N) %>%
    tidyr::pivot_wider(names_from = UGM, values_from = P, names_prefix = 'UGM_')

  variables_numericas <- c(variables_numericas, 'CUS_CUT')

  numericas_modulo_avances<-
    datos_finales_camiones %>%
    dplyr::group_by(id_mod, Intervalo) %>%
    dplyr::summarise_at(
      .vars = dplyr::vars(variables_numericas),
      .funs = dplyr::funs(
        mean,
        min,
        max,
        p50 = quantile(., 0.50),
        p75 = quantile(., 0.75),
        p90 = quantile(., 0.90)
      )
    ) %>%
    dplyr::ungroup()

  datos_modulos_avances <-
    dplyr::inner_join(
      UGM_modulo_avances,
      COMP_modulo_avances,
      by = c('id_mod', 'Intervalo')
    ) %>%
    dplyr::inner_join(
      numericas_modulo_avances,
      by = c('id_mod', 'Intervalo')
    ) %>%
    dplyr::mutate(
      id_mod_avance = stringr::str_glue('{id_mod}_{Intervalo}')
    )
  
  return(datos_modulos_avances)
}
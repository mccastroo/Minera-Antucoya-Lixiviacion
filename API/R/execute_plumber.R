custom_spec <- function(spec) {

    request_body <- list(
        description = "JSON con los datos de los camiones, las variables y los intervalos de tiempo de apilado",
        required = TRUE,
        content = list(
            `application/json` = list(
                schema = list(
                    type = "array",
                    items = list(
                        type = "object",
                        properties = list(
                            id = list(type = "integer"),
                            updated_at = list(type = "string", format = "date-time"),
                            revision = list(type = "string"),
                            time_empty = list(type = "string", format = "date-time"),
                            truck_id = list(type = "integer"),
                            shovel_id = list(type = "integer"),
                            blast_id = list(type = "integer"),
                            material_id = list(type = "integer"),
                            reclaim_cycle = list(type = "string"),
                            latitude = list(type = "integer"),
                            longitude = list(type = "integer"),
                            elevation = list(type = "integer"),
                            notes = list(type = "string"),
                            by_dispatcher = list(type = "boolean"),
                            deleted_at = list(type = "string", format = "date-time"),
                            assignment_reason_id = list(type = "integer"),
                            blend_model_id = list(type = "integer"),
                            assigned_dump_id = list(type = "integer"),
                            grade_quality_id = list(type = "integer"),
                            Intervalo = list(type = "string"),
                            id_mod = list(type = "string"),
                            TON = list(type = "string"),
                            CUT = list(type = "string"),
                            CUS = list(type = "string"),
                            NO3 = list(type = "string"),
                            CO3 = list(type = "string"),
                            MENA1 = list(type = "string"),
                            CAL1 = list(type = "string"),
                            IQS1 = list(type = "string"),
                            ACIDO = list(type = "string"),
                            MENA2 = list(type = "string"),
                            MENA3 = list(type = "string"),
                            CAL2 = list(type = "string"),
                            CAL3 = list(type = "string"),
                            CAL4 = list(type = "string"),
                            IQS2 = list(type = "string"),
                            IQS3 = list(type = "string"),
                            IQS4 = list(type = "string"),
                            UGM_NIR = list(type = "string"),
                            SZO = list(type = "string"),
                            CAO = list(type = "string"),
                            PGL = list(type = "string"),
                            CHL = list(type = "string"),
                            SER = list(type = "string"),
                            QZ = list(type = "string"),
                            FEO = list(type = "string"),
                            CAL = list(type = "string"),
                            YES = list(type = "string")
                        )
                    )
                )
            )
        )
    )
  spec$paths$`/jigsaw_avances`$post$requestBody <- request_body
  spec$paths$`/jigsaw_variables`$post$requestBody <- request_body
  return(spec)
}


pr <- plumber::plumb(file='plumber.R')
pr <- pr_set_api_spec(pr, custom_spec)
plumber::pr_run(pr, host = '0.0.0.0', port = 8787)
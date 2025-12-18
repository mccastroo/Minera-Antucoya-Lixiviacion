from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Any
from datetime import datetime, timedelta
from io import StringIO
from azure_connections import storage_c, cosmos_c
from config import grades_names, tz_cambio, ciclo_modulo, encontrar_intervalo
from copy import copy
from zoneinfo import ZoneInfo

import pytz
import json
import logging
import pandas as pd

# Configurar el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.get("/")
def read_root():
    mensaje = "API que trabaja los datos de Minera Antucoya para modelos predictivos"
    return mensaje

class Intervalo(BaseModel):
    ini_apil: str
    fin_apil: str
    delta : str

@app.post("/features_dates")
def features_dates(request: Intervalo):
    ini_apil_UTC_4_str = request.ini_apil
    fin_apil_UTC_4_str = request.fin_apil
    delta = float(request.delta)
    
    if len(ini_apil_UTC_4_str) == 10 :
        ini_apil_UTC_4_str = f"{ini_apil_UTC_4_str} 00:00:00"

    if len(fin_apil_UTC_4_str) == 10 :
        fin_apil_UTC_4_str = f"{fin_apil_UTC_4_str} 00:00:00"

    ini_apil_features_UTC_4_str = (
        datetime.strptime(ini_apil_UTC_4_str, '%Y-%m-%d %H:%M:%S') - timedelta(hours=delta)
        ).strftime('%Y-%m-%d %H:%M:%S')
    
    fin_apil_features_UTC_4_str = (
        datetime.strptime(fin_apil_UTC_4_str, '%Y-%m-%d %H:%M:%S') - timedelta(hours=delta)
        ).strftime('%Y-%m-%d %H:%M:%S')
    
    ini_apil_features_UTC_str = tz_cambio(ini_apil_features_UTC_4_str, pytz.timezone('America/Santiago'), pytz.timezone("UTC"))
    fin_apil_features_UTC_str = tz_cambio(fin_apil_features_UTC_4_str, pytz.timezone('America/Santiago'), pytz.timezone("UTC"))
    
    return {
        "ini_apil_features_UTC_str" : ini_apil_features_UTC_str,
        "fin_apil_features_UTC_str" : fin_apil_features_UTC_str
    }

@app.post("/unique_grade_id")
def unique_grade_id(trucks_input : List[Any]) -> List[int]:
    
    grade_ids = [record['grade_id'] for record in trucks_input if 'grade_id' in record]

    unique_grade_ids = list(set(grade_ids))
    unique_grade_ids = [grade_id for grade_id in unique_grade_ids if grade_id is not None]

    return unique_grade_ids

class TrucksQualitiesRequest(BaseModel):
    trucks_input: List[Any]
    qualities_input: List[Any]
    id_mod: str
    avances: List[Any]
    delta: float

@app.post("/trucks_qualities_union")
def trucks_qualities_union(request: TrucksQualitiesRequest):
    
    trucks_input = request.trucks_input
    qualities_input = request.qualities_input
    id_mod = request.id_mod
    avances = request.avances
    delta = request.delta
    

    for avance in avances:
        avance['Fecha'] = datetime.strptime(avance['Fecha'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=ZoneInfo('America/Santiago'))

    # Generar intervalos en una línea
    intervalos = [
        {
            'Intervalo': f"{a['Avance']} - {b['Avance']}", 
            'Fecha_inicio': a['Fecha'], 
            'Fecha_fin': b['Fecha']
        } 
        for a, b in zip(avances, avances[1:])
    ]
    truck_record_qualities = []

    for truck_record in trucks_input :
        # Asignación de Intervalo de Avance

        time_empty_UTC = datetime.strptime(truck_record['time_empty'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo('UTC'))
        time_empty_stacking = time_empty_UTC + timedelta(hours=delta)
        intervalo = encontrar_intervalo(time_empty_stacking, intervalos)
    
        truck_record['Intervalo'] = intervalo['Intervalo']

        
        # Filtrar los registros en data_qualities cuyo grade_id coincida con el grade_id del truck_record
        filtered_qualities = [
            record for record in qualities_input 
            if record['grade_id'] == truck_record['grade_id'] 
            and 
            record['start_date'] <= truck_record['time_empty']
        ]
        
        # Considerar el record con mayor valor de start_date
        if filtered_qualities:
            max_start_date_record = max(filtered_qualities, key=lambda x: x['start_date'])
            # Seleccionar solo las llaves grade_id y qualities
            selected_keys_record = {
                'grade_id': max_start_date_record['grade_id'],
                'qualities': max_start_date_record['qualities']
            }
            
            qualities_values = selected_keys_record['qualities'].split(',')
            
            qualities_dict = {'grade_id': selected_keys_record['grade_id']}
            qualities_dict.update(dict(zip(grades_names, qualities_values)))
    
            truck_record.update({'id_mod' : id_mod})
            truck_record_qualities.append({**truck_record, **qualities_dict})

            
        else:
            logger.info("No hay registros que coincidan con el grade_id y la fecha especificada.")
           
    return json.dumps(truck_record_qualities)



class DatosADF(BaseModel):
    datos_adf: str

def upload_blob(tipo_ontologia: str, blob_name: str, data: str, overwrite: bool = True):
    
    try:
        storage_c[tipo_ontologia].upload_blob(
            name=blob_name, 
            data=data, 
            overwrite=overwrite
        )
        
        return {"message": f"Archivo {blob_name} subido correctamente"}
    
    except :
        return {"message": f"Archivo {blob_name} no subido"}

def upload_document_cosmos(container_name: str, document: dict):
    
    try:
        cosmos_c[container_name].upsert_item(document)
        id_mod = document['id_mod']
        
        return {"message": f"Documento {id_mod} subido correctamente"}
    except:
        id_mod = document['id_mod']
        return {"message": f"Documento {id_mod} no subido"}


@app.post("/write_trucks_qualities_union")
def write_trucks_qualities_union(datos: DatosADF):
    
    json_datos = json.loads(datos.datos_adf)
    df_datos = pd.DataFrame(json_datos)
    
    id_mod = df_datos.id_mod.unique()[0]
    ciclo, modulo = ciclo_modulo(id_mod)

    # Convertir el DataFrame a un CSV en memoria
    csv_buffer = StringIO()
    df_datos.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Subir el CSV al contenedor de Azure Blob Storage
    blob_name = f"camiones_idmod/datos_ciclo_{ciclo}_modulo_{modulo}.csv"
        
    subir_azure = upload_blob(
        tipo_ontologia='datos-naturales', 
        blob_name=blob_name, 
        data=csv_buffer.getvalue(), 
        overwrite=True
    )
    
    return subir_azure
    
class DatosADFVariables(BaseModel):
    datos_adf: List[Any]

@app.post("/write_variables")
def write_variables(datos: DatosADFVariables):
    
    json_datos = datos.datos_adf
    df_datos = pd.DataFrame(json_datos)
    
    id_mod = df_datos.id_mod.unique()[0]
    ciclo, modulo = ciclo_modulo(id_mod)
    
    blob_name = f"modulos/mcp/variable_ciclo_{ciclo}_modulo_{modulo}.csv"
    
    subir_azure = upload_blob(
        tipo_ontologia='ontologia', 
        blob_name=blob_name, 
        data=df_datos.to_csv(index=False), 
        overwrite=True
    )
    
    return subir_azure


@app.post("/write_variables_cosmos")
def write_variables_cosmos(datos: DatosADFVariables):
    
    json_datos = datos.datos_adf
    
    id_mod = json_datos[0]['id_mod']
    ciclo, modulo = ciclo_modulo(id_mod)
    
    return_cosmos = []
    
    for document in json_datos:
        document['Ciclo'] = ciclo
        document['Modulo'] = modulo
        document['id'] = id_mod
        
        document = {
            'id': document['id'],
            'id_mod': document['id_mod'],
            'Ciclo': document['Ciclo'],
            'Modulo': document['Modulo'],
            **{k: v for k, v in document.items() if k not in ['id', 'id_mod', 'Ciclo', 'Modulo']}
        }

    
        subir_cosmos = upload_document_cosmos(
            container_name='mcp', 
            document=document
        )
        
        return_cosmos.append(subir_cosmos)

    return return_cosmos

class RangoApilado(BaseModel):
    ini_apil: str
    fin_apil: str
    id_mod: str

@app.post("/tonelaje_hl")
def tonelaje_hl(datos: RangoApilado):
    
    ini_apil = datos.ini_apil
    fin_apil = datos.fin_apil
    id_mod = datos.id_mod
    
    datos_tonelaje = cosmos_c['pi-system'].query_items(
        query=f"SELECT c.Fecha, c.Valor FROM c WHERE c.Fecha >= '{ini_apil}' AND c.Fecha <= '{fin_apil}'",
        enable_cross_partition_query=True
    )
    
    datos_tonelaje = copy(datos_tonelaje)
    datos_tonelaje = list(datos_tonelaje)
    
    datos_tonelaje = [{**item, 'Valor': float(item['Valor'])} for item in datos_tonelaje if item['Valor'] is not None]
    
    if datos_tonelaje:
        record_mayor_fecha = max(datos_tonelaje, key=lambda x: x['Fecha'])
        record_menor_fecha = min(datos_tonelaje, key=lambda x: x['Fecha'])
        
        years = {datetime.strptime(item['Fecha'], '%Y-%m-%d %H:%M:%S').year for item in datos_tonelaje}
        
        print(years)
        if len(years) > 1:
            menor_anio = min(years)
            
            record_mayor_menor_anio = max([item for item in datos_tonelaje if datetime.strptime(item['Fecha'], '%Y-%m-%d %H:%M:%S').year == menor_anio], key=lambda x: x['Fecha'])
            valor_cambio_anio = record_mayor_menor_anio['Valor']
        else:
            valor_cambio_anio = 0
        
        tonelaje = record_mayor_fecha['Valor'] + valor_cambio_anio - record_menor_fecha['Valor']
        ciclo, modulo = ciclo_modulo(id_mod)
        
        tonelaje_dict = {
            'Ciclo' : ciclo,
            'Modulo' : modulo,
            'id_mod' : id_mod,
            'Tonelaje' : tonelaje
        }
                
        return tonelaje_dict
    
    else:
        return {"message": "No hay datos de tonelaje en el rango proporcionado."}
    

@app.post("/write_tonelaje")
def write_tonelaje(datos_tonelaje : dict):
    ciclo = datos_tonelaje['Ciclo']
    modulo = datos_tonelaje['Modulo']
    
    tonelaje_df = pd.DataFrame([datos_tonelaje])
            
    csv_buffer = StringIO()
    tonelaje_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    blob_name = f"modulos/tonelajes/tonelaje_ciclo_{ciclo}_modulo_{modulo}.csv"
            
    subir_azure = upload_blob(
        tipo_ontologia='ontologia', 
        blob_name=blob_name, 
        data=csv_buffer.getvalue(), 
        overwrite=True
    )

    return subir_azure


@app.post("/write_tonelaje_cosmos")
def write_tonelaje_cosmos(datos_tonelaje : dict):

    datos_tonelaje['id_mod'] = str(datos_tonelaje['id_mod'])
    datos_tonelaje['id'] = datos_tonelaje['id_mod']
    
    document = {
        'id': datos_tonelaje['id'],
        'id_mod': datos_tonelaje['id_mod'],
        'Ciclo': datos_tonelaje['Ciclo'],
        'Modulo': datos_tonelaje['Modulo'],
        **{k: v for k, v in datos_tonelaje.items() if k not in ['id', 'id_mod', 'Ciclo', 'Modulo']}
    }
    
    subir_cosmos = upload_document_cosmos(
        container_name='tonelajes-general', 
        document=document
    )
    
    return subir_cosmos
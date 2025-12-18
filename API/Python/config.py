from datetime import datetime
import pytz


grades_names = [
  'TON', 'CUT', 'CUS', 'NO3', 'CO3', 'MENA1', 'CAL1', 'IQS1', 'ACIDO', 'MENA2', 'MENA3', 'CAL2', 'CAL3', 'CAL4', 
  'IQS2', 'IQS3', 'IQS4', 'UGM_NIR', 'SZO', 'CAO', 'PGL', 'CHL', 'SER', 'QZ', 'FEO', 'CAL', 'YES', 'ACIDO', 'MENA2', 
  'MENA3', 'CAL2', 'CAL3', 'CAL4', 'IQS2', 'IQS3', 'IQS4', 'UGM_NIR', 'SZO', 'CAO', 'PGL', 'CHL', 'SER', 'QZ', 'FEO', 'CAL', 'YES'	
  ]

def tz_cambio(date_str, original_tz, final_tz) : 

    '''
    **Function Name:** tz_cambio

    **Description:**
    The `tz_cambio` function converts a date string from one time zone to another. 
    It takes the `date_str`, original time zone (`original_tz`), and final time zone (`final_tz`) as input and returns the converted date string in the final time zone.

    **Parameters:**
    - `date_str` (str): The input date string in the format 'YYYY-MM-DD HH:MM:SS'.
    - `original_tz` (pytz time zone object): The original time zone of the input date string.
    - `final_tz` (pytz time zone object): The target time zone for converting the date.

    **Returns:**
    - `date_str_final_tz_str` (str): The converted date string in the final time zone, formatted as 'YYYY-MM-DD HH:MM:SS'.
    '''
    
    date_datetime_original_tz = original_tz.localize(datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S"), is_dst=False)
    date_datetime_final_tz = date_datetime_original_tz.astimezone(tz=final_tz)
    date_str_final_tz_str = date_datetime_final_tz.strftime("%Y-%m-%d %H:%M:%S")
    
    return date_str_final_tz_str
  
def ciclo_modulo(id_mod) :
    """
    **Function Name:** ciclo_modulo

    **Description:**
    The `ciclo_modulo` function extracts the cycle and module numbers from a given identifier string (`id_mod`). 
    The identifier string can be either 3 or 4 characters long. The function returns the cycle and module numbers as integers.

    **Parameters:**
    - `id_mod` (str): The identifier string containing the cycle and module numbers.

    **Returns:**
    - `ciclo` (int): The cycle number extracted from the identifier string.
    - `modulo` (int): The module number extracted from the identifier string.
    """
    
    if len(id_mod) == 3 :
        ciclo = int(id_mod[1:])
        modulo = int(id_mod[:1])
        return ciclo, modulo
    
    if len(id_mod) == 4 :
        ciclo = int(id_mod[2:])
        modulo = int(id_mod[:2])
        return ciclo, modulo

def encontrar_intervalo(fecha, intervalos):
    return next((i for i in intervalos if i['Fecha_inicio'] <= fecha <= i['Fecha_fin']), None)
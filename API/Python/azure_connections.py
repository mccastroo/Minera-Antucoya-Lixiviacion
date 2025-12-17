
import os

from azure.cosmos.cosmos_client import CosmosClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Credenciales de Cosmos
cosmos_endpoint_str = os.getenv("COSMOS_ENDPOINT_STR")
cosmos_key_str = os.getenv("COSMOS_KEY_STR")

# BD de cosmos
cosmos_database_general_str = os.getenv("COSMOS_DATABASE_GENERAL_STR")
cosmos_database_nf_str = os.getenv("COSMOS_DATABASE_NF_STR")

# Containers de Cosmos
cosmos_c_ripios_comportamiento = os.getenv("COSMOS_CONTAINER_RIPIOS_COMPORTAMIENTO_STR")
cosmos_c_ripios_inputs = os.getenv("COSMOS_CONTAINER_RIPIOS_INPUTS_STR")
cosmos_c_mcp = os.getenv("COSMOS_CONTAINER_MCP_STR")
cosmos_c_pi_system = os.getenv("COSMOS_CONTAINER_PI_SYSTEM_STR")
cosmos_c_tonelaje = os.getenv("COSMOS_CONTAINER_TONELAJE_STR")

# Credenciales de Storage
storage_account_endpoint_str = os.getenv("STORAGE_ACCOUNT_ENDPOINT_STR")
storage_account_key_str = os.getenv("STORAGE_ACCOUNT_KEY_STR")

# Containers de Storage
storage_c_datos_naturales_str = os.getenv("STORAGE_CONTAINER_DATOS_NATURALES_STR")
storage_c_ontologia_str = os.getenv("STORAGE_CONTAINER_ONTOLOGIA_STR")
azure_connection = True

while azure_connection :

    try :
        cosmos_client_connection = CosmosClient(url=cosmos_endpoint_str, credential=cosmos_key_str)
        cosmos_database_general_connection = cosmos_client_connection.get_database_client(database=cosmos_database_general_str)
        cosmos_database_nf_connection = cosmos_client_connection.get_database_client(database=cosmos_database_nf_str)
        
        cosmos_c_ripios_comportamiento_connection = cosmos_database_general_connection.get_container_client(container=cosmos_c_ripios_comportamiento)
        cosmos_c_ripios_inputs_connection = cosmos_database_general_connection.get_container_client(container=cosmos_c_ripios_inputs)
        cosmos_c_mcp_connection = cosmos_database_general_connection.get_container_client(container=cosmos_c_mcp)
        cosmos_c_pi_system_connection = cosmos_database_nf_connection.get_container_client(container=cosmos_c_pi_system)
        cosmos_c_tonelaje_connection = cosmos_database_nf_connection.get_container_client(container=cosmos_c_tonelaje)
        cosmos_c_tonelaje_general_connection = cosmos_database_general_connection.get_container_client(container=cosmos_c_tonelaje)
        
        storage_client_connection = BlobServiceClient(account_url=storage_account_endpoint_str, credential=storage_account_key_str)
        storage_c_datos_naturales_connection = storage_client_connection.get_container_client(container=storage_c_datos_naturales_str)
        storage_c_ontologia_connection = storage_client_connection.get_container_client(container=storage_c_ontologia_str)
        
        storage_c = {
            'datos-naturales' : storage_c_datos_naturales_connection,
            'ontologia' : storage_c_ontologia_connection
        }
        
        cosmos_c = {
            'pi-system' : cosmos_c_pi_system_connection,
            'tonelajes' : cosmos_c_tonelaje_connection,
            'mcp' : cosmos_c_mcp_connection,
            'tonelajes-general' : cosmos_c_tonelaje_general_connection
        }
        
        azure_connection = False
        
    except :
        continue
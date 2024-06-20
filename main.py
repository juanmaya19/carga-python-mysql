import mysql.connector
import pandas as pd
import requests
import numpy as np  # Importar numpy para manejar NaN

# URL de la API
url = 'https://www.datos.gov.co/resource/uvvq-7kbk.json'

# Realizar la solicitud GET a la API
response = requests.get(url)

# Verificar el estado de la solicitud
if response.status_code == 200:
    # Convertir la respuesta a formato JSON
    data_json = response.json()
    
    # Crear un DataFrame a partir de los datos JSON
    df = pd.DataFrame(data_json)
    
    # Limpiar NaN en el DataFrame y reemplazarlos por un valor predeterminado
    df = df.fillna(0)  # Puedes elegir otro valor en lugar de 0 según tus necesidades
    
else:
    print('Error al obtener los datos de la API:', response.status_code)

# Establecer la conexión a MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)

# Crear un cursor para ejecutar consultas SQL
cursor = mydb.cursor()

# Crear la base de datos si no existe
cursor.execute("CREATE DATABASE IF NOT EXISTS agricultura")
print("Base de datos 'agricultura' creada con éxito.")

# Seleccionar la base de datos
cursor.execute("USE agricultura")

# Generar la consulta SQL para crear la tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS agricultura (
    CodDpto INT,
    Departamento VARCHAR(255),
    CodMunicipio INT,
    Municipio VARCHAR(255),
    Cultivo VARCHAR(255),
    Periodo INT,
    `Área Sembrada (ha)` FLOAT DEFAULT 0,
    `Área Cosechada (ha)` FLOAT DEFAULT 0,
    `Producción (t)` FLOAT DEFAULT 0,
    `Rendimiento (t/ha)` FLOAT DEFAULT 0
)
"""

# Ejecutar la consulta para crear la tabla
cursor.execute(create_table_query)
print("Tabla 'agricultura' creada con éxito.")

# Insertar los datos del DataFrame en la tabla
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO agricultura (CodDpto, Departamento, CodMunicipio, Municipio, Cultivo, Periodo, `Área Sembrada (ha)`, `Área Cosechada (ha)`, `Producción (t)`, `Rendimiento (t/ha)`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(row)
    cursor.execute(insert_query, values)

# Confirmar los cambios en la base de datos
mydb.commit()
print("Datos insertados en la tabla 'agricultura' con éxito.")

# Cerrar el cursor y la conexión
cursor.close()
mydb.close()

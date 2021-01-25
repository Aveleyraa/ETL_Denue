import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///Denue_data.sqlite"



def check_if_valid_data(df: pd.DataFrame) -> bool:

    # Check if dataframe is empty
    if df.empty:
        print("No hay oxxos")
        return False 

#    # Primary Key Check
#    if pd.Series(df['Latitud']).is_unique:
#        pass
#    else:
#        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Valores nullos encontrados")

    return True    






if __name__ == "__main__":

    r = requests.get("https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarAreaActEstr/15/099/0/0/0/0/0/0/0/oxxo/1/15/0/0/0f598ede-496c-4c6e-8f0c-10cfb207e2a5")

    data = r.json()

    

    nombre = []
    ubicacion = []
    estrato= []
    Longitud = []
    Latitud = []

    # Extracting only the relevant bits of data from the json object      
    for i in data:
        nombre.append(i["Nombre"])
        ubicacion.append(i["Ubicacion"])
        estrato.append(i["Estrato"])
        Latitud.append(i["Latitud"])
        Longitud.append(i["Longitud"])
        



    oxxo_dict = {
        "nombre" : nombre,
        "ubicacion": ubicacion,
        "estrato" : estrato,
        "Latitud" : Latitud,
        "Longitud" : Longitud        
    }

    oxxo_df = pd.DataFrame(oxxo_dict, columns = ["nombre", "ubicacion", "estrato", "Latitud", "Longitud"])

    if check_if_valid_data(oxxo_df):
        print("Data valid, proceed to Load stage")



   # Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('Denue_data.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS Denue_data(
        Nombre VARCHAR(200),
        Ubicacion VARCHAR(200),
        Estrato VARCHAR(200),
        Latitud DECIMAL(200),
        Longitud DECIMAL(200)
    )
    """

    cursor.execute(sql_query)
    print("Se abrió la base de datos exitosamente")

    try:
        oxxo_df.to_sql("Denue_data", engine, index=False, if_exists='append')
    except:
        print("Datos ya existen en la base de datos")

    conn.close()
    print("Base de datos cerrada exitosamente")

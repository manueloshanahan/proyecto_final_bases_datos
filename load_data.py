# LUCÍA RAVENTÓS GONZALVO Y MANUEL O'SHANAHAN DELGADO-TARAMONA 2ºIMAT B

import configuracion as config
from pymongo import MongoClient
import json
import pymysql

def insertar_datos(client, conexion, nombre_bd_mongo, nombre_colec, nombre_bd_mysql, lista_rutas):
    
    try: 
        
        with conexion.cursor as cursor:
            client.drop_database(nombre_bd_mongo) #MongoDB nos permite borrar la base de datos, exista o no, con tal de dejarla "vacía" para volver a rellenarla (no hay que comprobar que exista)
            cursor.execute("DROP DATABASE IF EXISTS " + str(nombre_bd_mysql)) #primero la borramos por si acaso, para crearla de nuevo
            sql ="CREATE DATABASE IF NOT EXISTS " +str(nombre_bd_mysql) #evita volver a crearla si ya está creada
            cursor.execute(sql)
            
            cursor.execute(f"USE {nombre_bd_mysql}") #esta linea le dice que a partir de este momento trabajaremos con esa base de datos (como definir bd en la conexion)
            
            database = client[nombre_bd_mongo]
            coleccion = database[nombre_colec]
            
            
            sql_tabla_usuarios = f"""
                        CREATE TABLE IF NOT EXISTS usuarios (
                            reviewerID varchar(150) PRIMARY KEY,
                            reviewerName TEXT,
                        );
                        """
            cursor.execute(sql_tabla_usuarios)
            
            sql_tabla_fechas = f"""
                        CREATE TABLE IF NOT EXISTS fechas (
                            unixReviewTime BIGINT PRIMARY KEY,
                            reviewTime VARCHAR(100)
                            """
                            
            cursor.execute(sql_tabla_fechas)
            
            sql_tabla_reviews = f"""
                        CREATE TABLE IF NOT EXISTS reviews (
                            id_review INT AUTO_INCREMENT PRIMARY KEY,
                            reviewerID varchar(150),
                            asin varchar(150),
                            helpful_1 INT,
                            helpful_2 INT,
                            overall FLOAT,
                            unixReviewTime BIGINT,
                        );
                        """
            cursor.execute(sql_tabla_reviews)
            
            for ruta in lista_rutas:
                with open(ruta, "r", encoding="utf-8") as archivo:
                    for linea in archivo:
                        review_dicc = json.loads(linea)
                
                        coleccion.insert_one(review_dicc) 
                
    
                
                sql_inserciones = f"""
                    INSERT INTO {nombre_colec}(reviewerID, asin, reviewerName, helpful_1, helpful_2, reviewText, overall, summary, unixReviewTime, reviewTime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                tupla_valores = tuple()
                
                with open(ruta_fichero, 'r', encoding="UTF-8") as archivo:
                    cont = 0
                    for linea in archivo:
                        review_dicc = json.loads(linea)
                        lista_helpful = review_dicc.get('helpful', [])
                        h1 = lista_helpful[0] if len(lista_helpful) > 0 else None
                        h2 = lista_helpful[1] if len(lista_helpful) > 1 else None
                        
                        tupla_valores = (
                            review_dicc.get("reviewerID", None), 
                            review_dicc.get("asin", None),
                            review_dicc.get("reviewerName", None),
                            h1,
                            h2,
                            review_dicc.get("reviewText", None),
                            review_dicc.get("overall", None),
                            review_dicc.get("summary", None),
                            review_dicc.get("unixReviewTime", None),
                            review_dicc.get("reviewTime", None),
                        )
                        
                        cursor.execute(sql_inserciones, tupla_valores) 
                        
                        if cont >= 1000000:
                            conexion.commit()
                            cont = 0
                
                conexion.commit()
                
    except Exception as e:
        print("Ha surgido un error creando la base de datos y su tabla: ", e)




if __name__ == "__main__":
    
    lista_rutas = [config.ruta_juegos, config.ruta_juguetes, config.ruta_musica, config.ruta_instrumento]
    
    try:
        client = MongoClient(config.CONNECTION_STRING) 
        #inserta_mongodb(client, config.nombre_bd_mongo, config.coleccion_mongo, lista_ruta)
        client.close()
        
    except Exception as e:
        print("Error al conectar con MongoDB:", e)
        
    try:
        conexion_mysql = pymysql.connect(
            host=config.host, 
            user=config.user,
            password=config.password, 
        )
        #inserta_datos_mysql(conexion_mysql, nombre_bd, nombre_colecc, ruta_fichero_json)
        conexion_mysql.close()
        
    except Exception as e:
        print("Error al conectar con MySQL:", e)
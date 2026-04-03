# LUCÍA RAVENTÓS GONZALVO Y MANUEL O'SHANAHAN DELGADO-TARAMONA 2ºIMAT B

import configuracion as config
from pymongo import MongoClient
import json
import pymysql
from datetime import datetime

def insertar_datos(client, conexion, nombre_bd_mongo, nombre_colec, nombre_bd_mysql, lista_rutas):
    
    try: 
        
        with conexion.cursor() as cursor:
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
                            reviewerName TEXT
                        );
                        """
            cursor.execute(sql_tabla_usuarios)
            
            sql_tabla_fechas = f"""
                        CREATE TABLE IF NOT EXISTS fechas (
                            unixReviewTime BIGINT PRIMARY KEY,
                            reviewTime VARCHAR(100)
                            );
                            """
                            
            cursor.execute(sql_tabla_fechas)
            
            sql_tabla_reviews = f"""
                        CREATE TABLE IF NOT EXISTS reviews (
                            id_review INT NOT NULL PRIMARY KEY,
                            reviewerID varchar(150),
                            asin varchar(150),
                            helpful_1 INT,
                            helpful_2 INT,
                            overall FLOAT,
                            unixReviewTime BIGINT
                        );
                        """
            cursor.execute(sql_tabla_reviews)
                
    
                
            sql_insercion_us = f"""
                    INSERT INTO usuarios(reviewerID, reviewerName)
                    VALUES (%s, %s)
                    """
                
            sql_insercion_fecha = f"""
                    INSERT INTO fechas(unixReviewTime, reviewTime)
                    VALUES (%s, %s)
                    """
                    
            sql_insercion_reviews = f"""
                    INSERT INTO reviews(id_review, reviewerID, asin, helpful_1, helpful_2, overall, unixReviewTime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                
            id = 1
            cont = 0
            
            set_reviewers = set()
            set_unix = set()
            
            for ruta in lista_rutas:    
                with open(ruta, 'r', encoding="UTF-8") as archivo:
                    for linea in archivo:
                        review_dicc = json.loads(linea)
                        lista_helpful = review_dicc.get('helpful', [])
                        h1 = lista_helpful[0] if len(lista_helpful) > 0 else None
                        h2 = lista_helpful[1] if len(lista_helpful) > 1 else None
                        
                        reviewerID = review_dicc.get("reviewerID", None)
                        tupla_user = (reviewerID, review_dicc.get("reviewerName", None))
                        
                        unixReviewTime = review_dicc.get("unixReviewTime", None)
                        fecha_dicc = review_dicc.get("reviewTime", None)
                        fecha_mysql = None
                        if fecha_dicc:
                            try:
                                fecha_obj = datetime.strptime(fecha_dicc, '%m %d, %Y')
                                fecha_mysql = fecha_obj.strftime('%Y-%m-%d')
                                
                            except ValueError:
                                pass # Si la fecha viene mal, se quedará como None
                        
                        
                        tupla_fecha = (unixReviewTime, fecha_mysql)
                        
                        tupla_reviews = (
                            id,
                            reviewerID, 
                            review_dicc.get("asin", None),
                            h1,
                            h2,
                            review_dicc.get("overall", None),
                            unixReviewTime,
                        )
                        
                        documento = {"_id": id, "reviewText": review_dicc.get("reviewText", None), "summary": review_dicc.get("summary", None)}
                        
                        if reviewerID is not None and reviewerID not in set_reviewers:
                            cursor.execute(sql_insercion_us, tupla_user)
                            set_reviewers.add(reviewerID)
                        
                        if unixReviewTime is not None and unixReviewTime not in set_unix:
                            cursor.execute(sql_insercion_fecha, tupla_fecha)
                            set_unix.add(unixReviewTime)
        
                        cursor.execute(sql_insercion_reviews, tupla_reviews) 
 
                        coleccion.insert_one(documento) 

                        if cont >= 1000000:
                            conexion.commit()
                            cont = 0

                        id += 1
                        cont += 1
                        
                conexion.commit()
                
    except Exception as e:
        print("Ha surgido un error creando la base de datos y su tabla: ", e)


def conexion_mongo():
    return MongoClient(config.CONNECTION_STRING) 

def conexion_SQL():
    return pymysql.connect(
            host=config.host, 
            user=config.user,
            password=config.password, 
        )


if __name__ == "__main__":
    
    lista_rutas = [config.ruta_juegos, config.ruta_juguetes, config.ruta_musica, config.ruta_instrumento]
    
    try:
        client = conexion_mongo()
        conexion_mysql = conexion_SQL()
        
        insertar_datos(client, conexion_mysql, config.nombre_bd_mongo, config.coleccion_mongo, config.nombre_bd_sql, lista_rutas)
        
    except Exception as e:
        print("Error al conectar con MySQL o MongoDB:", e)
        
    finally: 
        if 'conexion_mysql' in locals() and conexion_mysql.open: #condición para que se cierren bien las conexiones en caso de que falle en el acceso
            conexion_mysql.close()
        if 'client' in locals():
            client.close()
# LUCÍA RAVENTÓS GONZALVO Y MANUEL O'SHANAHAN DELGADO-TARAMONA 2ºIMAT B

import configuracion as config
from pymongo import MongoClient
import json
import pymysql
from datetime import datetime

def insertar_datos(client, conexion, nombre_bd_mongo, nombre_colec, nombre_bd_mysql, lista_rutas):
    """
    Función con múltiples funciones; primero de todo, elimina todas las bases de datos que puedan tener el nombre de la que vamos a crear, 
    para posteriormente crear ambas bases de datos desde cero (este peso lo realiza tanto para MySQL como para MongoDB). Una vez creadas,
    para MySQL se encarga de crear 4 relaciones, una de articulos, otra de fechas, otra de usuarios registrados y otra que contiene todos los reviews.
    Por el otro lado, para MongoDB, se le define la colección reviews dentro de comercio, que contendrá también parte de estos reviews.
    Por último, la función indica qué inserciones hay que hacer en cada tabla y va recorriendo cada archivo, aislando la información según considera pertinente
    e insertando cada review en la base de datos, para ambos servidores. Además cambia el formato y gestiona errores en caso de que sea necesario.
    
    Args:
    - client: contiene la conexión-cliente perteneciente a MongoDB, que nos da la posibilidad de gestionar las colecciones y bases de datos de este servidor
    - conexion: equivale a la conexión con el servidor MySQL, que se usará para crear un objeto cursor
    - nombre_bd_mongo (str): es un string que contiene el nombre que se le otorgará a la base de datos a crear en MongoDB
    - nombre_colec (str): es un string que contiene el nombre que se le otorgará a la colección dentro de la base de datos 'nombre_bd_mongo' (en MongoDB)
    - nombre_bd_mysql (str): es un string que contiene el nombre que se le otorgará a la base de datos a crear en MySQL
    - lista_rutas (list): es una lista de tuplas, donde cada tupla está compuesta por dos strings, el primero que contiene la ruta del archivo a leer 
                          (de donde se obtienen los datos) y el segundo string contiene el nombre del archivo, es decir, la categoría de la review
    
    Returns:
    - None (ya se encarga por sí solo de realizar las modificaciones pertinentes en ambas bases de datos)
    """
    
    
    try: 
        
        with conexion.cursor() as cursor:
            client.drop_database(nombre_bd_mongo) #MongoDB nos permite borrar la base de datos, exista o no, con tal de dejarla "vacía" para volver a rellenarla (no hay que comprobar que exista)
            cursor.execute("DROP DATABASE IF EXISTS " + str(nombre_bd_mysql)) #primero la borramos por si acaso, para crearla de nuevo
            sql ="CREATE DATABASE IF NOT EXISTS " +str(nombre_bd_mysql) #evita volver a crearla si ya está creada (nunca debería ocurrir porque la borramos primero)
            cursor.execute(sql)
            
            cursor.execute(f"USE {nombre_bd_mysql}") #esta linea le dice que a partir de este momento trabajaremos con esa base de datos (como definir bd en la conexion)
            
            database = client[nombre_bd_mongo]
            coleccion = database[nombre_colec] #asilamos la colección con la que vamos a trabajar
            
            #EXPLICACIÓN DE CREACIÓN DE LAS TABLAS EN LA MEMORIA
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
                            reviewTime DATE
                            );
                            """
                            
            cursor.execute(sql_tabla_fechas)
            
            sql_tabla_articulos = f"""
                        CREATE TABLE IF NOT EXISTS articulos (
                            asin varchar(150) PRIMARY KEY,
                            categoria varchar(150)
                            );
                            """
                            
            cursor.execute(sql_tabla_articulos)
            
            sql_tabla_reviews = f"""
                        CREATE TABLE IF NOT EXISTS reviews (
                            id_review INT NOT NULL PRIMARY KEY, 
                            reviewerID varchar(150),
                            asin varchar(150),
                            helpful_1 INT,
                            helpful_2 INT,
                            overall FLOAT,
                            unixReviewTime BIGINT,
                            FOREIGN KEY (reviewerID) REFERENCES usuarios(reviewerID),
                            FOREIGN KEY (unixReviewTime) REFERENCES fechas(unixReviewTime),
                            FOREIGN KEY (asin) REFERENCES articulos(asin)
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
                    
            sql_insercion_articulos = f"""
                    INSERT INTO articulos(asin, categoria)
                    VALUES (%s, %s)
                    """
                    
            sql_insercion_reviews = f"""
                    INSERT INTO reviews(id_review, reviewerID, asin, helpful_1, helpful_2, overall, unixReviewTime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                
            id_review = 1
            cont = 0
            
            set_reviewers = set()
            set_unix = set()
            set_asin = set()
            
            for ruta, categoria in lista_rutas:    
                with open(ruta, 'r', encoding="UTF-8") as archivo:
                    for linea in archivo:
                        review_dicc = json.loads(linea)
                        lista_helpful = review_dicc.get('helpful', []) #por si acaso hacemos un manejo de errores en caso de que esté vacía la lista de hepful
                        h1 = lista_helpful[0] if len(lista_helpful) > 0 else None
                        h2 = lista_helpful[1] if len(lista_helpful) > 1 else None
                        
                        reviewerID = review_dicc.get("reviewerID", None)
                        tupla_user = (reviewerID, review_dicc.get("reviewerName", None))
                        
                        unixReviewTime = review_dicc.get("unixReviewTime", None)
                        fecha_dicc = review_dicc.get("reviewTime", None)
                        fecha_mysql = None
                        if fecha_dicc: #hacemos un cambio a fecha tal y como se nos pide
                            try:
                                fecha_obj = datetime.strptime(fecha_dicc, '%m %d, %Y')
                                fecha_mysql = fecha_obj.strftime('%Y-%m-%d')
                                
                            except ValueError:
                                pass # Si la fecha viene mal, se quedará como None
                            
                        asin = review_dicc.get("asin", None)
                        
                        
                        tupla_fecha = (unixReviewTime, fecha_mysql)
                        
                        tupla_reviews = (
                            id_review,
                            reviewerID, 
                            asin,
                            h1,
                            h2,
                            review_dicc.get("overall", None),
                            unixReviewTime,
                        )
                        
                        #también definimos el documento que enviaremos a MongoDb (explicamos en la memoria por qué guardamos la categoría)
                        documento = {"_id": id_review,
                                     "categoria": categoria,  
                                     "reviewText": review_dicc.get("reviewText", None), 
                                     "summary": review_dicc.get("summary", None)}
                        
                        if reviewerID is not None and reviewerID not in set_reviewers:
                            cursor.execute(sql_insercion_us, tupla_user)
                            set_reviewers.add(reviewerID)
                        
                        if unixReviewTime is not None and unixReviewTime not in set_unix:
                            cursor.execute(sql_insercion_fecha, tupla_fecha)
                            set_unix.add(unixReviewTime)
                        
                        if asin is not None and asin not in set_asin:
                            cursor.execute(sql_insercion_articulos, (asin, categoria))
                            set_asin.add(asin)
        
                        cursor.execute(sql_insercion_reviews, tupla_reviews) #hacemos la inserción en la tabla grande despues de haber rellenado las relaciones más pequeñas, ya que queremos que tenga un foreing key a esa relación
 
                        coleccion.insert_one(documento) #insertamos el documento en la colección de Mongo

                        cont += 1
                        if cont >= 1000000:
                            conexion.commit() #comiteamos cada 1000000 ejecuciones
                            cont = 0

                        id_review += 1
                        
                conexion.commit()
                
    except Exception as e:
        print("Ha surgido un error creando la base de datos y su tabla: ", e)


def conexion_mongo():
    """
    Función encargada de generar la conexión con nuestro MongoDB, para que acceda a esta y pueda realizar los cambios pertinentes
    
    Args:
    - None
    
    Returns:
    - cliente (MongoClient): es una conexión mediante un cliente el cual contendrá todas las bases de datos que tengamos en nuestro MongoDB
    """
    return MongoClient(config.CONNECTION_STRING) 

def conexion_SQL():
    """
    Función encargada de generar la conexión con el servidor de MySQL, utilizando los datos que se encuentran en el configuracion.py
    
    Args:
    - None
    
    Returns:
    - conexion (connect): es la conexion con el servidor de MYsql, ya con las credenciales indicadas, para su posterior uso (basta con crear un objeto cursor)
    """
    
    return pymysql.connect(
            host=config.host, 
            user=config.user,
            password=config.password, 
        )



if __name__ == "__main__":
    
    lista_rutas = [(config.ruta_juegos, "Video Games"),
                   (config.ruta_juguetes, "Toys and Games"),
                   (config.ruta_musica, "Digital Music"), 
                   (config.ruta_instrumento, "Musical Instruments")]
    
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
from load_data import conexion_db_SQL, conexion_mongo
from configuracion import ruta_alimentos, nombre_bd_mongo, coleccion_mongo, nombre_bd_sql
import json
from datetime import datetime


def obtener_ultimo_id(conexion):
    """
    Función encargada de lanzarle una consulta a MySQL e identificar el último id registrado hasta el momento en la base de datos,
    lo cual es igual a buscar el id_review más alto almacenado
    
    Args:
    - conexion: equivale a la conexión con el servidor MySQL, que se usará para crear un objeto cursor
    
    Returns:
    - resultado: devuelve el valor que se encuentra dentro de la tupla del fetchone, que esperemos que sea un int y sino será un 0
    """
    with conexion.cursor() as cursor:
        consulta = "SELECT MAX(id_review) FROM reviews"
        cursor.execute(consulta)
        resultado = cursor.fetchone() #nos quedamos solo con el uno (porque nos devolverá solo una fila)
        
        if resultado[0] is not None: #nos aseguramos de que la consulta se hay hecho bien
            return resultado[0] 
        else:
            return 0 # Si la base de datos está vacía, empezamos en 0



# aunque sea una pequeña saturación en memoria para el ordenador, y podríamos trabajar con un insert ignore, prefiero procurar evitar el cambio almacenando todas las claves ya registradas
def obtener_claves_primarias(conexion, atributo, relacion):
    """
    Función encargada de lanzarle una consulta a MySQL y obtener todos los valores del atributo en cuestión contenidos en esa tabla. Esta función está hecha
    para buscar valores de una clave primaria, ya que termina devolviendo un set con los valores distintos de esa columna de la relación. La idea es almacenar
    todos los valores de la clave primaria de una relación en un CONJUNTO para que nunca se repitan.
    
    Args:
    - conexion: equivale a la conexión con el servidor MySQL, que se usará para crear un objeto cursor
    - atributo (str): se espera el nombre del atributo a buscar en esa relación (nombre de la clave primaria)
    - relacion (str): se introduce el nombre de la relación en la que buscar esa clave primaria
    
    Returns:
    - set_reviewers (set): devuelve el conjunto de valores de la clave primaria ya rellenado en forma de set
    """
    
    with conexion.cursor() as cursor:
        consulta = f"SELECT {atributo} FROM {relacion}"
        cursor.execute(consulta)
        resultado = cursor.fetchall() 
        
        set_reviewers = set([fila[0] for fila in resultado]) #vamos a obtener todos los atributos ya registrados para que luego no haya problemas con la clave primaria al querer insertarlos
    
    return set_reviewers


def insertar_datos_nuevos(coleccion, conexion, nombre_bd_mysql, lista_rutas):
    """
    Función similar a la presente en load_data, solo que en este caso ni elimina ni crea la base de datos, simplemente se establece una base
    ya creada y desde ahí se encarga de realizar las diversas inserciones en cada una de las relaciones/colección. Se sigue la misma nomenclaruta que en load_data,
    haciendo inserciones por bloques (batch) y un commit cada tiempo. Cabe destacar que se rescatan los ids ya utilizados y las claves primarias ya almacenadas para
    que no falle el sistema.
    
    Args:
    - coleccion: directamente contiene el acceso a la coleccion de MongoDB, gracias a la conexion-cliente que se realiza desde el main
    - conexion: equivale a la conexión con el servidor MySQL, que se usará para crear un objeto cursor
    - nombre_bd_mysql (str): es un string que contiene el nombre que se le otorgará a la base de datos con la que trabajaremos
    - lista_rutas (list): es una lista de tuplas, donde cada tupla está compuesta por dos strings, el primero que contiene la ruta del archivo a leer 
                          (de donde se obtienen los datos) y el segundo string contiene el nombre del archivo, es decir, la categoría de la review    
    Returns:
        None
    """
    
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
    
    
    id_review = obtener_ultimo_id(conexion) + 1
    cont = 0
    
    # Conjuntos para evitar duplicados en la misma carga
    set_reviewers = obtener_claves_primarias(conexion, 'reviewerID', 'usuarios')
    set_unix = obtener_claves_primarias(conexion, 'unixReviewTime', 'fechas')
    set_asin = obtener_claves_primarias(conexion, 'asin', 'articulos')
    
    # Listas para el procesamiento por lotes
    datos_usuarios = []
    datos_fechas = []
    datos_articulos = []
    datos_reviews = []
    datos_coleccion = []
    
    try:
        with conexion.cursor() as cursor:
            cursor.execute(f"USE {nombre_bd_mysql}")
            
            for ruta, categoria in lista_rutas:    
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
                                    pass 
                                
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
                            
                            
                            documento = {"_id": id_review,
                                        "categoria": categoria,  
                                        "reviewText": review_dicc.get("reviewText", None), 
                                        "summary": review_dicc.get("summary", None)}
                            
                            if reviewerID is not None and reviewerID not in set_reviewers:
                                datos_usuarios.append(tupla_user)
                                set_reviewers.add(reviewerID)
                            
                            if unixReviewTime is not None and unixReviewTime not in set_unix:
                                datos_fechas.append(tupla_fecha)
                                set_unix.add(unixReviewTime)
                            
                            if asin is not None and asin not in set_asin:
                                datos_articulos.append((asin, categoria))
                                set_asin.add(asin)

                            datos_reviews.append(tupla_reviews)

                            datos_coleccion.append(documento)
                            
                            if len(datos_reviews) >= 15000:
                                cursor.executemany(sql_insercion_us, datos_usuarios)
                                cursor.executemany(sql_insercion_fecha, datos_fechas)
                                cursor.executemany(sql_insercion_articulos, datos_articulos)
                                cursor.executemany(sql_insercion_reviews, datos_reviews) #hacemos la inserción en la tabla grande despues de haber rellenado las relaciones más pequeñas, ya que queremos que tenga un foreing key a esa relación 
                                
                                coleccion.insert_many(datos_coleccion) #insertamos los documentos en la colección de Mongo
                                
                                datos_usuarios.clear()
                                datos_fechas.clear()
                                datos_articulos.clear()
                                datos_reviews.clear()
                                datos_coleccion.clear()

                            cont += 1
                            if cont >= 1000000:
                                conexion.commit() #comiteamos cada 1000000 ejecuciones
                                cont = 0

                            id_review += 1
                    
                    if len(datos_reviews) > 0:
                        cursor.executemany(sql_insercion_us, datos_usuarios)
                        cursor.executemany(sql_insercion_fecha, datos_fechas)
                        cursor.executemany(sql_insercion_articulos, datos_articulos)
                        cursor.executemany(sql_insercion_reviews, datos_reviews)
                        coleccion.insert_many(datos_coleccion)
                            
                        datos_usuarios.clear()
                        datos_fechas.clear()
                        datos_articulos.clear()
                        datos_reviews.clear()
                        datos_coleccion.clear() 
                           
                    conexion.commit()
                
    except Exception as e:
        print("Ha surgido un error creando la base de datos y su tabla: ", e) 




if __name__ == "__main__":

    try:
        client = conexion_mongo()
        conexion_mysql = conexion_db_SQL()
        
        database = client[nombre_bd_mongo]
        coleccion = database[coleccion_mongo]
        
        lista_rutas = [(ruta_alimentos, "Grocery and Gourmet Food")]
        
        insertar_datos_nuevos(coleccion, conexion_mysql, nombre_bd_sql, lista_rutas)
        
    except Exception as e:
        print("Error al conectar con MySQL o MongoDB:", e)
        
    finally: 
        if 'conexion_mysql' in locals() and conexion_mysql.open: #condición para que se cierren bien las conexiones en caso de que falle en el acceso
            conexion_mysql.close()
        if 'client' in locals():
            client.close()
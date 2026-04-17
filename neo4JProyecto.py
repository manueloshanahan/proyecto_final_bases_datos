# LUCÍA RAVENTÓS GONZALVO Y MANUEL O'SHANAHAN DELGADO-TARAMONA 2ºIMAT B

"""
NOTA: Para poder ejecutar este fichero, se debe tener la aplicación Neo4j abierta y el docker funcionando. En el caso contrario, saltará error.
"""
# Importaciones
from load_data import conexion_db_SQL, conexion_mongo
from configuracion import nombre_bd_mongo, coleccion_mongo, uri, neo4j_user, neo4j_password, num_usuarios_y_similitudes
import os
from neo4j import GraphDatabase
import time


# FUNCIONES GENERALES
def eliminar_anterior(driver):
    """
    Función que elimina todos los nodos y relaciones anteriores.
    Args:
    - Driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    with driver.session() as session: # Sesion

        consulta = """
        MATCH (n)
        DETACH DELETE n
        """
        session.run(consulta)


#Función que nos vendrá bien múltiples veces
def restriccion_nodos_u(driver):
    """
    Función que restringe que el identificador de cada usuario sea unico.

    Args:
    - Driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    with driver.session() as session: # Sesion

        # RESTRICCIÓN PARA EL USUARIO
        consulta1 = """
        CREATE CONSTRAINT unique_user IF NOT EXISTS
        FOR (user:USUARIO) REQUIRE user.id IS UNIQUE
        """
        session.run(consulta1) # Ejecutamos la consulta
    
    
# Funciones auxiliares de la primera funcionalidad
def usuarios_mas_reviews(cursor):
    """
    Función que consulta en MySQL los N usuarios con más reviews.
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - resultado (tuple): Tupla de tuplas, lo que devuelve la consulta, con el siguiente formato: (reviewerId, n_reviews)
    """
    consulta = """
    SELECT reviewerID, COUNT(id_review) as n_reviews
    FROM reviews
    GROUP BY reviewerID
    ORDER BY n_reviews DESC
    LIMIT %s;
    """
    cursor.execute(consulta, [num_usuarios_y_similitudes])
    return cursor.fetchall()


# es de las funciones más confusas, ya que nos devuelve un montón de valores que son todos necesarios para aplicar más adelante la fórmula de Pearson
def datos_formula_pearson(cursor, u, v):
    """
    Función que realiza una consulta a MySQL: busca todos los datos necesarios para la fórmula de la correlación de Pearson.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - u (str): identificador del primer usuario
    - v(str): identificador del segundo usuario
    Returns:
    - resultado (Tuple): tupla de tuplas que contiene lo que devuelve la consulta. Son múltiples valores diferentes para la fórmula
    """
    consulta = """
    SELECT r1.asin, r1.overall AS r_ui, r2.overall AS r_vi, medias.media_u, medias.media_v,
    (r1.overall - medias.media_u) * (r2.overall - medias.media_v) AS numerador_parcial,
    POW(r1.overall - medias.media_u, 2) AS denom_u_parcial,
    POW(r2.overall - medias.media_v, 2) AS denom_v_parcial
    
    FROM reviews r1
    JOIN reviews r2 ON r1.asin = r2.asin
    JOIN (SELECT AVG(r1.overall) AS media_u, AVG(r2.overall) AS media_v
          FROM reviews r1
          JOIN reviews r2 ON r1.asin = r2.asin
          WHERE r1.reviewerID = %s
          AND r2.reviewerID = %s
          ) medias
    WHERE r1.reviewerID = %s
    AND r2.reviewerID = %s;
    """
    # Devuelve todo lo que necesito para la fórmula: cada fila representa un artículo común i
    cursor.execute(consulta, [u, v, u, v])
    return cursor.fetchall()


def creacion_indice(cursor, conexion_mysql):
    """
    Función que crea un ínidice para optimizar la consulta.

    Args:
    - conexion_mysql: conexión a MySQL para modificar la base de datos
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None

    """
    consulta = """
    CREATE INDEX datos_pearson ON reviews
    (reviewerID, asin, overall);
    """
    try:
        cursor.execute(consulta)
        conexion_mysql.commit()
    except Exception as e:
        pass


def eliminar_indice(cursor, conexion_mysql):
    """
    Función que elimina el índice creado anteriormente.

    Args:
    - conexion_mysql: conexión a MySQL para modificar la base de datos
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None
    """
    # Eliminamos el índice
    consulta = """
    DROP INDEX datos_pearson ON reviews;
    """
    try:
        #apagamos un segundo las claves foráneas y sus restricciones para que no prohiba eliminar el índice (después de depurar errores y ver que fallaba)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(consulta)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conexion_mysql.commit()
    except Exception as e:
        pass

#en esta función es cuando, con los datos sacados anteriormente, aplicamos la fórmula
def similitudes_pearson(conexion_mysql, cursor, u_mas_reviews):
    """
    Función que genera una lista de tuplas, donde cada tupla contendrá (usuario A, usuario B, relación de Pearson entre ellos)
    De esta manera no hay que generar archivos externos y se puede devolver una ruta igual de válida

    Args:
    - conexion_mysql: conexión a MySQL para modificar la base de datos
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - u_mas_reviews (tuple):  tupla de tuplas con el siguiente formato: (reviewerId, n_reviews)
    Returns:
    - None
    """
        
    creacion_indice(cursor, conexion_mysql)
    
    lista_similitudes = []

    for i in range(len(u_mas_reviews)): #recorreremos cada usuario
        for j in range(i + 1, len(u_mas_reviews)): # lo compararemos con el resto de n-1 usuarios registrados
                # Para cada par de usuarios, se inicializan las variables

            u = u_mas_reviews [i][0]     # Usuario 1
            v = u_mas_reviews [j][0]     # Usuario 2
        
            resultado = datos_formula_pearson(cursor, u, v) # Datos necesarios de cada artículo en común entre u y v
            
            if not resultado: #en caso de que devuelva lista vacía, es decir, que no hay nada en común entre ellos
                continue
            
            suma_num = 0
            suma_den_u = 0
            suma_den_v = 0
            
            for fila in resultado: # Iterar cada artículo
                asin, r_ui, r_vi, media_u, media_v, numerador_parcial, denom_u_parcial, denom_v_parcial = fila # descomponer

                suma_num += float(numerador_parcial)
                suma_den_u += float(denom_u_parcial)
                suma_den_v += float(denom_v_parcial)

            if suma_den_u != 0 and suma_den_v != 0:
                pearson_uv = suma_num / ((suma_den_u ** 0.5) * (suma_den_v ** 0.5))
                    
                lista_similitudes.append((u, v, pearson_uv))
        
    eliminar_indice(cursor, conexion_mysql)

    return lista_similitudes
    

def insertar_usuarios_similitudes(driver, u_mas_reviews, lista_similitudes):
    """
    Función que inserta los usuarios como nodos y las similitudes como una relación dentro de Neo4J
    Hemos juntado todas las funciones de inserción en esta misma para que lo haga de una

    Args:
    - Driver: conexión con Neo4j que nos permite lanzarle las consultas 
    - u_mas_reviews (tuple):  tupla de tuplas (reviewerId, n_reviews)
    - lista_similitudes (list): lista de tuplas (usuario1, usuario2, pearson)
    Returns:
    - None
    """
    with driver.session() as session:
        
        # Insertamos los nodos
        consulta_nodo = "MERGE (u:USUARIO {id: $id_u})"
        
        for u, _ in u_mas_reviews:
            # Ejecutamos la consulta directamente
            session.run(consulta_nodo, id_u=u)

        # Se insertan las relaciones
        consulta_relacion = """
        MATCH (u: USUARIO {id: $id_u}), (v: USUARIO {id: $id_v})
        MERGE (u)-[:SIMILITUD {pearson: $dato_similitud}]->(v)
        MERGE (u)<-[:SIMILITUD {pearson: $dato_similitud}]-(v)
        """
        
        for u, v, similitud in lista_similitudes:
            # Ejecutamos la consulta directamente (asegurando que el dato es numérico)
            session.run(consulta_relacion, id_u=u, id_v=v, dato_similitud=float(similitud))
            

def mostrar_usuario(driver):
    """
    Función que realiza una consulta a Neo4J y muestra los usuarios con el número mayor de vecinos.
    Directamente el desempate (en caso de que lo haya) lo realiza Neo4J y devuelve solo aquellos con mayor nº de vecinos
    
    Args:
    - Driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    with driver.session() as session: 
        
        consulta = """
        MATCH (u:USUARIO)-[:SIMILITUD]-(v:USUARIO)
        WITH u, COUNT(DISTINCT v) AS vecinos
        WITH MAX(vecinos) AS max_vecinos
        
        MATCH (u:USUARIO)-[:SIMILITUD]-(v:USUARIO)
        WITH u, COUNT(DISTINCT v) AS vecinos, max_vecinos
        WHERE vecinos = max_vecinos
        
        RETURN u, vecinos
        """
        
        resultado = session.run(consulta)
        res = resultado.data() 

        if not res:
            print("No hay resultados de la consulta")
            return

        print("\nUsuario(s) con el número máximo de vecinos:")
        for elem in res:
            print(f"Usuario: {elem['u']['id']} | Número de vecinos: {elem['vecinos']}")


# EN ESTA FUNCIÓN SE AGRUPA TODO LO DEL PRIMER APARTADO
def primera_funcionalidad(conexion_mysql, cursor, driver):
    """
    Función que obtiene las similitudes entre los usuarios mediante la fórmula de la correlación de Pearson
    e importa estas similitudes entre los usuarios a Neo4J.
    Además realiza una consulta: cuál es el usuario con mas vecinos.

    Args:
    - conexion_mysql: conexión a MySQL para modificar la base de datos
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - Driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    u_mas_reviews = usuarios_mas_reviews(cursor) # Consulta de los n usuarios con más reviews en MySQL

    # FÓRMULA DE LA CORRELACIÓN DE PEARSON: guardandolo todo en la lista
    lista_similitudes = similitudes_pearson(conexion_mysql, cursor, u_mas_reviews)

    # Cargar similitudes en Neo4J
    eliminar_anterior(driver) # Limpiar Neo4j
    restriccion_nodos_u(driver) # Restringir que el id de los nodos sea único

    insertar_usuarios_similitudes(driver, u_mas_reviews, lista_similitudes)

    mostrar_usuario(driver)
    time.sleep(2)



# COMIENZA APARTADO 2
# Funciones auxiliares de la segunda funcionalidad
def eleccion_usuario(cursor):
    """
    Función que da a elegir al usuario el tipo de producto y el número de articulos
    Obtiene las categorías disponibles haciendo una consulta DISTINCT a MySQL.
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None
    """
    cursor.execute("SELECT DISTINCT categoria FROM articulos WHERE categoria IS NOT NULL") #obtenemos las categorías disponibles
    # Extraemos los resultados y limpiamos la lista
    categorias_db = [fila[0] for fila in cursor.fetchall()]

    # Imprimimos todas las opciones
    print("\nTipos de artículo disponibles: " + ", ".join(categorias_db))

    categorias_lower = [cat.lower() for cat in categorias_db] #pasamos todas a minúsculas para al menos aceptar algunas palabras más del usuario

    while True:
        tipo_consulta = input("Introduzca el tipo de artículo: ").strip().lower()

        if tipo_consulta in categorias_lower:
            # Si acierta, buscamos en qué posición está y recuperamos el nombre oficial con sus mayúsculas correctas
            indice = categorias_lower.index(tipo_consulta)
            tipo = categorias_db[indice]
            break
        
        print("El tipo de artículo introducido no se encuentra en la base de datos.\n")

    while True:
        entrada = input("Introduzca el número de artículos aleatorios: ").strip()
        try:
            n_articulos = int(entrada)
            if n_articulos > 0:
                break
            print("El número debe ser mayor que 0.\n")
        except ValueError:
            print("El valor es incorrecto. Vuelva a intentarlo.\n")

    return n_articulos, tipo


def consulta(cursor, n_articulos, tipo):
    """
    Función que realiza dos consultas en MySQL: n articulos aleatorios de un tipo específico y los usuarios que han hecho reseña de cada uno.
    Esta información se almacena en una lista que mantiene en memoria
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - tipo (str): tipo de producto escogido por el usuario
    - n_articulos (int): numero de articulos aleatorios.
    Returns:
    - asins (tuple): Tupla de tuplas que contiene los identificadores de los n usuarios aleatorios.
    """

    # Escoge n articulos del tipo escogido aleatorios
    consulta_asins = """
    SELECT asin
    FROM articulos
    WHERE categoria = %s
    ORDER BY RAND()
    LIMIT %s;
    """
    cursor.execute (consulta_asins, [tipo, n_articulos])
    asins = [fila[0] for fila in cursor.fetchall()] #generamos ya una lista "limpia" como queremos

    # Busca los usuarios que han hecho reseña acerca de cada uno y almacena tambien la nota y el tiempo
    consulta_reviews = """
    SELECT r.asin, r.reviewerID, r.overall, f.reviewTime
    FROM reviews r
    INNER JOIN fechas f ON r.unixReviewTime=f.unixReviewTime
    WHERE r.asin = %s;
    """

    datos_reviews = []
    
    for asin in asins:
        cursor.execute(consulta_reviews, [asin])
        # Vamos añadiendo los resultados de cada ASIN a la lista importante
        datos_reviews.extend(cursor.fetchall()) 

    return asins, datos_reviews


def restriccion_nodos_ua(driver):
    """
    Función que restringe que el identificador de cada usuario y de cada artículo sea único.

    Args:
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    with driver.session() as session: # Sesion

        # RESTRICCIÓN PARA EL USUARIO
        consulta1 = """
        CREATE CONSTRAINT unique_user IF NOT EXISTS
        FOR (user:USUARIO) REQUIRE user.id IS UNIQUE
        """
        session.run(consulta1) # Ejecutamos la consulta

        # RESTRICCIÓN PARA EL ARTÍCULO
        consulta2 = """
        CREATE CONSTRAINT unique_product IF NOT EXISTS
        FOR (product:PRODUCTO) REQUIRE product.id IS UNIQUE
        """
        session.run(consulta2) # Ejecutamos la consulta


def cargar_articulo_usuarios(driver, asins, datos_reviews):
    """
    Función que almacena los artículos y los usuarios como nodos y los relaciona según las 
    reviews con sus propiedades correspondientes en Neo4J, todo en una sola sesión.
    
    Args:
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    - asins (list): contiene la lista de los n articulos del tipo escogido 
    - datos_reviews (list): contiene la información de los usuarios para cada producto (y el resto de valores interesantes)
    Returns:
    - None
    """
    eliminar_anterior(driver) #llamamos de nuevo a las funciones globales
    restriccion_nodos_ua(driver) 

    with driver.session() as session:
        
        # Primero insertamos los nodos de tipo producto
        consulta_producto = "MERGE (p:PRODUCTO {id: $asin_id})"
        for asin in asins:
            session.run(consulta_producto, asin_id=asin)

        # Insertamos los usuarios y las relaciones con los productos
        consulta_relacion = """
        MATCH (p:PRODUCTO {id: $asin_id})
        MERGE (u:USUARIO {id: $reviewer_id})
        MERGE (u)-[:RESEÑA {nota: $nota, tiempo: $tiempo}]->(p)
        """
        for fila in datos_reviews:
            asin, reviewer_id, nota, tiempo = fila
            # Aseguramos que la nota entre como float (número) y no como string
            session.run(consulta_relacion, asin_id=asin, reviewer_id=reviewer_id, nota=float(nota), tiempo=tiempo)


def segunda_funcionalidad(cursor, driver):
    """
    Función que obtiene N artículos distintos del mismo tipo de producto (tipo y N dado por el usuario). 
    Asimismo, obtiene los usuarios que han realizado reseñas sobre cada producto obtenido anteriormente, además de la nota y tiempo de la reseña.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - driver: conexión con Neo4j que nos permite lanzarle las consultas 
    Returns:
    - None
    """
    # Pedir n, tipo de producto al usuario
    n_articulos, tipo = eleccion_usuario(cursor)

    # Consultar artículos y usuarios en MySQL
    asins, datos_reviews = consulta(cursor, n_articulos, tipo)

    if not asins:
        print("No hay artículos para esta categoría")
        return 
    
    # Insertar en Neo4J
    cargar_articulo_usuarios(driver, asins, datos_reviews)

    print("Carga de datos concluida. Ya puedes consultar la relación de cada artículo con los usuarios que lo han comentado.")
    time.sleep(2)
    

#COMIENZA APARTADO 3
def obtener_usuarios_multicategoria(cursor, opcion):
    """
    Realiza la consulta SQL para los primeros 400 usuarios por nombre y 
    filtra en Python aquellos que tienen más de una categoría distinta.
    Le dejamos al usuario que elija si el nodo puede tener nombre nulo o no, en caso de ser nulo, ordena por ID ASC
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - opcion (int): se indica con un 1 si se quieren borrar los nulos del nombre del reviewer o 2 si se tienen en cuenta
    Returns:
    - datos_para_neo4j (list): terminamos teniendo una lista de diccionario que contiene los datos a insertar en Neo4j
    """
    # Obtenemos a los primeros 400 usuarios ordenados por nombre alfabético,
    # junto con las categorías que han consumido y cuántos artículos de cada una.
    consulta = None
    if opcion == 1: #quitando los nulos
        consulta = """
        SELECT us.reviewerID, a.categoria, COUNT(r.asin) AS cantidad
        FROM (SELECT reviewerID
              FROM usuarios
              WHERE reviewerName IS NOT NULL
              ORDER BY reviewerName ASC
              LIMIT 400) AS us
        JOIN reviews r ON us.reviewerID = r.reviewerID
        JOIN articulos a ON r.asin = a.asin
        GROUP BY us.reviewerID, a.categoria;
        """
        
    elif opcion == 2: #tiene en cuenta los nulos y en segundo lugar ordena por ID
        consulta = """
        SELECT us.reviewerID, a.categoria, COUNT(r.asin) AS cantidad
        FROM (SELECT reviewerID
              FROM usuarios
              ORDER BY reviewerName ASC, reviewerID ASC
              LIMIT 400) AS us
        JOIN reviews r ON us.reviewerID = r.reviewerID
        JOIN articulos a ON r.asin = a.asin
        GROUP BY us.reviewerID, a.categoria;
        """
        
    cursor.execute(consulta)
    resultados = cursor.fetchall()

    # Vamos mirando usuario a usuario cuántos tienen reviews en más de una categoría para posteriormente filtrarlos
    diccionario_usuarios = {}
    for reviewerID, categoria, cantidad in resultados:
        if reviewerID not in diccionario_usuarios: #si ya está el reviewerID es porque vamos a añadir una nueva categoría
            diccionario_usuarios[reviewerID] = {}
        diccionario_usuarios[reviewerID][categoria] = cantidad
    #es decir, terminamos teniendo dos diccionarios así: diccionario_usuario = {reviewerID: {categoria1: cantidad, categoria:2: cantidad}}
    

    # Filtramos para quedarnos SOLAMENTE con los que tienen > de 1 categoría almacenada
    datos_para_neo4j = []
    for usuario, categorias in diccionario_usuarios.items(): #donde categorias sabemos que es otro diccionario
        if len(categorias) > 1: 
            for categ, cant in categorias.items():
                datos_para_neo4j.append({
                    "id_u": usuario, 
                    "categoria": categ, 
                    "cantidad": cant
                })
                
    return datos_para_neo4j


def restriccion_nodos_uc(driver):
    """
    Asegura que los nodos de Categoría también sean únicos, y también vuelve a recordarle que los usuarios sean únicos
    
    Args:
    - driver: conexión con Neo4j que nos permite lanzarle las consultas 
    Returns:
    - None
    """
    with driver.session() as session:
        # RESTRICCIÓN PARA EL USUARIO de nuevo por si acaso
        consulta1 = """
        CREATE CONSTRAINT unique_user IF NOT EXISTS
        FOR (user:USUARIO) REQUIRE user.id IS UNIQUE
        """
        session.run(consulta1)

        # RESTRICCIÓN PARA LA CATEGORÍA
        consulta2 = """
        CREATE CONSTRAINT unique_category IF NOT EXISTS
        FOR (cat:CATEGORIA) REQUIRE cat.nombre IS UNIQUE
        """
        session.run(consulta2)


def cargar_categorias_neo4j(driver, datos_lote):
    """
    Recibe la lista de diccionarios filtrada y la inyecta en Neo4j de forma masiva.
    
    Args:
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    - datos_lote (list): es la lista de diccionarios que contiene todos los datos a insertar
    Returns:
    - None
    """
    eliminar_anterior(driver) # Partimos de una base de datos limpia
    restriccion_nodos_uc(driver) # Añadimos la restricción de categoría
    
    with driver.session() as session:
        consulta = """
        MERGE (u:USUARIO {id: $id_u})
        MERGE (c:CATEGORIA {nombre: $categoria})
        MERGE (u)-[:CONSUME {cantidad: $cantidad}]->(c)
        """
        
        for fila in datos_lote: #fila recordemos que es un diccionario dentro de la lista
            session.run(consulta, 
                        id_u=fila["id_u"], 
                        categoria=fila["categoria"], 
                        cantidad=fila["cantidad"])


def tercera_funcionalidad(cursor, driver, opcion):
    """
    Función que selecciona los 400 primeros usuarios ordenados por nombre, 
    filtra aquellos que han consumido más de un tipo de artículo y los carga en Neo4j.
    Además hace la distinción entre tener en cuenta el nulo como nombre o no.
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    - opcion (int): se indica con un 1 si se quieren borrar los nulos del nombre del reviewer o 2 si se tienen en cuenta 
    Returns:
    - None
    """
    datos_filtrados = obtener_usuarios_multicategoria(cursor, opcion)
    
    if not datos_filtrados:
        print("No se encontraron usuarios que cumplan las condiciones")
        return

    cargar_categorias_neo4j(driver, datos_filtrados)
    
    print("Carga de datos del apartado 4.3 concluida ")
    time.sleep(2)


# COMIENZO DEL APARTADO 4.4

def obtener_articulos_populares(cursor):
    """
    Busca los 5 ASINs con más reviews dentro del límite de 40. 
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - cursor.fetchall(): devuelve directamente el contenido recuperado
    """
    # Para no hacer dos búsquedas aisladas en MySQL, se puede incluir toda la búsqueda en una sola query
    # Idealmente se podría crear una vista que realmente crease la tabla temporal con los 5 artículos más populares
    # De igual manera, tanto con FROM como con JOIN se nos permite hacer este pequeño filtrado, y funciona genial
    consulta_asins = """
    SELECT r.reviewerID, r.asin
    FROM reviews r
    JOIN (SELECT asin
          FROM reviews
          GROUP BY asin
          HAVING COUNT(id_review) < 40
          ORDER BY COUNT(id_review) DESC, asin ASC
          LIMIT 5
         ) AS top_articulos ON r.asin = top_articulos.asin;
      """
    cursor.execute(consulta_asins)
    return cursor.fetchall()


def obtener_intersecciones_usuarios(cursor, usuarios):
    """
    Calcula cuántos artículos (en total) tienen en común cada par de usuarios del total. 
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - usuarios: lista con todos los usuarios a analizar
    
    Returns:
    - cursor.fetchall(): lista que contiene parejas de IDs y sus relaciones
    """
    if len(usuarios) < 2:
        return []

    # Para optimizar, filtramos solo los pares de usuarios que nos interesan
    formato_in = ', '.join(['%s'] * len(usuarios))
    consulta = f"""
    SELECT r1.reviewerID, r2.reviewerID, COUNT(r1.asin) as comunes
    FROM reviews r1
    JOIN reviews r2 ON r1.asin = r2.asin
    WHERE r1.reviewerID < r2.reviewerID AND r1.reviewerID IN ({formato_in})
          AND r2.reviewerID IN ({formato_in})
    GROUP BY r1.reviewerID, r2.reviewerID;
    """
    #observación interesante, le pasamos la lista dos veces porque queremos que analice dos veces los ids 
    cursor.execute(consulta, usuarios + usuarios) 
    return cursor.fetchall() #nos devolverá parejas de IDs y las relaciones que tienen


def cargar_populares_neo4j(driver, prod_usuarios, afinidades):
    """
    Carga nodos y relaciones iterando en Neo4j, y también imponiendo restricción útil a la hora de crear los nodos
    
    Args:
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    - prod_usuarios (list): lista con los ids de los reviewers y de los productos 
    - afinidades (list): lista que contiene parejas de IDs y las relaciones que tienen 
    Returns:
    - None
    """
    eliminar_anterior(driver) 
    restriccion_nodos_ua(driver) 

    with driver.session() as session:
        # Creamos Relaciones Usuario -> Producto
        query_voto = """
        MERGE (u:USUARIO {id: $id_u})
        MERGE (p:PRODUCTO {id: $id_p})
        MERGE (u)-[:PUNTUO]->(p)
        """
        for u, p in prod_usuarios:
            session.run(query_voto, id_u=u, id_p=p)

        # Creamos Relaciones Usuario <-> Usuario (Común)
        query_comun = """
        MATCH (u1:USUARIO {id: $id_u1}), (u2:USUARIO {id: $id_u2})
        MERGE (u1)-[:COMUN {articulos: $cant}]-(u2)
        """
        for u1, u2, cant in afinidades:
            session.run(query_comun, id_u1=u1, id_u2=u2, cant=int(cant)) #imponemos que la cantidad sea un valor entero


def cuarta_funcionalidad(cursor, driver):
    """
    Selecciona los 5 artículos con más reviews (pero menos de 40), 
    los carga en Neo4j con sus usuarios y calcula artículos en común entre ellos.
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - driver: conexión con Neo4j que nos permite lanzarle las consultas
    Returns:
    - None
    """
    
    # Obtengo los productos populares y sus usuarios
    productos_y_usuarios = obtener_articulos_populares(cursor)
    
    if not productos_y_usuarios:
        print("No se han encontrado datos para esta consulta.")
        return

    # Extraemos la lista de usuarios únicos para calcular afinidades (es decir, es una lista que anteriormente se convirtió en set para borrar repetidos)
    lista_usuarios = list(set([fila[0] for fila in productos_y_usuarios]))


    try: #creamos aquí el índice porque lo necesitamos para optimizar la consulta
        cursor.execute("CREATE INDEX idx_temp_comunes ON reviews(reviewerID, asin);")
        conexion_mysql.commit()
    except Exception:
        pass
    
    # Calculo los artículos en común entre esos usuarios
    relaciones_comun = obtener_intersecciones_usuarios(cursor, lista_usuarios)

    try: #eliminamos el índice como hicimos antes
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("DROP INDEX idx_temp_comunes ON reviews;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conexion_mysql.commit()
    except Exception:
        pass
    
    # Hacemos la carga en Neo4J
    cargar_populares_neo4j(driver, productos_y_usuarios, relaciones_comun)
    
    print("Carga en Neo4J para el apartado 4.4 finalizada.") 
    time.sleep(2)


if __name__ == "__main__":
    try:
    
        client = conexion_mongo()
        db = client[nombre_bd_mongo] # DataBase
        collection = db[coleccion_mongo] # Colección

        conexion_mysql = conexion_db_SQL()

        driver = GraphDatabase.driver(uri, auth=(neo4j_user, neo4j_password))
        
        activo = True

        # menú principal
        with conexion_mysql.cursor() as cursor:
            while activo:
                print("\n" + "="*60)
                print(" " * 15 + "MENÚ PRINCIPAL TERCERA PARTE")
                print("="*60)
                print("1. Obtener similitudes entre usuarios (Pearson)")
                print("2. Obtener enlaces entre usuarios y artículos")
                print("3. Usuarios que han consumido múltiples categorías")
                print("4. Artículos populares y afinidades entre usuarios")
                print("0. Salir del programa")
                print("="*60)

                opcion_menu = input("Elige una opción (0-4): ").strip()

                if opcion_menu == '0':
                    print("\nSaliendo del programa...")
                    activo = False

                elif opcion_menu == '1':
                    print("\nEjecutando Funcionalidad 1")
                    primera_funcionalidad(conexion_mysql, cursor, driver)

                elif opcion_menu == '2':
                    print("\nEjecutando Funcionalidad 2")
                    # Los inputs de categoría y N artículos ya se piden automáticamente dentro
                    segunda_funcionalidad(cursor, driver)

                elif opcion_menu == '3':
                    print("\nEjecutando Funcionalidad 3")
                    print("¿Deseas tener en cuenta a los usuarios sin nombre registrado (es decir, con NULL)?")
                    print("  1. NO (Descartar usuarios con nombre nulo)")
                    print("  2. SÍ (Tenerlos en cuenta y ordenar por ID en caso de empate)")
                    print("  0. Volver al menú de consultas generales")
                    
                    while True:
                        opcion_null = input("Elige una opción (0, 1 o 2): ").strip()
                        if opcion_null in ['1', '2']:
                            tercera_funcionalidad(cursor, driver, int(opcion_null))
                            break
                        
                        elif opcion_null == '0':
                            break
                        
                        else:
                            print("Opción incorrecta. Por favor, introduce 1 o 2.")

                elif opcion_menu == '4':
                    print("\nEjecutando Funcionalidad 4")
                    cuarta_funcionalidad(cursor, driver)

                else:
                    print("\n⚠️ Opción no válida. Por favor, introduce un número del 0 al 4.")

    except Exception as e:
        print(f"\n❌ Error crítico en la ejecución: {e}")
    
    finally: 
        print("\nCerrando conexiones a las bases de datos...")
        if 'client' in locals():
            client.close()
        if 'conexion_mysql' in locals() and conexion_mysql.open:
            conexion_mysql.close()
        if 'driver' in locals():
            driver.close()
        print("Conexiones cerradas correctamente. Programa finalizado.")
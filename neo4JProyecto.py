"""
Fichero que contiene un menú con las siguientes opciones:
    1. Obtener similitudes entre usuarios y mostrar los enlaces en Neo4J
    2. Obtener enlaces entre usuarios y artículos
    3. Obtener algunos usuarios que han visto más de un determinado tipo de artículo
    4. Artículos populares y artículos en común entre usuarios


NOTA: Para poder ejecutar este fichero, se debe tener la aplicación Neo4j abierta. En el caso contrario, saltará error.
"""
# Importaciones
from load_data import conexion_db_SQL, conexion_mongo
from configuracion import nombre_bd_mongo, coleccion_mongo
import os
from neo4j import GraphDatabase

# VARIABLES GLOBALES
RUTA = "C:/Users/manut/OneDrive/Escritorio/2º/BD/proyecto_final_bases_datos"

NOMBRE_SIMILITUDES_PEARSON = "similitudes_pearson.txt"
RUTA_SIMILITUDES_PEARSON = os.path.join(RUTA, NOMBRE_SIMILITUDES_PEARSON)

NOMBRE_ARTICULO_POR_USUARIOS = "articulo_por_usuarios.txt"
RUTA_ARTICULO_POR_USUARIOS = os.path.join(RUTA, NOMBRE_ARTICULO_POR_USUARIOS)

# primera parte: Número de usuarios con más reviews que se desea conocer
N_U_MAS_REVIEWS = 30  

# segunda parte
TIPOS_VALIDOS_PROD = {
    "video games": "Video Games",
    "Video Games": "Video Games",
    "video game": "Video Games",
    "Video Game": "Video Games",
    "videojuegos": "Video Games",
    "Videojuegos": "Video Games",

    "toys and games": "Toys and Games",
    "Toys and Games": "Toys and Games",
    "toys": "Toys and Games",
    "Toys": "Toys and Games",
    "juguetes": "Toys and Games",
    "Juguetes": "Toys and Games",

    "digital music": "Digital Music",
    "Digital Music": "Digital Music",
    "music": "Digital Music",
    "Music": "Digital Music",
    "musica digital": "Digital Music",
    "Música digital": "Digital Music",

    "musical instruments": "Musical Instruments",
    "Musical Instruments": "Musical Instruments",
    "instruments": "Musical Instruments",
    "Instruments": "Musical Instruments",
    "instrumentos musicales": "Musical Instruments",
    "Instrumentos musicales": "Musical Instruments"
}


# Neo4j
uri = "neo4j://localhost:7687"
driver =GraphDatabase.driver(uri, auth=("neo4j", "Manu6488"))

# FUNCIONES GENERALES
def eliminar_anterior():
    """
    Función que elimina todos los nodos y relaciones anteriores.
    Input:
        None
    Output:
        None
    """
    with driver.session() as session: # Sesion

        consulta = """
        MATCH (n)
        DETACH DELETE n
        """
        session.run(consulta)


# 1. Obtener similitudes entre usuarios y mostrar los enlaces en Neo4J
def primera_funcionalidad(conexion_mysql, cursor):
    """
    Función que obtiene las similitudes entre los usuarios mediante la fórmula de la correlación de Pearson
    e importa estas similitudes entre los usuarios a Neo4J.
    Además realiza una consulta: cual es el usuario con mas vecinos.

    Input:
        conexion_mysql: conexión a MySQL para modificar la base de datos
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Output:
        None

    NOTA: no he puesto ninguna condición de "si ya está creado el documento similitudes_pearson.txt" no calcular simillitudes,
    debido al simple de hecho si se cambia la N. En ese caso, hay que recalcular todas las similitudes otra vez.
    """
    u_mas_reviews = usuarios_mas_reviews(cursor) # Consulta de los n usuarios con más reviews en MySQL

    # FÓRMULA DE LA CORRELACIÓN DE PEARSON: escribiendo resultados en el fichero: similitudes_pearson
    similitudes_pearson(conexion_mysql, cursor, u_mas_reviews)

    # Cargar similitudes en Neo4J
    cargar_similitudes(u_mas_reviews)

    mostrar_usuario()
    
# Funciones auxiliares de la primera funcionalidad
def usuarios_mas_reviews(cursor):
    """
    Función que consulta en MySQL los N usuarios con más reviews.
    
    Input:
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Output:
        resultado (tuple): Tupla de tuplas, lo que devuelve la consulta, con el siguiente formato: (reviewerId, n_reviews)
    """
    consulta = """
    SELECT reviewerID, COUNT(id_review) as n_reviews
    FROM reviews
    GROUP BY reviewerID
    ORDER BY n_reviews DESC
    LIMIT %s;
    """
    cursor.execute(consulta, [N_U_MAS_REVIEWS])
    return cursor.fetchall()

def comprobacion_existencia_productos_comunes(cursor, u, v):
    """
    Función que realiza una consulta a SQL: comprueba la existencia de reseñas de los mismos productos entre dos usuarios (u y v).

    Input:
        cursor: conexión a MySQL para realizar consultas en la base de datos
        u (str): identificador del primer usuario
        v(str): identificador del segundo usuario

    Output:
        (bool): True si si que hay reseñas de productos en común (intersección), False en caso contrario.
    """
    consulta = """
    SELECT COUNT(DISTINCT r1.asin) AS comunes
    FROM reviews r1
    JOIN reviews r2 ON r1.asin = r2.asin
    WHERE r1.reviewerID = %s
    AND r2.reviewerID = %s;
    """
    cursor.execute(consulta, [u,v])
    fila = cursor.fetchone()
    return fila[0] > 0 # booleano

def datos_formula_pearson(cursor, u, v):
    """
    Función que realiza una consulta a MySQL: busca todos los datos necesarios para la fórmula de la correlación de Pearson.

    Input:
        cursor: conexión a MySQL para realizar consultas en la base de datos
        u (str): identificador del primer usuario
        v(str): identificador del segundo usuario
    Output:
        resultado (Tuple): tupla de tuplas que contiene lo que devuelve la consulta.
    """
    consulta = """
    SELECT 
    r1.asin,
    r1.overall AS r_ui,
    r2.overall AS r_vi,
    medias.media_u,
    medias.media_v,
    (r1.overall - medias.media_u) * (r2.overall - medias.media_v) AS numerador_parcial,
    POW(r1.overall - medias.media_u, 2) AS denom_u_parcial,
    POW(r2.overall - medias.media_v, 2) AS denom_v_parcial
    
    FROM reviews r1
    JOIN reviews r2 
        ON r1.asin = r2.asin
    JOIN (
        SELECT 
            AVG(r1.overall) AS media_u,
            AVG(r2.overall) AS media_v
        FROM reviews r1
        JOIN reviews r2 
            ON r1.asin = r2.asin
        WHERE r1.reviewerID = %s
        AND r2.reviewerID = %s
    ) medias
    WHERE r1.reviewerID = %s
    AND r2.reviewerID = %s;
    """
    # Devuelve todo lo que necesito para la fórmula: cada fila representa un artículo común i
    cursor.execute (consulta, [u, v, u, v])
    return cursor.fetchall()

def creacion_indice(cursor, conexion_mysql):
    """
    Función que crea un ínidice para optimizar la consulta.

    Input:
        conexion_mysql: conexión a MySQL para modificar la base de datos
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Output:
        None

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

    Input:
        conexion_mysql: conexión a MySQL para modificar la base de datos
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Output:
        None
    """
    # Eliminamos el índice
    consulta = """
    DROP INDEX datos_pearson ON reviews;
    """
    try:
        #apagamos un segundo las claves foráneas y sus restricciones para que no prohiba eliminar el índice (después de depurar errores)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(consulta)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conexion_mysql.commit()
    except Exception as e:
        pass

def similitudes_pearson(conexion_mysql, cursor, u_mas_reviews):
    """
    Función que sobre un fichero (sobre)escribe el usuario1 (u), el usuario2 (v) y su similitud calculada mediante la correlación de Pearson.

    Input:
        conexion_mysql: conexión a MySQL para modificar la base de datos
        cursor: conexión a MySQL para realizar consultas en la base de datos
        u_mas_reviews (tuple):  tupla de tuplas con el siguiente formato: (reviewerId, n_reviews)
    Output:
        None
    """
    
    with open(RUTA_SIMILITUDES_PEARSON, "w", encoding="utf-8") as f:
        f.write("Usuario1\tUsuario2\tPearson\n")
        
        creacion_indice(cursor, conexion_mysql)

        for i in range(len(u_mas_reviews)):
            for j in range(i + 1, len(u_mas_reviews)):
                # Para cadad par de usuarios, se inicializan las variables
                suma_num = 0
                suma_den_u = 0
                suma_den_v = 0

                u = u_mas_reviews [i][0]     # Usuario 1
                v = u_mas_reviews [j][0]     # Usuario 2
            
                tienen = comprobacion_existencia_productos_comunes(cursor, u, v) # ¿Tienen productos evaluados en común u y v?
                if not tienen:
                    continue

                resultado = datos_formula_pearson(cursor, u, v) # Datos necesarios de cada artículo en común entre u y v
                for fila in resultado: # Iterar cada artículo
                    asin, r_ui, r_vi, media_u, media_v, numerador_parcial, denom_u_parcial, denom_v_parcial = fila # descomponer

                    suma_num += float(numerador_parcial)
                    suma_den_u += float(denom_u_parcial)
                    suma_den_v += float(denom_v_parcial)

                if suma_den_u != 0 and suma_den_v != 0:
                    pearson_uv = suma_num / ((suma_den_u ** 0.5) * (suma_den_v ** 0.5))
                    
                    f.write(f"{u}\t{v}\t{pearson_uv}\n") # Fila por cada u y v
        
        eliminar_indice(cursor, conexion_mysql)

def cargar_similitudes(u_mas_reviews):
    """
    Función que almacena las similitudes como relaciones y los usuarios como nodos en Neo4J.

    Input:
        u_mas_reviews (tuple):  tupla de tuplas con el siguiente formato: (reviewerId, n_reviews)
    Output:
        None
    
    Si se desea cargar más nodos habría que cambiar la variable global N.
    """
    eliminar_anterior() # Limpiar Neo4j
    restriccion_nodos_u() # Restringir que el id de los nodos sea único

    insertar_usuarios_similitudes(u_mas_reviews)    # Lectura de fichero y creación de nodos + relación en Neo4j

def restriccion_nodos_u():
    """
    Función que restringe que el identificador de cada usuario sea unico.

    Input:
        None
    Output:
        None
    """
    with driver.session() as session: # Sesion

        # RESTRICCIÓN PARA EL USUARIO
        consulta1 = """
        CREATE CONSTRAINT unique_user IF NOT EXISTS
        FOR (user:USUARIO) REQUIRE user.id IS UNIQUE
        """
        session.run(consulta1) # Ejecutamos la consulta

def insertar_usuarios_similitudes(u_mas_reviews):
    """
    Función que inserta los usuarios como nodos y las similitudes como una relación.
    Para los nodos, simplemente utiliza la lista de usuarios con más reviews.
    Para las relaciones, lee el fichero con ruta: RUTA_SIMILITUDES
    e inserta sus datos en Neo4j con el siguiente formato: [usuario1] - [similitud] -> [usuario2] y [usuario1] <- [similitud] - [departamento2],
    es decir: como una relacion bidireccinal.

    Input:
        u_mas_reviews (tuple):  tupla de tuplas con el siguiente formato: (reviewerId, n_reviews)
    Output:
        None
    """
    # INSERTAR NODOS
    for u, _ in u_mas_reviews:
        creacion_usuario (u)


    # INSERTAR RELACIÓN
    with open(RUTA_SIMILITUDES_PEARSON, "r", encoding = "utf-8") as f:
        next(f) # No leer la primera línea (es el indicador de cada columna)

        for linea in f:
            u, v, similitud = linea.strip().split() # descomponemos

            creacion_similitud(u, v, similitud) # Creación del nodo usuario1 (u), usuario2 (v) y de la relacion entre ellos (similitud)

def creacion_usuario(u):
    """
    Función que crea en Neo4j, mediante una conexion, el nodo usuario u.
    Input:
        u (str): identificador del nodo a crear.
    Output:
        None
    """

    with driver.session() as session: # Sesion

        consulta = """
        MERGE (u: USUARIO {id: $id_u})
        """
            # MERGE porque si existe ya el nodo no quiero que se duplica (como un CREATE IF NOT EXISTS)
        session.run(consulta, id_u=u)

def creacion_similitud(u, v, similitud):
    """
    Función que crea en Neo4j, mediante una conexion, el nodo usuario1 (u), el nodo usuario2 (v) y la relacion entre ellos BIDIRECCIONAL(similitud).
    Input:
        None
    Output:
        None
    """

    with driver.session() as session: # Sesion

        consulta = """
        MATCH (u: USUARIO {id: $id_u}), (v: USUARIO {id: $id_v})
        MERGE (u)-[:SIMILITUD {pearson: $dato_similitud}]->(v)
        MERGE (u)<-[:SIMILITUD {pearson: $dato_similitud}]-(v)
        """
            # MERGE porque si existe ya el nodo no quiero que se duplica (como un CREATE IF NOT EXISTS)
        session.run(consulta, id_u=u, id_v= v, dato_similitud= similitud)

def mostrar_usuario():
    """
    Función que realiza una consulta a Neo4J y muestra los usuarios con el número mayor de vecinos.

    Input:
        None
    Output:
        None
    """
    with driver.session() as session: # Sesion

        consulta = """
        MATCH (u:USUARIO)-[:SIMILITUD]-(v:USUARIO)
        WITH u, COUNT(DISTINCT v) AS vecinos
        RETURN u, vecinos
        ORDER BY vecinos DESC
        """
        # Se devuelven todos en vez de uno debido a que se ha visto que existen dos usuarios con el numero de vecinos máximos.
        # De esta forma se obtienen todos y se comparan entre sí.
        
        resultado = session.run(consulta)
        res = resultado.data() # Lista de disccionarios

        if not res:
            print("No hay resultados de la consulta")
            return

        max = [res[0]]
        # Comprobar los máximos
        for i in range (1,len(res)):
            if res[i]["vecinos"] == res[0]["vecinos"]: # Si es igual que el máximo
                max.append(res[i])
            else:
                break # A partir de aquí, son distintos

        print("Usuario(s) con el número máximo de vecinos:")
        for elem in max:
            print(f"Usuario: {elem['u']['id']}, Número de vecinos: {elem['vecinos']}")


# 2. Obtener enlaces entre usuarios y artículos
def segunda_funcionalidad(cursor):
    """
    Función que obtiene N artículos distintos del mismo tipo de producto (tipo y N dado por el usuario). 
    Asimismo, obtiene los usuarios que han realizado reseñas sobre cada producto obtenido anteriormente, además de la nota y tiempo de la reseña.

    Input:
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Output:
        None
    """
    # Pedir n, tipo de producto al usuario
    n_articulos, tipo = eleccion_usuario()

    # Consultar artículos y usuarios en MySQL
    asins = consulta(cursor, n_articulos, tipo)

    # Insertar en Neo4J
    cargar_articulo_usuarios(asins)

    print("Carga de datos concluida. Ya puedes consultar la relación de cada artículo con los usuarios que lo han comentado.")


# Funciones auxiliares de la segunda funcionalidad
def eleccion_usuario():
    """
    Función que da a elegir al usuario el tipo de producto que desea visualizar el estudio de sus reviews
    y cuantos articulos aleatorios desea visualizar.

    In:
        None
    Out:
        tipo (str): tipo de producto escogido por el usuario
        n_articulos (int): numero de articulos aleatorios.
    """

    print("Tipos de artículo: " + ", ".join(TIPOS_VALIDOS_PROD.values()))

    while True:
        tipo = input("Introduzca el tipo de artículo: ").strip()
        if tipo in TIPOS_VALIDOS_PROD.keys():
            tipo = TIPOS_VALIDOS_PROD[tipo]
            break
        print("El tipo de artículo introducido es incorrecto.\n")

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
    Esta información se almacena en un fichero llamado: articulo_por_usuarios.txt.
    
    Input:
        cursor: conexión a MySQL para realizar consultas en la base de datos
        tipo (str): tipo de producto escogido por el usuario
        n_articulos (int): numero de articulos aleatorios.
    Output:
        asins (tuple): Tupla de tuplas que contiene los identificadores de los n usuarios aleatorios.
    """

    # 1. Escoger n articulos del tipo escogido aleatorios
    consulta = """
    SELECT asin
    FROM articulos
    WHERE categoria = %s
    ORDER BY RAND()
    LIMIT %s;
    """
    cursor.execute (consulta, [tipo, n_articulos])
    asins = cursor.fetchall()

    # 2. Buscar los usuarios que han hecho reseña acerca de cada uno y almacenar tambien la nota y el tiempo
    consulta = """
    SELECT r.reviewerID, r.overall, f.reviewTime
    FROM reviews r
    INNER JOIN fechas f ON r.unixReviewTime=f.unixReviewTime
    WHERE r.asin = %s;
    """

    with open(RUTA_ARTICULO_POR_USUARIOS, "w", encoding="utf-8") as f:
        f.write(f"asin\treviewerID\toverall\treviewTime\n")

        for asin in asins:
            asin = asin[0] # quitar la tupla
            cursor.execute(consulta, [asin])
            datos_asin = cursor.fetchall()

            for fila in datos_asin:
                reviewerID, overall, reviewTime = fila
                f.write(f"{asin}\t{reviewerID}\t{overall}\t{reviewTime}\n") # Fila por cada u y v

    return asins

def cargar_articulo_usuarios(asins):
    """
    Función que almacena los articulos y los usuarios como nodos y los relaciona segun las reviews con sus propiedades correspondientes en Neo4J.

    Input:
        asins (tuple): Tupla de tuplas que contiene los identificadores de los n usuarios aleatorios.
    Output:
        None
    
    Si se desea cargar más nodos habría que cambiar la variable global N.
    """

    eliminar_anterior() # Limpiar Neo4j
    restriccion_nodos_ua() # Restringir que el id de los nodos sea único

    insertar_asin_usuarios (asins)   # Creación nodos producto

def restriccion_nodos_ua():
    """
    Función que restringe que el identificador de cada usuario y de cada artículo sea único.

    Input:
        None
    Output:
        None
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

def insertar_asin_usuarios(asins):
    """
    Función que inserta los nodos producto 
    y lee el fichero de los datos para insertar las relaciones de cada producto con sus usuarios, notas y tiempo en Neo4J.

    Input:
        asins (tuple): tupla de strs (los identificadores de los artículos)
    Output:
        None
    """
    # INSERTAR NODOS
    for asin in asins:
        asin = asin[0]
        creacion_asin (asin)

    # INSERTAR USUARIOS Y REL
    with open(RUTA_ARTICULO_POR_USUARIOS, "r", encoding = "utf-8") as f:
        next(f) # No leer la primera línea (es el indicador de cada columna)

        for linea in f:
            asin, reviewerID, overall, reviewTime = linea.strip().split() # descomponemos

            creacion_usuarios_review (asin, reviewerID, overall, reviewTime)

def creacion_asin(asin):
    """
    Función que crea en Neo4j, mediante una conexion, el nodo PRODUCTO con identificador: asin.
    Input:
        asin (str): identificador del nodo a crear.
    Output:
        None
    """

    with driver.session() as session: # Sesion

        consulta = """
        MERGE (product: PRODUCTO {id: $asin_id})
        """
            # MERGE porque si existe ya el nodo no quiero que se duplica (como un CREATE IF NOT EXISTS)
        session.run(consulta, asin_id = asin)

def creacion_usuarios_review (asin, reviewerID, overall, reviewTime):
    """
    Función que crea el nodo USUARIO y la relación entre ese usuario 
    y ese producto con nota y tiempo (momento que se hizo la reseña) como propiedad.

    Input:
        asin (str): identificador del producto
        reviewerID (str): identificador del usuario que ha realizado la reseña.
        overall (int): nota asignada al producto por el usuario en la reseña
        reviewTime (str): momento en el que se hizo la reseña.
    Output:
        None
    """
    with driver.session() as session: # Sesion

        consulta = """
        MATCH (producto: PRODUCTO {id: $asin_id})
        MERGE (reviewerID: USUARIO {id: $reviewer_ID})
        MERGE (reviewerID)-[:RESEÑA {nota: $nota, tiempo: $tiempo}]->(producto)
        """
            # MERGE porque si existe ya el nodo no quiero que se duplica (como un CREATE IF NOT EXISTS)
        session.run(consulta, asin_id = asin, reviewer_ID=reviewerID, nota= overall, tiempo = reviewTime)




def obtener_usuarios_multicategoria(cursor):
    """
    Realiza la consulta SQL para los primeros 400 usuarios por nombre y 
    filtra en Python aquellos que tienen más de una categoría distinta.
    """
    # Obtenemos a los primeros 400 usuarios ordenados por nombre alfabético,
    # junto con las categorías que han consumido y cuántos artículos de cada una.
    consulta = """
    SELECT us.reviewerID, a.categoria, COUNT(r.asin) AS cantidad
    FROM (SELECT reviewerID
    FROM usuarios
    WHERE reviewerName IS NOT NULL
    ORDER BY reviewerName ASC
    LIMIT 400) AS us
    LEFT JOIN reviews r ON us.reviewerID = r.reviewerID
    LEFT JOIN articulos a ON r.asin = a.asin
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

def restriccion_nodos_uc():
    """
    Asegura que los nodos de Categoría también sean únicos, y también vuelve a recordarle que los usuarios sean únicos
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

def cargar_categorias_neo4j(datos_lote):
    """
    Recibe la lista de diccionarios filtrada y la inyecta en Neo4j de forma masiva.
    """
    eliminar_anterior() # Partimos de una base de datos limpia
    restriccion_nodos_uc() # Añadimos la restricción de categoría
    
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




def tercera_funcionalidad(cursor):
    """
    Función que selecciona los 400 primeros usuarios ordenados por nombre, 
    filtra aquellos que han consumido más de un tipo de artículo y los carga en Neo4j.
    """
    datos_filtrados = obtener_usuarios_multicategoria(cursor)
    
    if not datos_filtrados:
        print("No se encontraron usuarios que cumplan las condiciones")
        return

    cargar_categorias_neo4j(datos_filtrados)
    
    print("Carga de datos del apartado 4.3 concluida ")




# APARTADO 4.4

def obtener_articulos_populares(cursor):
    """
    SQL: Busca los 5 ASINs con más reviews dentro del límite de 40. 
    """
    # Para no hacer dos búsquedas aisladas en MySQL, se puede incluir toda la búsqueda en una sola query
    # Idealmente se podría crear una vista que realmente crease la tabla temporal con los 5 artículos más populares
    # De igual manera, tanto con FROM como con JOIN se nos permite hacer este peqieño filtrado, y funciona genial
    consulta_asins = """
    SELECT r.reviewerID, r.asin
    FROM reviews r
    JOIN (SELECT asin
          FROM reviews
          GROUP BY asin
          HAVING COUNT(id_review) < 40
          ORDER BY COUNT(id_review) DESC
          LIMIT 5
         ) AS top_articulos ON r.asin = top_articulos.asin;
      """
    cursor.execute(consulta_asins)
    return cursor.fetchall()


def obtener_intersecciones_usuarios(cursor, usuarios):
    """
    SQL: Calcula cuántos artículos (en total) tienen en común cada par de usuarios del pool. 
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
    cursor.execute(consulta, usuarios + usuarios) 
    return cursor.fetchall() #nos devolverá parejas de IDs y las relaciones que tienen


def cargar_populares_neo4j(prod_usuarios, afinidades):
    """
    Carga nodos y relaciones iterando en Python (sin UNWIND).
    """
    eliminar_anterior() 
    restriccion_nodos_ua() 

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
            session.run(query_comun, id_u1=u1, id_u2=u2, cant=cant)


def cuarta_funcionalidad(cursor):
    """
    Selecciona los 5 artículos con más reviews (pero menos de 40), 
    los carga en Neo4j con sus usuarios y calcula artículos en común entre ellos.
    """
    
    # Obtengo los productos populares y sus usuarios
    productos_y_usuarios = obtener_articulos_populares(cursor)
    
    if not productos_y_usuarios:
        print("No se han encontrado datos para esta consulta.")
        return

    # Extraemos la lista de usuarios únicos para calcular afinidades
    conjunto_usuarios = set()
    for fila in productos_y_usuarios:
        usuario = fila[0]
        conjunto_usuarios.add(usuario)

    lista_usuarios = list(conjunto_usuarios)

    # Calculo los artículos en común entre esos usuarios
    relaciones_comun = obtener_intersecciones_usuarios(cursor, lista_usuarios)

    # Hacemos la carga en Neo4J
    cargar_populares_neo4j(productos_y_usuarios, relaciones_comun)
    
    # Mensaje de finalización según el enunciado
    print("Carga en Neo4J para el apartado 4.4 finalizada.") 


if __name__ == "__main__":
    try:
        client = conexion_mongo()
        db = client[nombre_bd_mongo] # DataBase
        collection = db[coleccion_mongo] # Colección

        conexion_mysql = conexion_db_SQL()


        with conexion_mysql.cursor() as cursor:
            #primera_funcionalidad(conexion_mysql, cursor) # 1
            #segunda_funcionalidad(cursor)
            #tercera_funcionalidad(cursor)
            cuarta_funcionalidad(cursor)


    except Exception as e:
        print("Error:", e)
    
    finally: 
        # Cerrar conexiones
        if 'client' in locals():
            client.close()
        if 'conexion_mysql' in locals():
            conexion_mysql.close()
# LUCÍA RAVENTÓS GONZALVO Y MANUEL O'SHANAHAN DELGADO-TARAMONA 2ºIMAT B

"""
Fichero que crea una aplicación mediante la librería Streamlit que permite al usuario 
elegir distintas visualizaciones y que el programa no termine hasta que elija "salir" (basta con cerrar la pestaña).


Ejecución por terminal:      streamlit run menu_visualizacion.py                ó
python -m streamlit run menu_visualizacion.py
"""
from load_data import conexion_db_SQL, conexion_mongo
from configuracion import nombre_bd_mongo, coleccion_mongo
import streamlit as st
import matplotlib.pyplot as plt
import re
from collections import Counter
from wordcloud import WordCloud
import numpy as np
import pandas as pd
import os #las usaremos para matar el poceso 
import signal


# Configuración general de la página de streamlit
st.set_page_config(page_title="Menú de visualización", layout="wide")

# VARIABLES GLOBALES
dict_frecuencias = {}


# FUNCIONES DE STREAMLIT Y/O QUE SE USARÁN MÁS DE UNA VEZ EN ESTE FICHERO
def mostrar_inicio():
    """
    Función que muestra la información en la pantalla inicial de la aplicación en streamlit.
    Args:
        None
    Returns:
        None
    """
    st.title("Aplicación Python para el acceso y visualización de los datos.📊 🔎 📈 📉")
    st.write("Esta aplicación ha sido desarrollada por Manuel O'Shanahan Delgado-Taramona y Lucía Raventós Gonzalvo.")
    st.write("En esta aplicación se pueden visualizar distintas gráficas relacionadas con los datos de reviews")
    st.write("Para ello, basta con seleccionar la visualización que se desea ver en el menú lateral.")
    
    
def eleccion_tipo():
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación 
    y da a elegir al usuario el tipo de producto, o incluso todos, que desea visualizar el estudio de sus reviews.

    Args:
        None
    Returns:
    - opcion (str): tipo de producto escogido por el usuario
    """
    # Pestaña que da a elegir
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments", "TODO"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida     
    return opcion


#función similar a la anterior pero en esta no se incluye la opción de TODO
def eleccion_tipo_sin_todo():
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación 
    y da a elegir al usuario el tipo de producto que desea visualizar el estudio de sus reviews.

    Args:
    - None
    
    Returns:
    - opcion (str): tipo de producto escogido por el usuario
    """
    # Pestaña que da a elegir
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida     
    return opcion


def eleccion_tipo_comp():
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación y da a elegir al usuario 
    el tipo de producto o un artículo en concreto cuyo estudio de sus reviews desea visualizar.

    Args:
        None
    Returns:
    - opcion (str): tipo de producto escogido por el usuario
    """
    # Elección
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments", "TODO", "Artículo individual"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida        
    
    return opcion


def intro_codigo_articulo(cursor):
    """
    Función que abre una pestaña y pide al usuario que introduzca el código del artículo que desea visualizar.
    Comprueba que el artículo existe y si no es así, se le informa al usuario y se le da la opción de elegir otro código o incluso otro tipo de visualización.
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - texto (str): Input del usuario con el código del artículo que desea visualizar, o None si no se ha introducido nada o el artículo no existe.
    """
    
    texto = None # Inicializamos variable
    texto = st.text_input("Escribe el identificador del artículo cuyas notas deseas consultar:").strip() # Pedir el identificador de artículo (asin)
    
    if texto: # cuando ya se ha introducido el código del artículo
        # Comprobación de existencia
        consulta = """
        SELECT asin 
        FROM reviews 
        WHERE asin = %s 
        LIMIT 1;
        """
        cursor.execute(consulta, [texto])
        resultado = cursor.fetchone()

        if not resultado:
            st.write("El artículo no existe")
            st.write("Elige otra opción o otro código de artículo válido.")
            texto = None # actualizamos variable

    return texto


def comprobacion_notas(notas, recuentos):
    """
    Función que comprueba que en las notas obtenidas en la consulta están todas las notas posibles (1, 2, 3, 4 y 5) 
    y si no es así, les asigna un recuento de 0 para que se muestren correctamente en el histograma.
    En principio es una función que no debería ocurrir ya que queremos suponer que todas las notas serán un valor númerico entre 1 y 5

    Args:
    - notas (tuple): tupla con las notas obtenidas en la consulta
    - recuentos (tuple): tupla con los recuentos de cada nota obtenidos en la consulta
    Returns:
    - notas (tuple): tupla con las notas, incluyendo las que no estaban en la consulta con recuento 0
    - recuentos (tuple): tupla con los recuentos de cada nota, incluyendo 0 para las notas que no estaban en la consulta
    """
    # Comprobacion para que estén todas las notas posibles [1-5]
    if len(notas) != 5:
        diccionario = dict(zip(notas, recuentos))
        diccionario = {i: diccionario.get(i, 0) for i in range(1, 6)} # 0 porque al no estar en la lista de notas significa que tiene asociadas 0 reseñas

        notas, recuentos = zip(*diccionario.items()) #usamos la función zip para volver a generar el diccionario
    return notas, recuentos


def comprobacion_existencia_usuarios(cursor, ids):
    """
    Función que comprueba que los usuarios introducidos por el usuario existen en la base de datos 
    y si no es así, se le informa al usuario y se elimina de la lista de ids a analizar.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - ids (list): lista con los identificadores de los usuarios que se quieren analizar
    Returns:
    - ids (list): lista actualizada con los identificadores de los usuarios que sí existen en la base de datos
    """
    ids_validos = [] # para almacenar los ids que sí existen y evitar errores de consulta con ids no existentes
    # Comprobación de existencia
    for id in ids:
        if id in ids_validos: # Si se ha introducido un id repetido, se evita
            continue

        consulta = "SELECT reviewerID FROM usuarios WHERE reviewerID = %s LIMIT 1;"
        cursor.execute(consulta, [id])
        resultado = cursor.fetchone()
        

        if resultado:
            ids_validos.append(id)
        else: # Si el usuario no existe, se le informa y se elimina de la lista de ids a analizar
            st.write(f"El usuario introducido: '{id}' no existe.")
    
    ids = ids_validos # actualizamos variable con los ids que sí existen
    return ids


def salida():
    """
    Función que muestra un mensaje de despedida al usuario y le informa de cómo cerrar la aplicación.
    Args:
    - None
    Returns:
    - None
    """
    st.warning("Puedes cerrar esta pestaña del navegador.")
    st.info("Para detener completamente la aplicación, pulsa Ctrl+C en la terminal o pulse el siguiente botón")
    
    if st.button("APAGAR LA CONEXIÓN"):
        os.kill(os.getpid(), signal.SIGINT) #no es la opción más óptima, pero de esta manera podemos matar el proceso (COMO VIMOS EN EL TEMARIO DE FUSO)


# GRÁFICO 1
def evolucion_por_años(cursor):
    """
    Función que muestra la evolucion del número de reviews por año. 
    Se da la opción a escoger el tipo de producto.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None
    """
    st.title("Evolución del número de reviews por años")

    opcion = eleccion_tipo() # Elección del usuario 

    # Consulta en MySQL (diferenciamos si le importa la categoría o no con el WHERE)
    if opcion == "TODO":
        consulta = """
        SELECT YEAR(f.reviewTime) as año, COUNT(r.id_review) as n_reviews
        FROM reviews r
        INNER JOIN fechas f ON f.unixReviewTime=r.unixReviewTime
        GROUP BY YEAR(f.reviewTime) 
        ORDER BY año ASC;
        """                 
        cursor.execute(consulta)

    else:
        consulta = """
        SELECT YEAR(f.reviewTime) as año, COUNT(r.id_review) as n_reviews
        FROM reviews r
        INNER JOIN articulos a ON r.asin=a.asin
        INNER JOIN fechas f ON f.unixReviewTime=r.unixReviewTime
        WHERE a.categoria = %s
        GROUP BY YEAR(f.reviewTime) 
        ORDER BY año ASC;
        """                 
        cursor.execute(consulta, [opcion])
    
    resultado = cursor.fetchall() 
    # Para evitar errores si resultado está vacío
    if not resultado:
        st.write ("No se ha recuperado ningún resultado en la consulta.")
        return
    
    años, datos = zip(*resultado) # desempaquetamos el cursor en dos listas diferentes
    
    fig, ax = plt.subplots()
    ax.bar(años, datos) # Histograma
    
    # Diseño
    if opcion == "TODO":
        ax.set_title("Reviews por años de todos los productos")
    else:
        ax.set_title(f"Reviews por años de los productos con categoría: {opcion}")

    ax.set_xlabel("Año")
    ax.set_ylabel("Número de reviews")

    ax.set_xticks(años)
    ax.set_xticklabels(años, rotation=45)

    st.pyplot(fig) # Mostrar figura dentro de streamlit


# GRÁFICO 2
def evolucion_popularidad(cursor):
    """
    Función que muestra la popularidad en orden descendiente de los articulos. 
    Se da la opción a escoger el tipo de producto.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos

    Returns:
    - None
    """
    st.title("Evolución de la popularidad de los artículos.")
    st.write("La popularidad de un artículo está representada por el número de reviews de dicho artículo.")

    opcion = eleccion_tipo() # dar la opción al usuario de elegir que tipo desea de producto o todos

    # Consulta en MySQL
    if opcion == "TODO":
        consulta = """
        SELECT asin as id_articulo, COUNT(id_review) as recuento
        FROM reviews
        GROUP BY asin
        ORDER BY recuento DESC;
        """                 # sin condición de tipo de review
        cursor.execute(consulta)

    else:
        consulta = """
        SELECT r.asin as id_articulo, COUNT(r.id_review) as recuento
        FROM reviews r
        INNER JOIN articulos a ON r.asin=a.asin
        WHERE a.categoria=%s
        GROUP BY r.asin
        ORDER BY recuento DESC;
        """                 # con condición WHERE para el tipo
        cursor.execute(consulta, [opcion])
    
    resultado = cursor.fetchall() # Tupla de tuplas
    # Para evitar errores si resultado está vacío
    if not resultado:
        st.write ("No se ha recuperado ningún resultado en la consulta.")
        return
     
    asin_ids, recuento = zip(*resultado) # desempaquetar

    # Transformar los ids de los artículos por un número para contarlos
    asin = list(range(len(asin_ids)))
    
        
    fig, ax = plt.subplots()
    ax.plot(asin, recuento) # Función continua
    
    # Diseño
    if opcion == "TODO":
        ax.set_title("Evolución de la popularidad de todos los productos.")
    else:
        ax.set_title(f"Evolución de la popularidad de los productos con categoría: {opcion}")

    ax.set_xlabel("Artículos")
    ax.set_ylabel("Popularidad")

    st.pyplot(fig) # Mostrar figura dentro de streamlit


#GRÁFICO 3
def histograma_nota(cursor):
    """
    Función que muestra un histograma con las notas que ha(n) obtenido todos los productos, los productos de una cierta clasificación
    o incluso un único producto concreto.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos

    Returns:
    - None
    """
    st.title("Histograma de las notas en función del número de reviews en cierto(s) producto(s).")

    opcion = eleccion_tipo_comp() # Elección del tipo de producto o artículo concreto

    # Consulta en MySQL
    if opcion == "TODO":
        consulta = """
        SELECT overall, COUNT(id_review)
        FROM reviews
        GROUP BY overall
        ORDER BY overalL ASC;
        """                 
        cursor.execute(consulta)

    elif opcion == "Artículo individual":
        texto = intro_codigo_articulo(cursor) # Elección del artículo concreto, si se ha escogido esa opción
        if texto:                             # si se ha introducido un código de artículo válido, para evitar errores de consulta con texto vacío
            consulta = """
            SELECT overall, COUNT(id_review) as recuento
            FROM reviews
            WHERE asin=%s
            GROUP BY overall
            ORDER BY overall ASC;
            """                 
            cursor.execute(consulta, [texto])
        else: 
            return #cuando se introduzca algo
    
    else: # los demás tipos
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews r
        INNER JOIN articulos a ON a.asin=r.asin
        WHERE a.categoria=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 
        cursor.execute(consulta, [opcion])
    
    resultado = cursor.fetchall() 
    # Para evitar errores si resultado está vacío
    if not resultado:
        st.write ("No se ha recuperado ningún resultado en la consulta.")
        return
    
    notas, recuentos = zip(*resultado) 

    # Comprobacion para que estén todas las notas posibles [1-5]
    notas, recuentos = comprobacion_notas(notas, recuentos)
    
        
    fig, ax = plt.subplots()
    ax.bar(notas, recuentos) # Histograma
    
    # Diseño
    if opcion == "TODO":
        ax.set_title("Histograma de las notas de todos los artículos.")
    elif opcion == "Artículo individual" and texto:
        ax.set_title(f"Histograma de las notas del artículo con identificador: {texto}.")
    elif opcion != "Artículo individual":
        ax.set_title(f"Histograma de las notas de los artículos clasificados como: {opcion}")

    ax.set_xlabel("Notas")
    ax.set_ylabel("Número de reviews")

    st.pyplot(fig) # Mostrar figura dentro de streamlit


#GRÁFICO 4
def evolucion_tiempo_categorias(cursor):
    """
    Función que muestra la evolución acumulada del número de reviews a lo largo del tiempo
    para todas las categorías de producto.

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos

    Returns:
    - None
    """
    st.title("Evolución del número de reviews a lo largo del tiempo")

    consulta = """
    SELECT a.categoria, r.unixReviewTime, COUNT(*) AS n_reviews
    FROM reviews r
    INNER JOIN articulos a ON a.asin = r.asin
    GROUP BY a.categoria, r.unixReviewTime
    ORDER BY a.categoria ASC, r.unixReviewTime ASC;
    """
    cursor.execute(consulta)
    resultado = cursor.fetchall()

    if not resultado:
        st.write("No se ha recuperado ningún resultado en la consulta.")
        return

    # Agrupar por categoría (vamos rellenando el diccionario que contiene el momento y el recuento para los momentos)
    datos_por_categoria = {}
    for categoria, tiempo, n_reviews in resultado:
        if categoria not in datos_por_categoria:
            datos_por_categoria[categoria] = {"tiempos": [], "recuentos": []}

        datos_por_categoria[categoria]["tiempos"].append(tiempo)
        datos_por_categoria[categoria]["recuentos"].append(n_reviews)

    fig, ax = plt.subplots(figsize=(10, 6))

    for categoria, datos in datos_por_categoria.items():
        tiempos = datos["tiempos"]
        recuentos = datos["recuentos"]

        acumulado = []
        suma = 0
        for r in recuentos: #aquí se hace el cálculo acumulado, para que se vaya sumando
            suma += r
            acumulado.append(suma)

        ax.plot(tiempos, acumulado, label=categoria)

    ax.set_title("Evolución del número de reviews a lo largo del tiempo")
    ax.set_xlabel("Tiempo (Unix Review Time)")
    ax.set_ylabel("Número de reviews")
    ax.grid(True)
    ax.legend()

    st.pyplot(fig)


#GRÁFICO 5
def histograma_usuario(cursor):
    """
    Función que muestra un histograma que representa cuantos usuarios han publicado una cantidad de reviews. 

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None
    """
    st.title("Histograma que representa cuántos usuarios han publicado una cantidad de reviews.")

    # Consulta en MySQL
    consulta = """
    SELECT n_reviews, COUNT(reviewerID) AS n_usuarios
    FROM (SELECT reviewerID, COUNT(id_review) AS n_reviews  -- cada usuario ha hecho n_reviews
          FROM reviews
          GROUP BY reviewerID) AS t
    GROUP BY n_reviews
    ORDER BY n_reviews ASC;
    """
    cursor.execute(consulta)
    
    resultado = cursor.fetchall() # Tupla de tuplas

    # Para evitar errores si resultado está vacío
    if not resultado:
        st.write ("No se ha recuperado ningún resultado en la consulta.")
        return
    
    n_reviews, n_usuarios = zip(*resultado) 
    
    st.subheader("Visualización en Escala Normal")
    st.write("En la escala estándar, la concentración de usuarios que solo ha publicado 1 o 2 reviews crea una diferencia gigante que 'aplasta' visualmente al resto de usuarios con mayor actividad.")

    fig, ax = plt.subplots()
    ax.bar(n_reviews, n_usuarios) # Histograma
    
    # Diseño
    ax.set_title("Histograma de reviews por usuario (Escala Normal)")

    ax.set_xlabel("Número de reviews")
    ax.set_ylabel("Número de usuarios con esa cantidad de reviews")

    st.pyplot(fig) # Mostrar figura dentro de streamlit
    
    #TAMBIÉN QUEREMOS REPRESENTAR EL MISMO GRÁFICA EN ESCALA LOGARÍTMICA PARA QUE SE IDENTIFIQUE MEJOR:
    
    st.markdown("---") # Creamos una línea horizontal de separación
    st.subheader("Visualización en Escala Logarítmica")
    st.write("Se pueden identificar de manera aislada la cantidad de reviewers que han hecho más de 200 reviews:")
    
    ax.set_yscale('log') #AQUÍ INDICAMOS EL NUEVO EJE
    ax.set_title("Histograma de reviews por usuario (Escala Logarítmica)")
    ax.set_ylabel("Número de usuarios con esa cantidad de reviews (Log)")
    st.pyplot(fig)


#esta función se utilizará para el wordcloud (es de las más complejas)
def summaries_tipos(collection, opcion):
    """
    Función que estudia la frecuencia que tiene cada palabra de todos los summaries de un tipo determinado, las consultas las hará mediante MongoDB

    Args:
    - collection (Object): conexión a MySQL para realizar consultas en la base de datos
    
    Returns:
    - frecuencias (dict): diccionario que contiene palabra y su frecuencia en todos los summaries de ese tipo de producto.
    """
    resultados = collection.find({"categoria": opcion}, {"_id": 0, "summary": 1}) #filtramos para quedarnos sólo con los summaries
    summaries = [doc["summary"] for doc in resultados if doc.get("summary")] # extraer unicamente el valor del campo, olvidarme del nombre "summary"

    if not summaries: # condicion de seguridad
        st.write("No hay summaries para la categoría seleccionada.")
        return
    
    texto = " ".join(summaries).lower() # Unir todos los summaries en un único texto y asi solo hacer un regex

    palabras = re.findall(r"[a-záéíóúñ]+", texto) # Extraer palabras mediante regex
    palabras_validas = [p for p in palabras if len(p) > 3] # cumplir condicion de longitud mayor que tres

    if not palabras_validas: # condicion de seguridad
        st.write("No hay palabras válidas para generar la nube.")
        return

    frecuencias = Counter(palabras_validas) # Devuelve la frecuencia de cada palabra ordenadas de manera descendente

    return frecuencias


#GRÁFICO 6
def nube_palabras(collection):
    """
    Función que muestra una nube de palabras según el tipo de productos elegido por el usuario. 
    Esta cosulta la usa observando los datos almacenados en MongoDB, ya que en este se encuentrar los strings
    
    Args:
    - collection: conexión a Mongo para realizar consultas en la base de datos

    Returns:
    - None
    """
    global dict_frecuencias # para poder actualizar el diccionario definido al principio

    st.title("Nube de palabras")
    st.write("Visualización usando Wordcloud.")

    opcion = eleccion_tipo_sin_todo()

    # Consulta en Mongo
    if opcion in dict_frecuencias:                    # Ya ha sido estudiado
        frecuencias = dict_frecuencias[opcion]
    else:                                             # Se evita para optimizar el rendimiento
        frecuencias = summaries_tipos(collection, opcion)
        dict_frecuencias[opcion] = frecuencias # Almacenar para la próxima vez

    if not frecuencias: # Ha saltado alguna condición de seguridad
        return

    # Creamos la nube
    nube = WordCloud(width=800, height=400, background_color="white")
    nube.generate_from_frequencies(frecuencias) # se define la nube y se ajusta a las frecuencias almacenadas

    # Mostrarl la figura en streamlit
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(nube, interpolation="bilinear")
    ax.axis("off")

    st.pyplot(fig)
    

# AQUÍ COMIENZAN TODAS LAS FUNCIONES NECESARIAS PARA NUESTRO ADICIONAL (GRÁFICOS AÑADIDOS)
def consultas_notas_usuarios(cursor, ids):
    """
    Función que realiza la consulta en MySQL para obtener las notas puestas por un usuario o varios usuarios (nuestra consulta extra)
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - ids (list): lista con los identificadores de los usuarios que se quieren analizar
    
    Returns:
        None
    """
     # Caso base
    if len(ids) == 0:
        st.write("No se ha introducido ningún usuario válido.")
        return
    elif len(ids) == 1: # Solo un usuario, se muestra su histograma
        id = ids[0]
    
        # Consulta en MySQL
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews
        WHERE reviewerID=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 # con condición WHERE para el código del árticulo
        cursor.execute(consulta, [id])
    
    
    else: # Varios usuarios, se muestra un histograma con la media de las notas puestas por cada usuario

        placeholders = ", ".join(["%s"] * len(ids)) # Se crean tantos placeholders como ids haya. Tipo: ["%s", "%s", "%s"] para len = 3
        consulta = f"""
        SELECT overall, AVG(n_reviews) as media_recuento
        FROM   (
            SELECT reviewerID, overall, COUNT(id_review) as n_reviews
            FROM reviews
            WHERE reviewerID IN ({placeholders})
            GROUP BY reviewerID, overall) t
        GROUP BY overall
        ORDER BY overall;
        """
        cursor.execute(consulta, ids)
# si observamos no hay un return como tal para no cargar la RAM, lo que haremos será realizar el fetchall desde la propia función que llama a esta
    

# FUNCIÓN QUE SE USARÁ PARA MOSTRAR NUESTRAS GRÁFICAS PERSONALES (EL ADICIONAL)
def metricas(ids, notas, recuentos):
    """
    Función que calcula y muestra las métricas individuales de un usuario o varios usuarios, 
    incluyendo el total de reviews, la nota media y la nota más frecuente.

    Args:
    - ids (list): lista de identificadores de usuarios analizados
    - notas (tuple): tupla con las notas puestas por el usuario o usuarios, incluyendo las que no estaban en la consulta con recuento 0
    - recuentos (tuple): tupla con los recuentos de cada nota puesta por el usuario o usuarios, incluyendo 0 para las notas que no estaban en la consulta
    Returns:
    - total_reviews (int): número total de reviews del usuario o usuarios
    - media (float): nota media del usuario o usuarios
    - moda (int): nota más frecuente del usuario o usuarios
    """
    
    total_reviews = sum(recuentos)
    media = sum(n*r for n, r in zip(notas, recuentos)) / total_reviews if total_reviews > 0 else 0
    moda = max(zip(notas, recuentos), key=lambda x: x[1])[0] if recuentos else None

    st.subheader(f"Usuario: {ids[0]}" if len(ids) == 1 else f"Usuarios: {', '.join(ids)}") #mostramos los usuarios

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total de reviews", total_reviews)
    with col2:
        st.metric("Nota media", f"{media:.2f}")
    with col3:
        st.metric("Nota más frecuente", moda)

    return total_reviews, media, moda #también devolvemos los valores para mostrarlos gráficamente


#Aquí empezamos a representar los plots de nuestra función adicional
def impresion_histograma_notas_usuario(notas, recuentos, ids):
    """
    Función que muestra un histograma con la distribución de las notas puestas por un usuario o varios usuarios.
    En el caso de varios usuarios, se muestra la media de las notas puestas por cada usuario.
    Args:
    - notas (tuple): tupla con las notas. 
    - recuentos (tuple): tupla con los recuentos de cada nota.
    - ids (list): lista de identificadores de usuarios analizados
    Returns:
    - None
    """
    fig, ax = plt.subplots()
    ax.bar(notas, recuentos) # Definimos las barras del histograma
    
    # Diseño
    if len(ids) == 1:
        ax.set_title(f"Histograma de las notas puestas por el usuario con identificador: {ids[0]}.")
    else:
        ax.set_title(f"Histograma de las notas puestas por los usuarios con identificadores: {', '.join(ids)}.")
        st.write("Se muestra la media de las notas puestas por cada usuario.")

    ax.set_xlabel("Notas")
    ax.set_ylabel("Número de reviews")

    st.pyplot(fig) # Mostrar figura dentro de streamlit


def comportamiento_por_usuario(cursor, id):
    """
    Función que muestra el comportamiento de un usuario concreto, 
    incluyendo un histograma con la distribución de las notas que ha puesto en sus reviews y sus métricas individuales (total de reviews, nota media y nota más frecuente).
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    - id (str): identificador del usuario que se quiere analizar
    Returns:
    - notas_i (tuple): tupla con las notas puestas por el usuario, incluyendo las que no estaban en la consulta con recuento 0
    - recuentos_i (tuple): tupla con los recuentos de cada nota puesta por el usuario, incluyendo 0 para las notas que no estaban en la consulta
    - total_reviews (int): número total de reviews del usuario
    - media (float): nota media del usuario
    - moda (int): nota más frecuente del usuario
    """
    consultas_notas_usuarios(cursor, [id])
    resultado_individual = cursor.fetchall() # Resultado de cada usuario (AQUÍ SE RECUPERA EL RESULTADO DEL EXECUTE SIN RETURN ANTERIORMENTE NOMBRADO)

    if not resultado_individual: # error, no se ha recuperado ningun valor
        return None

    notas_i, recuentos_i = zip(*resultado_individual)                      # desempaquetar
    notas_i, recuentos_i = comprobacion_notas(notas_i, recuentos_i)        # Comprobacion para que estén todas las notas posibles [1-5]

    total_reviews, media, moda = metricas([id], notas_i, recuentos_i)      # Sus métricas individuales
    impresion_histograma_notas_usuario(notas_i, recuentos_i, [id])         # Histograma individual de cada usuario

    return (notas_i, recuentos_i, total_reviews, media, moda)


def comparacion_distribucion_notas(ids, recuentos):
    """
    Muestra una comparación de la distribución de notas (1-5) entre varios usuarios.
    Args:
    - ids (list): lista de identificadores de usuarios
    - recuentos (list): lista de listas de recuentos de cada nota para cada usuario.
    - No traemos la variable notas (lista de listas) debido a que todas las listas que contiene son iguales [1,2,3,4,5].
    Returns:
    - None
    """

    st.subheader("Comparación de la distribución de notas entre usuarios")
    notas_posibles = [i for i in range(1, 6)] # Notas posibles de 1 a 5

    # Tabla comparativa
    tabla = []
    #iteramos directamente sobre el usuario y el recuento de sus notas
    for id_usuario, recuento_usuario in zip(ids, recuentos): 
        
        fila = {"Usuario": id_usuario}
        
        # Iteramos sobre las 5 notas posibles y las añadimos a la fila como columnas
        for indice, nota in enumerate(notas_posibles):
            fila[f"Nota {nota}"] = recuento_usuario[indice]

        tabla.append(fila)
        
    st.table(tabla)

    # Gráfico de barras agrupadas
    x = np.arange(len(notas_posibles))
    ancho = 0.8 / len(ids)   # reparte el ancho total entre todos los usuarios

    fig, ax = plt.subplots(figsize=(10, 6))

    for i, id_usuario in enumerate(ids):
        recuentos_usuario = recuentos[i] # recuentos del usuario actual
        ax.bar(x + i * ancho, recuentos_usuario, width=ancho, label=id_usuario)

    ax.set_title("Distribución de notas por usuario")
    ax.set_xlabel("Nota")
    ax.set_ylabel("Número de reviews")
    
    ax.set_xticks(x + ancho * (len(ids) - 1) / 2) #centrar las barras del eje x
    ax.set_xticklabels(notas_posibles)
    ax.legend()

    st.pyplot(fig) #se plotea el gráfico ya adaptado en streamlit


def graficas_comparativas(ids, almacen_dict):
    """
    Función que muestra gráficas comparativas entre varios usuarios, incluyendo la comparación de la distribución de notas y la comparación de la nota media.

    Args:
    - ids (list): lista de identificadores de usuarios
    - almacen_dict (dict): diccionario que contiene las métricas individuales de cada usuario.
    Returns:
    - None        
    """
    usuario_mas_activo = max(almacen_dict, key=lambda u: almacen_dict[u][2])        # Usuario con más reviews
    usuario_media_mas_alta = max(almacen_dict, key=lambda u: almacen_dict[u][3])    # Usuario con media de notas más alta

    # Mostramos los resultados:
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Usuario más activo", usuario_mas_activo, almacen_dict[usuario_mas_activo][2])
    with col2:
        st.metric("Mayor nota media", usuario_media_mas_alta, f"{almacen_dict[usuario_media_mas_alta][3]:.2f}")

    # Gráfica comparativa de la nota media entre los usuarios introducidos
    medias = [almacen_dict[id][3] for id in ids] # Media de cada usuario para la gráfica
    fig, ax = plt.subplots()
    ax.bar(ids, medias, color=plt.cm.Paired.colors[:len(ids)], edgecolor='black') #lo que hacemos es meterle una paleta de colores de matplotlib (más estético)
    
    ax.set_title("Comparación de nota media entre usuarios")
    ax.set_xlabel("Usuario")
    ax.set_ylabel("Nota media")
    ax.tick_params(axis='x', rotation=45) #movemos las etiquetas
    st.pyplot(fig)

    # Gráfica comparativa de actividad entre los usuarios introducidos (total de reviews)
    totales_reviews = [almacen_dict[id][2] for id in ids] # Total de reviews de cada usuario para la gráfica
    fig, ax = plt.subplots()
    
    ax.bar(ids, totales_reviews, color=plt.cm.Pastel1.colors[:len(ids)], edgecolor='black')
    ax.set_title("Comparación del número de reviews entre usuarios")
    ax.set_xlabel("Usuario")
    ax.set_ylabel("Total de reviews")
    ax.tick_params(axis='x', rotation=45)
    st.pyplot(fig)

    # Gráfica comparativa de la distribución de notas entre los usuarios introducidos
    recuentos = [almacen_dict[id][1] for id in ids]
    comparacion_distribucion_notas(ids, recuentos) #aquí esta la función ya creada


#ESTA ES LA FUNCIÓN QUE MUESTRA EL DISPLAY EN STREAMLIT Y CONTIENE TODO LO QUE SE VA A MOSTRAR (DEL ADICIONAL)
def histograma_notas_usuario(cursor):
    """
    Función que muestra un histograma de las notas que ha puesto el usuario introducido por el usuario.
    Da la opción de introducir un único usuario o varios. En ese caso, se encarga de organizar y ver si 
    sólo hay que analizar a un individuo o la relación entre varios

    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    Returns:
    - None
    """
    st.title("Distribución de las puntuaciones emitidas por uno o varios usuarios.")

    st.write("Puedes introducir el identificador de un usuario o varios usuarios separados por comas para analizar las notas que han puesto en sus reviews.")
    st.write("En el caso en el que solo desees conocer las notas puestas por un usuario, se mostrará un histograma con la distribución de sus notas.")
    st.write("En el caso de introducir varios usuarios, se mostrará un histograma con la media de las notas puestas por cada usuario y una comparación entre ellos.")
    
    # Pedir el input del reviewerID
    reviewer_id = st.text_input("Escribe el identificador del usuario(s separados mediante comas) cuyas notas puestas deseas analizar:").strip()

    if reviewer_id: # cuando ya se ha introducido algo en el input, para evitar errores de consulta con id vacío
        ids = [i.strip() for i in reviewer_id.split(",")]  # Dividir por comas y quitar espacios en blanco (limpiar)
        ids = comprobacion_existencia_usuarios(cursor, ids) # Comprobar que los usuarios existen y quedarnos solo con los que existen
        
        if len(ids) == 0: # Si no ha quedado ningún usuario válido, se le informa al usuario y se sale de la función
            st.write("No se ha introducido ningún usuario válido para analizar.")
            return
        
        # Métricas complementarias
        elif len(ids) > 1:                                # Hay varios usuarios

            # Consulta de todos los usuarios
            consultas_notas_usuarios(cursor, ids)         # Realizar la consulta en MySQL (llamamos a función)
            resultado_comun = cursor.fetchall()           # Resultado de todos los usuarios (se lanza aquí el fetchall, en vez de enviar como return)
            
            notas_c, recuentos_c = zip(*resultado_comun)                     
            notas_c = [float(n) for n in notas_c]
            recuentos_c = [int(r) for r in recuentos_c]
            
            notas_c, recuentos_c = comprobacion_notas(notas_c, recuentos_c)   # Comprobacion para que estén todas las notas posibles [1-5]
            
            st.markdown("📊 Visión Global del Grupo")
            metricas(ids, notas_c, recuentos_c)
            impresion_histograma_notas_usuario(notas_c, recuentos_c, ids) #Histograma de todos (aquí llamamos a las funciones)


            # Consulta de cada usuario
            almacen_list = []       # Almacenar las métricas individuales de cada usuario para mostrar después
            almacen_dict = {} 

            for reviewer_id in ids:
                datos = comportamiento_por_usuario(cursor, reviewer_id) #uno a uno
                
                if datos is None: #para que no falle el programa
                    st.warning(f"No se pudieron recuperar datos para {reviewer_id}")
                    continue
                
                notas_i, recuentos_i, total_reviews, media, moda = datos # despejar
                
                # Para uso posterior en la comparación entre usuarios
                almacen_list.append({
                    "Usuario": reviewer_id,
                    "Total reviews": total_reviews,
                    "Nota media": round(media, 2) if media is not None else None,
                    "Moda": moda 
                    })
                
                almacen_dict[reviewer_id] = (notas_i, recuentos_i, total_reviews, media, moda) #actualizamos para mandarlo

            # Comparación entre todos los usuarios introducidos
            st.subheader("Comparación de los usuarios introducidos:")
            st.table(almacen_list) # Tabla de métricas individuales de cada usuario
            graficas_comparativas(ids, almacen_dict) # Gráficas comparativas entre usuarios


        else:                   # Solo un usuario
            st.markdown("Análisis Individual")
            comportamiento_por_usuario(cursor, ids[0])    


# EJERCICIO DE LA QUINTA PARTE DEL PROYECTO (considero oportuno meterlo en el streamlit)
def articulos_populares_no_consumidos(cursor):
    """
    Esta función se encarga de hacer tanto una consulta a MySQL como posteriormente almacenar el resultado de esa consulta en un dataframe para 
    mostrarlo en el streamlit. En este caso, esta función espera que el cliente inserte un ID de usuario válido y también una categoría de review, 
    y entonces le devolverá a ese cliente los 10 artículos más populares de esa categoría que todavía no ha consumido (incluyendo el nº de reviews de ese producto).
    Es una forma de recomendarle al cliente artículos bastante "valorados" que todavía no ha probado
    
    Args:
    - cursor: conexión a MySQL para realizar consultas en la base de datos
    
    Returns:
    - None
    
    """
    st.title("Listado de los 10 artículos más populares no consumidos ")

    st.write("Se requiere el ID específico de un usuario y un tipo de categoría para poder analizar en su caso cuales son los 10 artículos más populares que el usuario no ha valorado")
    
    # Pedir el input del reviewerID
    reviewer_id = st.text_input("Escribe el identificador del usuario (sin comillas) cuyos artículos consumidos desees analizar ").strip()

    #REUTILIZAMOS LA FUNCIÓN YA DEFINIDA PARA ELEGIR CATEGORÍA
    categoria = eleccion_tipo_sin_todo()
    
    if st.button("Generar Recomendaciones"):
        
        if reviewer_id: #es decir, que el usuario haya puesto un ID (algo)
            
            ids_comprobados = comprobacion_existencia_usuarios(cursor, [reviewer_id]) #USAMOS LA FUNCIÓN QUE REVISA QUE EL ID ESTÉ EN LA BD
            
            if ids_comprobados: #la lista no vuelve vacía   
            
                consulta = """
                SELECT r.asin, COUNT(r.id_review) AS num_reviews
                FROM reviews r
                JOIN articulos art ON r.asin = art.asin
                WHERE art.categoria = %s 
                AND r.asin NOT IN (SELECT asin
                                FROM reviews
                                WHERE reviewerID = %s)
                GROUP BY r.asin
                ORDER BY num_reviews DESC
                LIMIT 10
                """
                try: 
                    cursor.execute(consulta, (categoria, reviewer_id))
                    resultado = cursor.fetchall()
                    
                    if resultado: #mostramos en streamlit los 10 artículos

                            # Hacemos un dataframe con los resultados (pandas interpreta perfectamente el restulado del cursor)
                            df = pd.DataFrame(resultado, columns=["ID de artículo más popular", "Número de Reviews"])
                            
                            df.index = df.index + 1 #esta línea es pura decoración, para que los índices se muestren del 1 al 10
                            
                            st.success(f"Se han encontrado las reviews para el usuario {reviewer_id} sobre productos del tipo {categoria}")
                            
                            # Dibujamos la tabla en Streamlit
                            st.dataframe(df, width='stretch') 
                
                    else:
                        st.warning("No se encontraron artículos para esta combinación.")
                
                except Exception as err:
                    st.error(f"Error a la hora de buscar la información en SQL: {err}")
        
        else:
            st.write("Por favor, inserte un ID para comenzar la consulta")




if __name__ == "__main__":
    
    try:
        client = conexion_mongo()
        db = client[nombre_bd_mongo] # DataBase
        collection = db[coleccion_mongo] # Colección

        conexion_mysql = conexion_db_SQL()

        # MENÚ LATERAL
        st.sidebar.title("MENÚ")
        opcion = st.sidebar.radio(
            "Selecciona una visualización:",
            [
                "Inicio",
                "Reviews por años",
                "Popularidad de los artículos",
                "Notas por producto",
                "Reviews a lo largo del tiempo para todas las categorías",
                "Reviews por usuario",
                "Nube de palabras en función de la categoría",
                "Notas por usuario",
                "Artículos populares no consumidos",
                "SALIDA"
            ]
        )

        with conexion_mysql.cursor() as cursor:
            # CONTROL DEL MENÚ
            if opcion == "Inicio":
                mostrar_inicio()
            elif opcion == "Reviews por años":
                evolucion_por_años(cursor)
            elif opcion == "Popularidad de los artículos":
                evolucion_popularidad(cursor)
            elif opcion == "Notas por producto":
                histograma_nota(cursor)
            elif opcion == "Reviews a lo largo del tiempo para todas las categorías":
                evolucion_tiempo_categorias(cursor)
            elif opcion == "Reviews por usuario":
                histograma_usuario(cursor)
            elif opcion == "Nube de palabras en función de la categoría":
                nube_palabras(collection)
            elif opcion == "Notas por usuario":
                histograma_notas_usuario(cursor)
            elif opcion == "Artículos populares no consumidos":
                articulos_populares_no_consumidos(cursor)
            elif opcion == "SALIDA":
                salida()

    except Exception as e:
        print("Error al conectarse:", e)
    
    finally: 
        # Cerrar conexiones
        if 'client' in locals():
            client.close()
        if 'conexion_mysql' in locals():
            conexion_mysql.close()



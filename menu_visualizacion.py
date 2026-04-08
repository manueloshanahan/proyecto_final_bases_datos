"""
Fichero que crea una aplicación mediante la librería Streamlit que permite al usuario 
elegir distintas visualizaciones y que el programa no termine hasta que elija "salir" (basta con cerrar la pestaña).

Ejecución por terminal:      streamlit run menu_visualizacion.py
"""
from load_data import conexion_db_SQL, conexion_mongo
from configuracion import nombre_bd_sql, nombre_bd_mongo, coleccion_mongo
import streamlit as st
import matplotlib.pyplot as plt
import re
from collections import Counter
from wordcloud import WordCloud


# Configuración general de la página
st.set_page_config(page_title="Menú de visualización", layout="wide")

# VARIABLES GLOBALES
dict_frecuencias = {}

# FUNCIONES AUXILIARES
def eleccion_tipo():
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación 
    y da a elegir al usuario el tipo de producto, o incluso todos, que desea visualizar el estudio de sus reviews.

    In:
        None
    Out:
        opcion (str): tipo de producto escogido por el usuario
    """
    # Pestaña que da a elegir
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments", "TODO"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida     
    return opcion
    

def eleccion_tipo_comp(cursor):
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación 
    y da a elegir al usuario el tipo de producto o un artículo en concreto cuyo estudio de sus reviews desea visualizar.
    Además, si la opción escogida es el artículo: verifica si existe.

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Out:
        opcion (str): tipo de producto escogido por el usuario
        texto (str): Input del us
    """
    texto = None # Inicializamos variable 

    # Elección
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments", "TODO", "Artículo individual"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida        
    
    if opcion == "Artículo individual":
        texto = st.text_input("Escribe el identificador del artículo cuyas notas deseas consultar:").strip() # Pedir el input del asin
        
        # Comprobación de existencia
        consulta = "SELECT asin FROM reviews WHERE asin = %s LIMIT 1;"
        cursor.execute(consulta, [texto])
        resultado = cursor.fetchone()

        if not resultado:
            st.write("El artículo no existe")
            st.write("Elige otra opción o otro código de artículo válido.")
            texto = None # actualizamos variable
    
    
    return opcion, texto


def eleccion_tipo_sin_todo():
    """
    Función que abre una pestaña en la pantalla escogida de la aplicación 
    y da a elegir al usuario el tipo de producto que desea visualizar el estudio de sus reviews.

    In:
        None
    Out:
        opcion (str): tipo de producto escogido por el usuario
    """
    # Pestaña que da a elegir
    opcion = st.selectbox(
        "Elige una opción",
        ["Video Games", "Toys and Games", "Digital Music", "Musical Instruments"])
    
    st.write("Has elegido:", opcion) # Nombrar opción escogida     
    return opcion

def summaries_tipos(collection, opcion):
    """
    Función que estudia la frecuencia que tiene cada palabra de todos los summaries de un tipo determinado.

    In:
        collection (Object): conexión a MySQL para realizar consultas en la base de datos
    Out:
        frecuencias (dict): diccionario que contiene palabra: su frecuencia en todos los summaries de ese tipo de producto.
    """
    resultados = collection.find({"categoria": opcion}, {"_id": 0, "summary": 1})
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


# FUNCIONES
def mostrar_inicio():
    """
    Función que muestra la información en la pantalla inicial de la aplicación en streamlit.

    In:
        None
    Out:
        None
    """
    st.title("App de visualización")
    st.write("Selecciona una opción en el menú lateral para ver las gráficas.")
    

def evolucion_por_años(cursor):
    """
    Función que muestra la evolucion del número de reviews por año. 
    Se da la opción a escoger el tipo de producto.

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos
    Out:
        None
    """
    st.title("Evolución del número de reviews por años")
    st.write("Visualización usando matplotlib.")

    opcion = eleccion_tipo() # Elección del usuario 

    # Consulta en MySQL
    if opcion == "TODO":
        consulta = """
        SELECT YEAR(f.reviewTime) as año, COUNT(r.id_review) as n_reviews
        FROM reviews r
        INNER JOIN fechas f ON f.unixReviewTime=r.unixReviewTime
        GROUP BY YEAR(f.reviewTime) 
        ORDER BY año ASC;
        """                 # sin condición de tipo de review
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
        """                 # con condición WHERE para el tipo
        cursor.execute(consulta, [opcion])
    
    resultado = cursor.fetchall() # Tupla de tuplas. Ej: ((1998, 149), (1999, 772), (2000, 6182), ...)
    # Para evitar errores si resultado está vacío
    if resultado:
        años, datos = zip(*resultado) # desempaquetar
        
        fig, ax = plt.subplots()
        ax.bar(años, datos) # Histograma
        
        # Diseño
        if opcion == "TODO":
            ax.set_title("Reviews por años de todos los productos")
        else:
            ax.set_title(f"Reviews por años de los productos con categoría: {opcion}")

        ax.set_xlabel("Año")
        ax.set_ylabel("Número de reviews")
        ax.grid(True)

        st.pyplot(fig) # Mostrar figura dentro de streamlit

    else:
        años, datos = [], [] 
        st.write ("No se ha recuperado ningún resultado en la consulta.")

    
def evolucion_popularidad(cursor):
    """
    Función que muestra la popularidad en orden descendiente de los articulos. 
    Se da la opción a escoger el tipo de producto.

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos

    Out:
        None
    """
    st.title("Evolución de la popularidad de los artículos.")
    st.write("Visualización usando matplotlib.")
    st.write("La popularidad de un artículo está representada por el número de reviews de dicho artículo.")

    opcion = eleccion_tipo()

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
    if resultado:
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

    else:
        asin_ids, recuento = [], [] 
        st.write ("No se ha recuperado ningún resultado en la consulta.")


def histograma_nota(cursor):
    """
    Función que muestra un histograma con las notas que ha(n) obtenido todos los productos, los productos de una cierta clasificación
    o incluso un único producto concreto.

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos

    Out:
        None
    """
    st.title("Histograma de las notas en función del número de reviews en cierto(s) producto(s).")
    st.write("Visualización usando matplotlib.")

    opcion, texto = eleccion_tipo_comp(cursor)

    # Consulta en MySQL
    if opcion == "TODO":
        consulta = """
        SELECT overall, COUNT(id_review)
        FROM reviews
        GROUP BY overall
        ORDER BY overalL ASC;
        """                 # sin condición de tipo de review
        cursor.execute(consulta)

    elif opcion == "Artículo individual" and texto:  # Artículo en concreto
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews
        WHERE asin=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 # con condición WHERE para el código del árticulo
        cursor.execute(consulta, [texto])
    
    elif opcion != "Artículo individual": # los demás tipos
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews r
        INNER JOIN articulos a ON a.asin=r.asin
        WHERE a.categoria=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 # con condición WHERE para el tipo de artículo
        cursor.execute(consulta, [opcion])
    
    else: # No existen más opciones pero por si acaso
        st.error("Se ha producido un caso no previsto.")
        return
    

    resultado = cursor.fetchall() # Tupla de tuplas
    # Para evitar errores si resultado está vacío
    if resultado:
        notas, recuentos = zip(*resultado) # desempaquetar
    
        # Comprobacion para que estén todas las notas posibles [1-5]
        if len(notas) != 5:
            diccionario = dict(zip(notas, recuentos))
            diccionario = {i: diccionario.get(i, 0) for i in range(1, 6)} # 0 porque al no estar en la lista de notas significa que tiene asociadas 0 reseñas

            notas, recuentos = zip(*diccionario.items())
        
            
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
    
    
    else:
        notas, recuentos = [], []
        st.write ("No se ha recuperado ningún resultado en la consulta.")


# preguntar!!
def evolucion_tiempo_categorias():
    """
    Función que muestra la evolucion del número de reviews por año. 
    Se da la opción a escoger el tipo de review.

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos

    Out:
        None
    """
    st.title("Evolución de las reviews para todas las categorías a lo largo del tiempo.")
    st.write("Visualización usando matplotlib.")

    opcion, texto = eleccion_tipo_comp(cursor)

    # Consulta en MySQL
    if opcion == "TODO":
        consulta = """
        SELECT overall, COUNT(id_review)
        FROM reviews
        GROUP BY overall
        ORDER BY overalL ASC;
        """                 # sin condición de tipo de review
        cursor.execute(consulta)

    elif opcion == "Artículo individual" and texto:  # Artículo en concreto
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews
        WHERE asin=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 # con condición WHERE para el código del árticulo
        cursor.execute(consulta, [texto])
    
    elif opcion != "Artículo individual": # los demás tipos
        consulta = """
        SELECT overall, COUNT(id_review) as recuento
        FROM reviews r
        INNER JOIN articulos a ON a.asin=r.asin
        WHERE a.categoria=%s
        GROUP BY overall
        ORDER BY overall ASC;
        """                 # con condición WHERE para el tipo de artículo
        cursor.execute(consulta, [opcion])

    resultado = cursor.fetchall() # Tupla de tuplas
    # Para evitar errores si resultado está vacío
    if resultado:
        notas, recuentos = zip(*resultado) # desempaquetar
    
        # Comprobacion para que estén todas las notas posibles [1-5]
        if len(notas) != 5:
            diccionario = dict(zip(notas, recuentos))
            diccionario = {i: diccionario.get(i, 0) for i in range(1, 6)} # 0 porque al no estar en la lista de notas significa que tiene asociadas 0 reseñas

            notas, recuentos = zip(*diccionario.items())
        
            
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
    
    
    else:
        notas, recuentos = [], []
        st.write ("No se ha recuperado ningún resultado en la consulta.")


def histograma_usuario(cursor):
    """
    Función que muestra un histograma que representa cuantos usuarios han publicado una cantidad de reviews. 

    In:
        cursor: conexión a MySQL para realizar consultas en la base de datos

    Out:
        None
    """
    st.title("Histograma que representa cuantos usuarios han publicado una cantidad de reviews.")
    st.write("Visualización usando matplotlib.")

    # Consulta en MySQL
    consulta = """
    SELECT n_reviews, COUNT(reviewerID) AS n_usuarios
    FROM (
        SELECT reviewerID, COUNT(id_review) AS n_reviews  -- cada usuario ha hecho n_reviews
        FROM reviews
        GROUP BY reviewerID
    ) t
    GROUP BY n_reviews
    ORDER BY n_reviews ASC;
    """
    cursor.execute(consulta)
    
    resultado = cursor.fetchall() # Tupla de tuplas

    # Para evitar errores si resultado está vacío
    if resultado:
        n_reviews, n_usuarios = zip(*resultado) # desempaquetar
    
        fig, ax = plt.subplots()
        ax.bar(n_reviews, n_usuarios) # Histograma
        
        # Diseño
        ax.set_title("Histograma de reviews por usuario.")

        ax.set_xlabel("Número de reviews.")
        ax.set_ylabel("Número de usuarios que han hecho esa cantidad de reviews.")

        st.pyplot(fig) # Mostrar figura dentro de streamlit

    else:
        n_reviews, n_usuarios = [], [] 
        st.write ("No se ha recuperado ningún resultado en la consulta.")

def nube_palabras(collection):
    """
    Función que muestra una nube de palabras según el tipo de productos elegido por el usuario. 
    
    In:
        collection: conexión a Mongo para realizar consultas en la base de datos

    Out:
        None
    """
    global dict_frecuencias # para poder actualizar

    st.title("Nube de palabras")
    st.write("Visualización usando wordcloud.")

    opcion = eleccion_tipo_sin_todo()

    # Consulta en Mongo
    if opcion in dict_frecuencias:                    # Ya ha sido estudiado
        frecuencias = dict_frecuencias[opcion]
    else:                                             # Se evita para optimizar el rendimiento
        frecuencias = summaries_tipos(collection, opcion)
        dict_frecuencias[opcion] = frecuencias # Almacenar para la próxima vez

    if not frecuencias: # Ha saltado alguna condición de seguridad
        return

    # Crear nube
    nube = WordCloud(width=800, height=400, background_color="white")
    nube.generate_from_frequencies(frecuencias)

    # Mostrar
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(nube, interpolation="bilinear")
    ax.axis("off")

    st.pyplot(fig)

def salida():
    global client, conexion_mysql
    st.title("Boxplot de goles por equipo")
    client.close()
    conexion_mysql.close()


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
                "Histograma por nota",
                "Reviews a lo largo del tiempo para todas las categorías",
                "Reviews por usuario",
                "Nube de palabras en función de la categoría",
                "   ..... ",
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
            elif opcion == "Histograma por nota":
                histograma_nota(cursor)
            elif opcion == "Reviews a lo largo del tiempo para todas las categorías":
                evolucion_tiempo_categorias(cursor)
            elif opcion == "Reviews por usuario":
                histograma_usuario(cursor)
            elif opcion == "Nube de palabras en función de la categoría":
                nube_palabras(collection)
            elif opcion == "   .... ":
                #funcion()
                pass
            elif opcion == "SALIDA":
                salida()



    except Exception as e:
        print("Error al conectarse:", e)


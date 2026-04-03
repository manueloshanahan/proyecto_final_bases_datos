"""
Fichero que crea una aplicación mediante la librería Streamlit que permite al usuario 
elegir distintas visualizaciones y que el programa no termine hasta que elija "salir" (basta con cerrar la pestaña).

Ejecución por terminal:      streamlit run menu_visualizacion.py
"""
from load_data import conexion_SQL, conexion_mongo
import streamlit as st
import matplotlib.pyplot as plt

# Configuración general de la página
st.set_page_config(page_title="Menú de visualización", layout="wide")

# FUNCIONES
def mostrar_inicio():
    st.title("App de visualización")
    st.write("Selecciona una opción en el menú lateral para ver las gráficas.")
    st.write("Vista previa de los datos:")
    #st.dataframe(df)

def evolucion_por_años():
    st.title("Evolución de reviews por años")
    st.write("Visualización usando seaborn.")

    # Elección de tipo de review
    opcion = st.sidebar.selectbox(
        "Elige una opción",
        ["Videojuegos", "Juguetes y juegos", "Música", "Instrumentos", "TODO"])

    st.write("Has elegido:", opcion)

    fig, ax = plt.subplots()



    st.pyplot(fig)
    

def evolucion_popularidad():
    st.title("Boxplot de goles por equipo")
    st.write("Visualización usando seaborn.")
    fig, ax = plt.subplots()

    st.pyplot(fig)

def histograma_nota():
    st.title("Boxplot de goles por equipo")
    st.write("Visualización usando seaborn.")
    fig, ax = plt.subplots()

    st.pyplot(fig)

def evolucion_tiempo_categorias():
    st.title("Boxplot de goles por equipo")
    st.write("Visualización usando seaborn.")
    fig, ax = plt.subplots()

    st.pyplot(fig)

def histograma_usuario():
    st.title("Boxplot de goles por equipo")
    st.write("Visualización usando seaborn.")
    fig, ax = plt.subplots()

    st.pyplot(fig)

def nube_palabras():
    st.title("Boxplot de goles por equipo")
    st.write("Visualización usando seaborn.")
    fig, ax = plt.subplots()

    st.pyplot(fig)

def salida():
    global client, conexion_mysql
    st.title("Boxplot de goles por equipo")
    client.close()
    conexion_mysql.close()


if __name__ == "__main__":
    try:
        client = conexion_mongo()
        db = client["reviews"] # DataBase
        # collection = db[COLLECTION_NAME] # Colección

        conexion_mysql = conexion_SQL()

        # MENÚ LATERAL
        st.sidebar.title("MENÚ")
        opcion = st.sidebar.radio(
            "Selecciona una visualización:",
            [
                "Inicio",
                "Evolución de reviews por años",
                "Evolución de la popularidad de los artículos",
                "Histograma por nota",
                "Evolución de las reviews a lo largo del tiempo para todas las categorías",
                "Histograma de reviews por usuario",
                "Obtener una nube de palabras en función de la categoría",
                "   ..... ",
                "SALIDA"
            ]
        )

        with conexion_mysql.cursor() as cursor:
            # CONTROL DEL MENÚ
            if opcion == "Inicio":
                mostrar_inicio()
            elif opcion == "Evolución de reviews por años":
                evolucion_por_años()
            elif opcion == "Evolución de la popularidad de los artículos":
                evolucion_popularidad()
            elif opcion == "Histograma por nota":
                histograma_nota()
            elif opcion == "Evolución de las reviews a lo largo del tiempo para todas las categorías":
                evolucion_tiempo_categorias()
            elif opcion == "Histograma de reviews por usuario":
                histograma_usuario()
            elif opcion == "Obtener una nube de palabras en función de la categoría":
                nube_palabras()
            elif opcion == "   .... ":
                funcion()
            elif opcion == "SALIDA":
                salida()



    except Exception as e:
        print("Error al conectarse:", e)


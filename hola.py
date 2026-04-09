# preguntar!!
def evolucion_tiempo_categorias(cursor):
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


    # Consulta en MySQL
    consulta = """
    SELECT unixReviewTime, COUNT(id_review)
    FROM reviews
    GROUP BY unixReviewTime
    ORDER BY unixReviewTime ASC;
    """
    cursor.execute(consulta)
    resultado = cursor.fetchall() # Tupla de tuplas
    # Para evitar errores si resultado está vacío
    if not resultado:
        st.write("No se ha recuperado ningún resultado en la consulta.")
        return

    tiempo, recuentos = zip(*resultado) # desempaquetar

    #Acumulación de reviews a lo largo del tiempo
    acumulacion = np.cumsum(recuentos)
        
    fig, ax = plt.subplots()
    ax.bar(tiempo, acumulacion) # Histograma
    
    # Diseño
    ax.set_title("Evolución del número de reviews a lo largo del tiempo.")
    ax.set_xlabel("Tiempo (Unix Review Time)")
    ax.set_ylabel("Número de reviews")

    st.pyplot(fig) # Mostrar figura dentro de streamlit


    
SELECT r.unixReviewTime, COUNT(*) AS n_reviews
FROM reviews r
GROUP BY r.unixReviewTime
ORDER BY r.unixReviewTime ASC;
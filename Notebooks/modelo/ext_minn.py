from google.cloud import bigquery
import os
import pandas as pd
import streamlit as st

#conectarse a bucket gcs y bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credencial.json'


bq_client = bigquery.Client()

# Definir la consulta SQL REVIEWS
query_reviews = """
    SELECT *
    FROM `xenon-mantis-431402-j8.db.reviews_yelp`
    LIMIT 100000
"""
# Ejecutar la consulta y convertir los resultados en un DataFrame de Pandas
reviews = bq_client.query(query_reviews).to_dataframe()
 
# Definir la consulta SQL NEGOCIOS
query_yelp = """
    SELECT *
    FROM `xenon-mantis-431402-j8.db.Negocios_Yelp`
    LIMIT 100000
"""
# Ejecutar la consulta y convertir los resultados en un DataFrame de Pandas
negocios_yelp = bq_client.query(query_yelp).to_dataframe()


# Definir la consulta SQL CIUDAD
query_ciudad = """
    SELECT *
    FROM `xenon-mantis-431402-j8.db.Ciudad`
    LIMIT 100000
"""
# Ejecutar la consulta y convertir los resultados en un DataFrame de Pandas
ciudad = bq_client.query(query_ciudad).to_dataframe()


# Definir la consulta SQL
query_categoria = """
    SELECT *
    FROM `xenon-mantis-431402-j8.db.Categoria`
    LIMIT 100000
"""
# Ejecutar la consulta y convertir los resultados en un DataFrame de Pandas
reviews = bq_client.query(query_reviews).to_dataframe()
negocios_yelp= bq_client.query(query_yelp).to_dataframe()
ciudad = bq_client.query(query_ciudad).to_dataframe()
categoria = bq_client.query(query_categoria).to_dataframe()

st.title('Sistema de Recomendación de Negocios')
st.subheader('Si utiliza alguna de estas recomendaciones, recuerda dejar tu opinión, es muy importante para nuestros clientes =)')

ciudad_input = st.selectbox("Seleccione la ciudad:", ciudad['Ciudad'].unique())

categoria_input = st.selectbox("Seleccione la categoría:", categoria['Categoria'].unique())

# Botón para generar recomendaciones
if st.button('Obtener Recomendaciones'):
    # Filtrar la ciudad seleccionada
    ciudad_seleccionada = ciudad[ciudad['Ciudad'].str.lower() == ciudad_input.lower()]['Id_Ciudad'].values

    if len(ciudad_seleccionada) == 0:
        st.error("Ciudad no encontrada.")
    else:
        id_ciudad_seleccionada = ciudad_seleccionada[0]
# Filtrar la categoría seleccionada
        categoria_seleccionada = categoria[categoria['Categoria'].str.lower() == categoria_input.lower()]['Id_Categoria'].values

        if len(categoria_seleccionada) == 0:
            st.error("Categoría no encontrada.")
        else:
            # Filtrar los negocios por ciudad y categoría
            negocios_filtrados = negocios_yelp[(negocios_yelp['Id_Ciudad'] == id_ciudad_seleccionada) & 
                                               (negocios_yelp['Id_Categoria'].isin(categoria_seleccionada))]

            if negocios_filtrados.empty:
                st.warning("No se encontraron negocios para la categoría y ciudad ingresadas.")
            else:
                # Filtrar las reseñas de esos negocios
                reseñas_filtradas = reviews[reviews['business_id'].isin(negocios_filtrados['Id_Negocio'])]

                # Calcular el sentimiento promedio por negocio
                sentimiento_promedio = reseñas_filtradas.groupby('business_id')['compound'].mean().reset_index()

                # Unir con la tabla de negocios para obtener los nombres
                recomendaciones = pd.merge(sentimiento_promedio, negocios_filtrados, left_on='business_id', right_on='Id_Negocio')

                # Ordenar por sentimiento y mostrar resultados
                recomendaciones = recomendaciones.sort_values(by='compound', ascending=False)

                # Mostrar las recomendaciones
                if not recomendaciones.empty:
                    st.subheader("Recomendaciones:")
                    for index, row in recomendaciones.head(10).iterrows():
                        st.write(f"{row['Nombre']}** - Dirección: {row['Direccion']} - Sentimiento: {row['compound']:.2f}")
                else:
                    st.warning("No hay suficientes datos de reseñas para generar recomendaciones.")
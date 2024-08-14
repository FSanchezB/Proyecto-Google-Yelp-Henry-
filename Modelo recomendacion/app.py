import streamlit as st
import pandas as pd

# Ejemplo de DataFrames (deberías cargar los tuyos desde archivos o bases de datos)
reviews = pd.read_csv(r'C:\Users\Fernando\Desktop\Proyecto-Google-Yelp-Henry-\reviews_sentimiento.csv')
negocios_yelp = pd.read_csv(r'Datasets/Tablas csv/negocios_yelp.csv', encoding='utf-8')
ciudad = pd.read_csv(r'Datasets/Tablas csv/Ciudad.csv', encoding='utf-8')
categoria = pd.read_csv(r'Datasets/Tablas csv/Categoria.csv', encoding='utf-8')
negocios_yelp.columns = negocios_yelp.columns.str.strip()

# Título de la aplicación
st.title('Sistema de Recomendación de Negocios')

# Entrada de la ciudad
ciudad_input = st.selectbox("Seleccione la ciudad:", ciudad['Ciudad'].unique())

# Entrada de la categoría
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

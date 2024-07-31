# Proyecto Final - Henry Data Science Bootcamp

![Portada](./assets/portada.jpg)

## Contexto

"Como parte de una consultora de data, nos han contratado para poder realizar un análisis del mercado estadounidense. Nuestro cliente es parte de un conglomerado de empresas de establecimientos, y desean tener un análisis detallado de la opinión de los usuarios en Yelp y cruzarlos con los de Google Maps sobre hoteles, restaurantes y otros negocios, utilizando análisis de sentimientos, debemos predecir cuáles serán los rubros de los negocios que más crecerán (o decaerán). Además, desean saber dónde es conveniente emplazar los nuevos locales de restaurantes y afines, y desean poder tener un sistema de recomendación de establecimientos para los usuarios de ambas plataformas para darle, al usuario por ejemplo la posibilidad de poder conocer nuevos sabores basados en sus experiencias previas."

## Datos

Los datos utilizados en este proyecto nos fueron proporcionados por Henry:

- **Google Maps**: Se nos proporcionó varios datasets de Google Maps con la información sobre los establecimientos y las reseñas dejadas por los usuarios de Google Maps. [Datasets de Google](https://drive.google.com/drive/folders/1Wf7YkxA0aHI3GpoHc9Nh8_scf5BbD4DA).

- **Yelp**: Al igual que con Google Maps, se nos proporcionó los datasets de Yelp con la información sobre los establecimientos y las reseñas dejadas por sus usuarios. [Datasets de Yelp](https://drive.google.com/drive/folders/1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF)

- **Diccionario de Datos**: Adicionalmente se nos proporcionó un diccionario de datos para fácilitar la comprensión de los mismos. [Diccionario de Datos](https://docs.google.com/document/d/1ASLMGAgrviicATaP1UJlflpmBCXtuSTHQGWdQMN6_2I/edit).

## Repositorio de GitHub

Este repositorio contiene las siguientes carpetas:

- [`Notebooks`](./Notebooks/): Carpeta con los Jupiter Notebooks empleados para hacer los distintos EDA's y limpiezas de datos.
- [`assets`](./assets/): Carpeta con imágenes empleadas en este repositorio.
- [`README.md`](./README.md): Este archivo, que proporciona una descripción general del proyecto y sus resultados.

## Objetivo

Ya vista la propuesta de trabajo nuestro objetivo se inclinó hacía las reseñas de Yelp, de las cuales se hizo su análisis de sentimiento y sistema de recomendación para mejorar el sector de servicios en Estados Unidos, además de que se hizo un exhaustivo análisis y tratamiento a los datos para tener insights sobre como mejorar el sector de servicios fuera del sistema de recomendación y el análisis de sentimiento.

## Metodología

Para llevar a cabo este proyecto, se siguieron los siguientes pasos:

### Elaborar un Diagrama de Grannt
Se trata de un cronograma que se elaboró en el primer día del proyecto con el fin de organizar todas las tareas del proyecto y cumplir con un plazo determinado por el trabajo y el tiempo de todos los integrantes de este proyecto.
Para este fin utilizamos la herramienta ClickUp para asignar tareas a los días y poder poner anotaciones individuales o en conjunto.

### Carga y limpieza inicial de los datos
Se realizó un proceso de limpieza de datos para preparar los datos antes del análisis. Esto incluyó la extracción de los datos desde las fuentes originales, la limpieza y transformación (ETL) de los mismos para garantizar su calidad y consistencia. El proceso de ETL se encuentra detallado en los archivos de la carpeta Notebook (Para mas información ver el archivo "AboutNotebooks" para aprender más sobre ellos) [`Notebooks`](./Notebooks/).

### Análisis Exploratorio de Datos (EDA)
Se realizó un análisis exhaustivo de los datos utilizando Python y las librerías pandas, numpy, seaborn y matplotlib en varios notebook de Jupyter. Durante este proceso, se llevaron a cabo las siguientes tareas:
- Búsqueda y tratamiento de valores faltantes, atípicos y duplicados.
- Análisis de la distribución de las variables y su relación con los siniestros viales.
- Generación de visualizaciones coherentes para una mejor comprensión de los datos.
- Documentación detallada de los hallazgos y conclusiones en cada etapa del análisis.

El análisis exploratorio de datos se encuentra en la carpeta de Notebooks. [`Notebooks`](./Notebooks/).

### Alcance

Con el fin de aprovechar el tiempo propuesto para este proyecto se ha decidido centrarse en analizar las reseñas de Yelp poder proponer mejoras mas generales al mercado mas no a una cadena y/o servicio especifico. 
Para esto se ha decidido disponer de los siguientes datasets: 
- Google Maps:
1. “metadata-sitios”
Otorga información sobre los comercios con su respectiva localización, categoría y atributos varios
- Yelp: 
1. “business”
Otorga información similar a la de “metadata-sitios” pero en este caso se obtiene la que se encuentra en la base de datos de Yelp. Es un dataset útil para poder relacionarlo al de Google Maps.
2. “reviews”
Otorga la lista de reseñas que les dio a los establecimientos a completo detalle

### KPI's
Se definieron los siguientes KPI's (Indicadores clave de rendimiento):
- KPI 1:
- KPI 2:
- KPI 3 

## Conclusiones

A continuación se detallan las conclusiones del proyecto:

### Conclusion 1
- Punto 1 
- Punto 2

## Stack de Tecnologias
- Visual Studio Code
- Python (Incluye las librerias: Numpy, Pandas, Matplotlib, Seaborn y SK Learn)
- ClickUp
- Git y GitHub
- Canva
- PowerBI

## Autores

Este proyecto fue desarrollado por las siguientes personas:

Sebastian Quintero
- GitHub: [https://github.com/SebastianQuintero04](https://github.com/SebastianQuintero04)
- Correo electrónico: [quinterosebastian820@gmail.com](mailto:quinterosebastian820@gmail.com)

Fernando Sanchez Barrera
- GitHub: [https://github.com/FSanchezB](https://github.com/FSanchezB)
- Correo electrónico: [fernandosb0320@gmail.com](mailto:fernandosb0320@gmail.com)

Erick Daniel Romero
- GitHub: [https://github.com/erickdrl](https://github.com/erickdrl)
- Correo electrónico: [erick3728@gmail.com](mailto:erick3728@gmail.com)

Fabián Rodríguez
- GitHub: [https://github.com/farodd](https://github.com/farodd)
- Correo electrónico: [farodriguezorellana@gmail.com](mailto:farodriguezorellana@gmail.com)

Miguel Ángel Jaramillo
- GitHub: [https://github.com/MAngelJaramillo](https://github.com/MAngelJaramillo)
- Correo electrónico: [miguelangelgomezj@gmail.com](mailto:miguelangelgomezj@gmail.com)

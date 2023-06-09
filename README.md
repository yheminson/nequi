# Validación de rating para plataformas de streaming
La idea es plantear un caso de uso donde podamos validar las películas más vistas, con mejor rating, las más votadas, con el fin de ofrecer un mejor servicios y una mejor parrilla de programación en una plataforma de streaming y así tratar de ser la plataforma con mayores suscriptores ya que tendríamos la mejor programación.

## Plataformas de streaming analizadas:

  - IMDB  
  - MUBI  
  - NETFLIX

## Funcionalidades:
  
  ### Python
  - Descarga los DataSet desde keggle (https://www.kaggle.com/)
  - Los DataSet se descargan a través de un API con la librería de kaggle en python.
  - Se descomprimen los DataSet
  - Se realizan validaciones y manipulación de data con pandas
    - Verificar si hay valores perdidos en todo el DataFrame
    - Limpieza de datos duplicados
    - Elimina duplicados en el DataFrame
    - Verificar el tipo de datos de cada columna
    - Eliminar registros null en los Primary Key
    - Sustituye los valores \N por valores null
    - Se realiza la cargar de los DataSet en un bucket S3 AWS
  - El flujo cuenta con un directorio log el cual guarda el paso a paso y los errores en la ejecución
  
  ### Glue
  - Se realiza la lectura desde el S3
  - Se realiza la carga a Redshift AWS

  ### Herramientas
  - Python
  - pandas
  - Glue
  - S3 AWS
  - Redshift

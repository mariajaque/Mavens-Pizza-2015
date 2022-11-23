# Mavens-Pizza-2015
En este repositorio se encuentra un código que realiza una predicción sobre los ingredientes que debe comprar la empresa 'Mavens-Pizza' una semana cualquiera de 2016. Esta predicción se devuelve en formato csv y se devuelve también un informe con la calidad de los datos de 2015 en formato txt.

Este repositorio contiene los siguientes ficheros necesarios para la ejecución:
- **"requirements.txt"**: contiene todas las librerías necesarias para la ejecución del programa
- **"orders.csv"**: contiene los datos relacionados a las fechas de cada pedido
- **"order_details.csv"**: contiene los datos relacionados a la cantidad y tipo de pizza de cada pedido concreto
- **"pizzas.csv"**: contiene información relacionada con los distintos tipos de pizza de la pizzería como su tamaño y su precio
- **"pizza_types.csv"**: incluye los datos relacionados con la categoría de cada tipo de pizza y los ingredientes que contiene
- **"pizzas.py"**: contiene el código que se debe de ejecutar para obtener la predicción de los ingredientes que deberá de comprar la pizzaría. El programa consta de dos ETL, una para la extracción y el tratado de los datos y otra para la elaboración de la predicción. En lo relacionado al tratado de los datos, el programa los manipula hasta obtener un único dataframe que contenga la información de cada pedido, la semana y día de la semana en el que se realizó y los ingredientes requeridos para la preparación de ese pedido en concreto. Para la realización de la predicción lo que hace el programa es establecer la predicción como la media de las modas de cada ingrediente por semana, es decir, calcula el total de cada ingrediente necesitado cada semana del año y se queda con la moda de cada ingrediente. En caso de empate hace la media de las modas.Finalmente la predicción se almacena en el fichero "salida.csv". El programa también realiza un análisis de la calidad de los datos contenidos en los ".csv" de Mavens Pizza. Para dicho análisis tiene en cuenta valores como el número de nulls y de nans de cada fichero.

Y tras su ejecución el programa genera los siguientes ficheros de salida:
- **"informe_calidad_datos.txt"**: contiene el análisis de la calidad de los datos de la pizzería
- **"salida.csv"**: contiene la predicción de los ingredientes que deberá comprar la empresa para una semana

### Ejecución del programa:

Primero se deberá hacer un pip install requirements.txt para que se descarguen todas las librerías necesarias para el programa. Posteriormente ya se podrá ejecutar el fichero "pizzas.py" que tardará aproximadamente un minuto en devolver la predicción.



# Qué hay que comprar para la semana que viene
"""
- The order_details tables has 48621 rows containing order details regarding
pizza type and order quantity.
- The orders table record the datetime indicators of the 21351 orders.
- The pizza_types table specifies the category, ingredients information about
the 33 different pizza types offered by the pizza place.
- The pizzas table has 97 rows containing the pricing details of pizza based on
the size and pizza type
"""
import pandas as pd
import signal
import sys
import warnings

warnings.filterwarnings("ignore")


def handler_signal(signal,frame):

    # Salida controlada del programa en caso de pulsar
    # control C

    print("\n\n [!] out .......\ n")

    sys.exit(1)

signal.signal(signal.SIGINT,handler_signal)


def extract_csv():

    # Esta función crea los 4 df y genera el informe de la
    # calidad de los datos recibidos

    fechas = pd.read_csv('orders.csv', sep=',')
    pedidos = pd.read_csv('pizzas.csv', sep=',')
    detalles = pd.read_csv('order_details.csv', sep=',')
    ingredientes = pd.read_csv('pizza_types.csv', sep=',', encoding='Windows-1252')

    informe_de_datos(fechas, pedidos, detalles, ingredientes)

    return (fechas, pedidos, detalles, ingredientes)


def transform_csv(fechas, pedidos, detalles, ingredientes):

    # Vamos a querer tener todos los datos en un único dataframe
    # Modificaremos el de order details pues es el más completo
    # Le añadiremos una nueva columna que sea el número de la semana
    # asociado a la fecha del pedido. Añadiremos una
    # columna por cada posible ingrediente de la pizza

    # Añadimos en fechas la semana asociada a cada fecha
    # Añadiremos también el día de la semana en el que ocurre
    # El pedido
    dias = []
    num_semana = []

    for fecha in fechas['date']:
        dia = pd.to_datetime(fecha, dayfirst=True)
        dias.append(dia.day_of_week)
        num_semana.append(dia.week)

    fechas['semana'] = num_semana
    fechas['dia_semana'] = dias

    # Nos guardamos para cada order_id en detalles su fecha
    # asociada

    semanas = []
    dia_semana = []

    for s in detalles['order_id']:

        indice = fechas[fechas['order_id'] == s].index
        semana = fechas['semana'].iloc[indice]
        d = fechas['dia_semana'].iloc[indice]

        semanas.append(int(semana))
        dia_semana.append(int(d))

    detalles['semana'] = semanas
    detalles['dia'] = dia_semana

    # Obtenemos todos los posibles ingredientes que emplea
    # en la elaboración de sus pizzas

    lista_ingredientes = []
    for ingrediente in ingredientes['ingredients']:
        varios = ingrediente.split(',')
        lista_ingredientes += varios

    set_ingredientes = set(lista_ingredientes)

    # Creamos una columna por cada ingrediente en detalles
    # Almacenamos en un diccionario el índice de cada ingrediente

    indices = dict()

    for i in set_ingredientes:
        detalles[i] = [0 for i in detalles.index]
        indice = detalles.columns.get_loc(i)
        indices[i] = indice

    # Para cada tipo de pizza en order detail, les sumamos
    # las cantidades a sus ingredientes correspondientes
    # Para las s sumaremos una unidad de cada ingrediente
    # Para las m sumaremos 2 y para las L sumaremos 3

    tipos_de_pizzas = pedidos['pizza_id'].tolist()
    tamanos = ['s', 'm', 'l', 'xl', 'xxl']
    ing_asociados = dict()

    for tipo in tipos_de_pizzas:

        tamano = tipo.split('_')[-1]
        ingredientes_str = ingredientes[ingredientes['pizza_type_id'] == tipo[:-len(tamano)-1]]['ingredients'].tolist()[0]
        lista_ingredientes_comprar = ingredientes_str.split(',')
        ing_asociados[tipo] = lista_ingredientes_comprar

    # Sumamos la cantidad de cada ingrediente que ha necesitado cada pedido

    for i in detalles.index:

        pedido = detalles['pizza_id'].iloc[i]
        cantidad = detalles['quantity'].iloc[i]
        ing = ing_asociados[pedido]
        tamano = pedido.split('_')[-1]

        for j in ing:
            detalles.loc[i, j] += cantidad * (tamanos.index(tamano) + 1)

    return detalles


def load_csv(datos):

    # El dataframe obtenido en el transform csv
    pass


def extract():

    # Extrae los datos finales ya trabajados de la pizería
    pass


def transform(datos):

    ingredientes = datos.columns.values
    ingredientes = ingredientes[6:]

    # Nuestro predict será la media de las modas de cada ingrediente

    suma_semana = datos.pivot_table(index='semana', aggfunc='sum')
    suma_semana_ingredientes = suma_semana[ingredientes]
    modas = suma_semana_ingredientes.mode().mean().round().tolist()

    # Creamos un dataframe con el valor calculado para cada ingrediente

    d = {'Ingredientes:': ingredientes, 'Unidades a comprar:': modas}
    res = pd.DataFrame(data=d)

    return res


def load(res):

    # Guarda el resultado como un CSV
    res.to_csv('salida.csv')


def informe_de_datos(fechas, pedidos, detalles, ingredientes):

    # Primero vemos el número de NaNs y de Nulls de cada df
    # Agregamos también el tipo de cada columna

    fichero = open('informe_calidad_datos.txt', 'w')

    dfs = [fechas, pedidos, detalles, ingredientes]
    nombres = ['orders.csv', 'pizzas.csv', 'order_details.csv', 'pizza_types.csv']

    for df in range(len(dfs)):

        nulls = dfs[df].isnull().sum()
        nans = dfs[df].isna().sum()

        fichero.write('\n\n' + nombres[df] + ':')
        fichero.write('\n\nNulls: \n' + str(nulls))
        fichero.write('\n\nNans: \n' + str(nans) + '\n')

        columnas = dfs[df].columns.values.tolist()

        for c in columnas:

            tipos = dfs[df][c].dtypes
            fichero.write('\nTipos en la columna ' + c + ':' + str(tipos))





if __name__ == '__main__':

    fechas, pedidos, detalles, ingredientes = extract_csv()
    datos = transform_csv(fechas, pedidos, detalles, ingredientes)
    res = transform(datos)
    load(res)

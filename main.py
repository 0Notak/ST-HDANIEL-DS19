from fastapi import FastAPI
import pyarrow
import pandas as pd

app=FastAPI(debug=True)

DataSet_Final = pd.read_parquet('DataSet_Final.parquet', engine='pyarrow')


@app.get('/')
def message():
    return 'PROYECTO INTEGRADOR ML OPS de Hevert Daniel Martinez (agregar /docs al enlace para acceder a las funciones / add /docs to link to access features)'


@app.get('/developer/')
def developer(desarrollador: str) -> dict:
    # Filtrar el DataFrame por el desarrollador proporcionado
    df_desarrollador = DataSet_Final[DataSet_Final['developer'] == desarrollador]

    # Sumar los valores de la columna "items" por año
    sum_items_por_ano = df_desarrollador.groupby('anio')['items_count'].sum()

    # Calcular el porcentaje de valores iguales a 0.0 en la columna "precio" por año
    total_por_ano = df_desarrollador.groupby('anio')['price'].count()
    count_zeros_por_ano = df_desarrollador[df_desarrollador['price'] == 0.0].groupby('anio')['price'].count()
    porcentaje_zeros_por_ano = (count_zeros_por_ano / total_por_ano) * 100

    # Retornar un diccionario con los resultados
    return {
        'Suma de Items Por Año': sum_items_por_ano.to_dict(),
        'Porcentaje Contenido Free': porcentaje_zeros_por_ano.to_dict()
    }

@app.get('/user_data/')
def userdata(User_id: str) -> dict:
    # Filtrar el DataFrame por el User_id proporcionado
    user_data = DataSet_Final[DataSet_Final['user_id'] == User_id]

    # Calcular la cantidad de dinero gastado por el usuario
    dinero_gastado = user_data['price'].sum()

    # Calcular el porcentaje de recomendación
    total_recomendaciones = user_data['recommend'].count()
    recomendaciones_positivas = user_data['recommend'].sum()
    porcentaje_recomendacion = (recomendaciones_positivas / total_recomendaciones) * 100

    # Calcular la cantidad de items del usuario
    cantidad_items = user_data['items_count'].sum()

    # Retornar un diccionario con los resultados
    return {
        "Usuario": User_id,
        "Dinero gastado": f"{dinero_gastado} USD",
        "% de recomendación": f"{porcentaje_recomendacion}%",
        "Cantidad de items": cantidad_items
    }

@app.get('/UserForGenre/')
def UserForGenre(genero: str) -> dict:
    # Filtrar el DataFrame por el género proporcionado
    filtered_data = DataSet_Final[DataSet_Final['genres'] == genero]

    # Agrupar por usuario y año, sumar las horas jugadas
    grouped_data = filtered_data.groupby(['user_id', DataSet_Final['release_date'].dt.year])['playtime_forever'].sum()

    # Encontrar el usuario con más horas jugadas para el género dado por año
    max_hours_per_year = grouped_data.reset_index().groupby('release_date').apply(lambda x: x.loc[x['playtime_forever'].idxmax()])

    # Construir la lista de acumulación de horas jugadas por año
    horas_jugadas = max_hours_per_year[['release_date', 'playtime_forever']].to_dict('records')

    # Obtener el usuario con más horas jugadas
    usuario_mas_horas = max_hours_per_year.iloc[0]['user_id']

    # Retornar el resultado como un diccionario
    return {
        "Usuario con más horas jugadas para Género": usuario_mas_horas,
        "Horas jugadas": horas_jugadas
    }

@app.get('/best_developer_year/')
def best_developer_year(año: int):
    # Filtrar el dataset por el año especificado
    year_data = DataSet_Final[DataSet_Final['anio'] == año]

    # Contar la cantidad de juegos recomendados por desarrollador para el año dado
    developer_recommendations = year_data['developer'].value_counts()

    # Obtener los top 3 desarrolladores con más juegos recomendados
    top_3_developers = developer_recommendations.head(3)

    # Construir la lista de retorno
    return [{"Puesto 1": top_3_developers.index[0]}, 
            {"Puesto 2": top_3_developers.index[1]}, 
            {"Puesto 3": top_3_developers.index[2]}]


@app.get('/develorper_reviews_analysis/')
def developer_reviews_analysis(desarrolladora: str):
    # Filtrar el dataset por el desarrollador especificado
    developer_data = DataSet_Final[DataSet_Final['developer'] == desarrolladora]

    # Contar la cantidad de reseñas positivas, neutras y negativas
    positive_reviews = (developer_data['sentiment_score'] == 2).value_counts()
    neutral_reviews = (developer_data['sentiment_score'] == 1).value_counts()
    negative_reviews = (developer_data['sentiment_score'] == 0).value_counts()

    # Construir el diccionario de retorno
    return {desarrolladora: {'Positive': positive_reviews, 'Neutral': neutral_reviews, 'Negative': negative_reviews}}

# Aquí puedes especificar el nombre del desarrollador que deseas analizar
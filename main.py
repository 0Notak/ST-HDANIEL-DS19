from fastapi import FastAPI
import pyarrow
import pandas as pd

app=FastAPI(debug=True)

DataSet_Final = pd.read_parquet('DataSet_Final.parquet', engine='pyarrow')


@app.get('/')
def message():
    return 'PROYECTO INTEGRADOR ML OPS de Hevert Daniel Martinez (agregar /docs al enlace para acceder a las funciones / add /docs to link to access features)'


@app.get('/developer/')
def developer(desarrollador: str):
    # Filtrar el DataFrame por la empresa desarrolladora especificada
    developer_data = DataSet_Final[DataSet_Final['developer'] == desarrollador]

    # Agrupar los datos por año y empresa desarrolladora
    grouped_data = developer_data.groupby(['anioyear'])

    # Inicializar diccionarios para almacenar la cantidad de ítems y el porcentaje de contenido gratuito por año
    cantidad_items_por_anio = {}
    porcentaje_contenido_free_por_anio = {}

    # Iterar sobre los grupos
    for year, group in grouped_data:
        # Calcular la cantidad de ítems por año
        cantidad_items = len(group['item_id'])

        # Calcular el porcentaje de contenido gratuito por año
        contenido_free = group[group['price'] == 0]
        cantidad_contenido_free = len(contenido_free)
        porcentaje_free = (cantidad_contenido_free / cantidad_items) * 100 if cantidad_items != 0 else 0

        # Almacenar los resultados en los diccionarios
        cantidad_items_por_anio[year] = cantidad_items
        porcentaje_contenido_free_por_anio[year] = porcentaje_free

    return {
        print("Cantidad de ítems por año:", cantidad_items_por_anio, 
              "Porcentaje de contenido gratuito por año:", porcentaje_contenido_free_por_anio) 
              }


@app.get('/user_data/')
def userdata(user_id:str):
    df_filtrado = DataSet_Final.loc[DataSet_Final["user_id"]== user_id]
    total_items= df_filtrado['item_id'].nunique()
    porcentaje_recomendacion= (df_filtrado['recommend'].sum() / total_items) * 100
    cantidad_dinero= df_filtrado['price'].sum()
    return {f'usuario':user_id, 'porcentaje de recomendacion':porcentaje_recomendacion, 'dinero gastado':cantidad_dinero}

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
    positive_reviews = (developer_data['sentiment_score'] == 2).sum()
    neutral_reviews = (developer_data['sentiment_score'] == 1).sum()
    negative_reviews = (developer_data['sentiment_score'] == 0).sum()

    # Construir el diccionario de retorno
    return {desarrolladora: {'Positive': positive_reviews, 'Neutral': neutral_reviews, 'Negative': negative_reviews}}
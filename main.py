from fastapi import FastAPI, HTTPException
import pandas as pd

# Leer solo las columnas necesarias del archivo parquet 'ETL-Steam_game_cleaned.parquet'
columns_steam_games = ['item_id', 'developer', 'Año', 'price']
df_steam_games = pd.read_parquet('data_deployment/ETL-Steam_game_cleaned.parquet', columns=columns_steam_games)

# Leer solo las columnas necesarias del archivo parquet 'ETL-UserItems.csv'
columns_user_items = ['item_id', 'user_id']
df_user_items = pd.read_parquet('data_deployment/ETL-User_items_cleaned.parquet', columns=columns_user_items)

# Leer solo las columnas necesarias del archivo parquet 'ETL-UserReviews.csv'
columns_user_reviews = ['item_id', 'recommend', 'sentiment_analysis']
df_user_reviews = pd.read_parquet('data_deployment/ETL-User_reviews_cleaned.parquet', columns=columns_user_reviews)

# Merge de los DataFrames
df_merged1 = pd.merge(df_steam_games, df_user_reviews, on='item_id', how='inner')
df_merged2 = pd.merge(df_steam_games, df_user_items, on='item_id', how='inner')

app = FastAPI()

# Definición de tus APIs...


#API 1

@app.get("/developer/{desarrollador}")
def developer(desarrollador: str):
    try:
        # Filtro el DataFrame por el desarrollador
        df_desarrollador = df_merged1[df_merged1['developer'] == desarrollador]
        items_xaño = df_desarrollador.groupby('Año').size()
        contenido_free_xaño = (df_desarrollador[df_desarrollador['price'] == 'Free']
                              .groupby('Año')
                              .size() / items_xaño * 100)
        
        # Redondeo los porcentajes a 2 decimales
        contenido_free_xaño = contenido_free_xaño.round(2)

        # Convierto los años a enteros
        items_xaño.index = items_xaño.index.astype(int)

        # Creao un DataFrame con los resultados
        df_resultado = pd.DataFrame({
            'Número de Juegos': items_xaño,
            'Porcentaje de Juegos Gratuitos (%)': contenido_free_xaño
        })
        
        # Reemplazo NaN con 0
        df_resultado = df_resultado.fillna(0)  

        # Converto el DataFrame a un diccionario con orientación 'index'
        return df_resultado.to_dict(orient='index')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#API 2

@app.get('/userdata/{User_id}')
def userdata(User_id: str):
    try:
        # Filtro el DataFrame por el User_id proporcionado
        user_data = df_merged1[df_merged1['user_id'] == User_id]
        
        # Calculo la cantidad de dinero gastado por el usuario
        dinero_gastado = user_data['price'].sum()
        
        # Calculo el porcentaje de recomendación en base a reviews.recommend
        porcentaje_recomendacion = round(user_data['recommend'].mean() * 100)
        
        # Calculo la cantidad de items del usuario
        cantidad_items = len(user_data)
        
        # Creo el diccionario de retorno
        return {
            "Usuario": User_id,
            "Dinero gastado": f"{dinero_gastado:.2f} USD",
            "% de recomendación": f"{porcentaje_recomendacion}%",
            "Cantidad de items": cantidad_items
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#API 3 



# Supongamos que ya tienes cargado tu DataFrame df_merged2 con las columnas necesarias

@app.get("/UserForGenre/{genero}")
def UserForGenre(genero: str):
    # Filtrar los datos para el género dado
    df_filtered = df_merged2[df_merged2['genres'] == genero]
    
    # Encontrar el usuario con más horas jugadas para el género dado
    usuario_max_horas = df_filtered.loc[df_filtered['playtime_forever'].idxmax()]['user_id']
    
    # Calcular la acumulación de horas jugadas por año de lanzamiento
    horas_por_año = df_filtered.groupby('Año')['playtime_forever'].sum().reset_index()
    horas_por_año = horas_por_año.rename(columns={'Año': 'Año', 'playtime_forever': 'Horas'})
    
    # Convertir los resultados a una lista de diccionarios
    horas_por_año_list = horas_por_año.to_dict(orient='records')
    
    # Crear el diccionario de retorno con el formato esperado
    retorno = {"Usuario con más horas jugadas para Género {}: ".format(genero): usuario_max_horas, "Horas jugadas": horas_por_año_list}
    
    return retorno



#API 4

@app.get("/best_developer_year/{año}")
def best_developer_year(año: int):
    # Filtro los datos para el año dado
    df_filtered = df_merged1[df_merged1['Año'] == año]
    
    # Agrupo por desarrollador y contar la cantidad de juegos recomendados
    developer_stats = df_filtered.groupby('developer')['sentiment_analysis'].sum().sort_values(ascending=False)
    
    # Seleccio los 3 mejores desarrolladores
    top_developers = developer_stats.head(3).index.tolist()
    
    # Creo el formato de retorno
    retorno = [{"Puesto {}: ".format(i+1): developer} for i, developer in enumerate(top_developers)]
    
    return retorno



#API 5 

@app.get("/developer_reviews_analysis/{desarrollador}")
def developer_reviews_analysis(desarrollador: str):
    try:
        # Filtro los datos para el desarrollador dado
        df_filtered = df_merged1[df_merged1['developer'] == desarrollador]
    
        # Cuento la cantidad de registros de reseñas positivas y negativas
        positive_reviews = df_filtered[df_filtered['sentiment_analysis'] > 0].shape[0]
        negative_reviews = df_filtered[df_filtered['sentiment_analysis'] < 0].shape[0]
    
        # Creo el diccionario de retorno con el formato esperado
        retorno = {desarrollador: {'Negative': negative_reviews, 'Positive': positive_reviews}}
    
        return retorno
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")


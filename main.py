import pandas as pd
import glob
class proceso_ETL:    
    
    def cargar_ratings():
        listacsv = glob.glob('**/[1-8].csv', recursive=True)
        df = pd.DataFrame(pd.read_csv(listacsv[0]))

        for i in range(1,len(listacsv)):
            data = pd.read_csv(listacsv[i])
            df = pd.DataFrame(data)
            df = pd.concat([df,df])

        return df

    def cargar_peliculas():
        ruta_carpeta = 'MLOpsReviews'
        lista_csv = glob.glob(ruta_carpeta + "/*.csv")

        ids = 'adhn'

        peliculas = pd.DataFrame({})

        for i in range(len(lista_csv)):
            #Leer cada csv
            data = pd.read_csv(lista_csv[i],parse_dates=['date_added'])
            df = pd.DataFrame(data)

            #Eliminar datos con duracion nula
            df = df[df.duration.notna()]

            #Cambiar columna 'show_id' por 'id' con identificador de plataforma 
            df.insert(1,'id',ids[i]+df.show_id)
            
            df.rating.fillna('G',inplace=True)

            df = df.apply(lambda x: x.astype(str).str.lower())

            df[['duration_int','duration_type']] = df.duration.str.split(expand=True)
            df.duration_int = df.duration_int.astype('int')
            df.drop(columns=['show_id','duration'],inplace=True)


            peliculas = pd.concat([peliculas,df],axis=0)

        peliculas.date_added = pd.to_datetime(peliculas.date_added,format = "%Y-%m-%d")
        peliculas.release_year = peliculas.release_year.astype('int')

        return peliculas

peliculas = proceso_ETL.cargar_peliculas()
ratings = proceso_ETL.cargar_ratings()

from fastapi import FastAPI

app = FastAPI()

@app.get('/')
def index():
    msg = "Esta API dispone de cuatro rutas\n \n   /duracion\n/peliculas_por_puntaje\n/peliculas_plataforma\n/actor\n"
    msg2 = "Para mayor infromacion consultas https://app/docs"
    return msg + msg2    

@app.get('/duracion')
def get_max_duration(year: int,platform: str,duration_type: str):
    '''Función para obtener pelicula con máxima duración con filtros de
    año, plataforma y tipo de audiovisual(Pelicula o Serie) '''
    platform = platform[0]
    if platform not in 'adhn':
        return 'El campo platform solo admite los valores amazon,disney,hulu o netflix'
    
    if duration_type == 'min':
        df = peliculas[(peliculas.id.str.startswith(platform)) & (peliculas.type== 'movie') & (peliculas.release_year == year)] 
        
    elif duration_type == 'season':
        df = peliculas[(peliculas.id.str.startswith(platform)) & (peliculas.type== 'tv show') & (peliculas.release_year == year)]
    else: 
        return 'El campo duration_type solo admite los valores "min" o "season"'

    idx = df.duration_int.idxmax()        
    return df.title[idx] 
    

@app.get('/peliculas_por_puntaje')
def get_score_count(platform,scored: float,year: int):
    '''Función para obtener el número de  peliculas con un puntaje mayor a un valor especifico
    en un año determinado '''
    return 'f'

@app.get('/peliculas_plataforma')
def get_platform_count(platform: str):
    '''Función para obtener el número de peliculas disponibles de una plataforma
    '''
    platform = platform[0]
    if platform not in 'adhn':
        return 'El campo platform solo admite los valores amazon,disney,hulu o netflix'
    
    return peliculas[peliculas.id.str.startswith(platform)].id.count()

@app.get('/actor')
def get_actor(platform,year: int):
    ''' Función para obtener el actor que mas interpretaciones ha realizado en una plataforma y año especifico'''
    platform = platform[0]
    if platform not in 'adhn':
        return 'El campo platform solo admite los valores amazon,disney,hulu o netflix'
    
    actores = plataformas[(plataformas.id.str.startswith(platform)) & (plataformas.release_year == year)].cast
    actores = actores.str.split(", ").explode()
    actores = actores.value_counts().drop(labels='nan')

    return actores.index[0]
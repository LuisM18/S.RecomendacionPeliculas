import pandas as pd
class proceso_ETL():    
    import glob

    def cargar_ratings(self):
        listacsv = self.glob.glob('**/[1-8].csv', recursive=True)
        df = pd.DataFrame(pd.read_csv(listacsv[0]))

        for i in range(1,len(listacsv)):
            data = pd.read_csv(listacsv[i])
            df = pd.DataFrame(data)
            df = pd.concat([df,df])

        return df

    def cargar_peliculas(self):
        ruta_carpeta = 'MLOpsReviews'
        lista_csv = self.glob.glob(ruta_carpeta + "/*.csv")

        ids = ['a','d','h','n']

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
def get_max_duration(year: int,platform,duration_type):
    '''Función para obtener pelicula con máxima duración con filtros de
    año, plataforma y tipo de audiovisual(Pelicula o Serie) '''
    
    return 'f'

@app.get('/peliculas_por_puntaje')
def get_score_count(platform,scored: float,year: int):
    '''Función para obtener el número de  peliculas con un puntaje mayor a un valor especifico
    en un año determinado '''
    return 'f'

@app.get('/peliculas_plataforma')
def get_platform_count(platform: str):
    '''Función para obtener el número de peliculas disponibles de una plataforma
    '''
    return 'f'

@app.get('/actor')
def get_actor(platform,year: int):
    ''' Función para obtener el actor que mas interpretaciones ha realizado en una plataforma y año especifico'''
    return 'f'
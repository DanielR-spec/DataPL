from sqlalchemy import create_engine
import psycopg2


uid = "motivy_user"
pwd = "53745344-e5dc-4406-a163-d765c3bf5ecf"
host = "motivy-prod-postgress.cgyidm3l3vnq.us-east-1.rds.amazonaws.com"
port = 5432
db = "motivy"

'''
La clase Db contiene los metodos de acceso a la base de datos PostgreSQL
'''


class Db:
    '''
    La funcion init() instancia y crea la conexion hacia la base de datos de PostgreSQL
    '''

    def __init__(self):
        self.connection = create_engine(
            f'postgresql://{uid}:{pwd}@{host}:{port}/{db}')

    '''
    La funcion load() carga la informacion de carda archivo a la base de datos 
    PostgreSQL
    Parametros: df (dataframe), representa el archivo a ser almacenado
    '''

    def load(self, df):

        try:
            print(f'Importando filas... hacia tabla precios')
            df.to_sql(name='precios', con=self.connection,
                      if_exists='append', index=False)

            filas = len(df)
            maximo = df["price"].max()
            minimo = df["price"].min()
            medio = df["price"].mean()

            print("Numero de filas almacenadas: " + str(filas))
            print("Valor maximo: " + str(maximo))
            print("Valor minimo: " + str(minimo))
            print("Valor medio: " + str(medio))
            print("_______________\n")

        except Exception as err:
            raise

    '''
    La funcion getData() retorna la informacion de la base de datos 
    PostgreSQL.
    '''

    def getData(self):
        conexion = psycopg2.connect(host=host,
                                    database=db,
                                    user=uid,
                                    password=pwd)

        cur = conexion.cursor()

        cur.execute("select count(price) as total from public.precios")
        totalFilas = cur.fetchone()

        cur.execute("select avg(price) as promedio from public.precios")
        promPrecio = cur.fetchone()

        cur.execute("select min(price) as minimo from public.precios")
        minPrecio = cur.fetchone()

        cur.execute("select max(price) as maximo from public.precios")
        maxPrecio = cur.fetchone()

        print("Procesamiento de archivos finalizados")
        print("Estadisticas finales para el campo price:")
        print("Numero de filas almacenadas: " + str(totalFilas))
        print("Valor maximo: " + str(maxPrecio))
        print("Valor minimo: " + str(minPrecio))
        print("Valor promedio: " + str(promPrecio))
        print("_______________\n")

        conexion.close()

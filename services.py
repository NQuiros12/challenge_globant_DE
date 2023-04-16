from db import Connector
from fastapi import File, UploadFile
import pandas as pd
from sqlalchemy import create_engine

DB_USER ="root"
DB_PASSWORD = "micolash12"
DB_HOST = "localhost"
DATABASE ="globant_db"
DB_PORT = 3306
	

#connector = Connector(DATABASE, DB_HOST, DB_USER, DB_USER,DB_PASSWORD, DB_PORT)
#Create the connection up and running
#
async def get_all(table_name:str)->list:
    try:
        connector.get_connection()
        cursor = connector.cursor(dictionary=True)
        cursor.execute("select * from %s" % (table_name))
        rows = cursor.fetchall()
        return rows
    except mysql.connector.Error as e:
        return JSONResponse(status_code = 500, content = { "message":"Error connecting to database"})

#Create the service of reading the csv
def read_csv(file:UploadFile = File()) -> pd.DataFrame:
    df = pd.read_csv(file)
    return df
#Upload the file to the database
def upload_csv(df:pd.DataFrame,table_name:str) ->None:
	connect_string = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DATABASE}?charset=utf8'
	engine = create_engine(connect_string)
	# Use the built-in function 'to_sql' to write the dataframe to the database
	df.to_sql(table_name, engine, if_exists='replace', index=False)
	print("Csv upload to database")
	return None
def get_all_files(dir:str) -> list:
    #Return all files names from the data dir without extension
    files = list(map(lambda x: os.path.basename(x).replace(".csv",""),os.listdir("data")))
    return files
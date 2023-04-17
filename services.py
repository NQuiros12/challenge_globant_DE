from fastapi import File, UploadFile
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import os
DB_USER = "root"
DB_PASSWORD = "micolash12"
DB_HOST = "localhost"
DATABASE = "globant_db"
DB_PORT = 3306
connect_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DATABASE}?charset=utf8"
engine = create_engine(connect_string)
# Create the connection up and running
def get_all_files(dir: str) -> list:
    # Return all files names from the data dir without extension
    files = list(
        map(lambda x: os.path.basename(x).replace(
            ".csv", ""), os.listdir("data"))
    )
    return files

async def upload_csv(file: UploadFile = File(...))->str:
    # create a path to store the file in the data directory
    file_path = os.path.join("data", file.filename)

    # open the file and write the contents to the specified path
    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    return {"filename": file.filename}
# Create the service of reading the csv
def read_csv(file: UploadFile = File()) -> pd.DataFrame:
    df = pd.read_csv(file)
    return df


# Upload the file to the database (batch)
def upload_df_to_sql(df: pd.DataFrame, table_name: str) -> None:

    # Use the built-in function 'to_sql' to write the dataframe to the database
    df.to_sql(table_name, engine, if_exists="append",
              index=False, chunksize=1000)
    print(f"{table_name} csv upload to database")
    return None
def add_constraints(table_name:str)->None:
    # Use the engine to write the primary key constraint to the database
    with engine.connect() as con:
        # and in the case of 'hired_employees' we add also a foreign key constraint
        con.execute(sa.text(f"alter table {table_name} add primary key (id);"))
        if ("hired_employees_t" in table_name):
            con.execute(sa.text(
                f"ALTER TABLE {table_name} ADD CONSTRAINT department_fk FOREIGN KEY(department_id) references departments_t(id)"
            ))
            con.execute(sa.text(
                f"ALTER TABLE {table_name} ADD CONSTRAINT job_fk FOREIGN KEY(job_id) references jobs_t(id)"
            ))
        
from fastapi import File, UploadFile
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from sqlalchemy import create_engine
import os
import datetime as dt
import configparser

# Read the config file
config = configparser.ConfigParser()
config.read('database.ini')

# Get the database host
host = config['database']['host']

# Get the database port
port = config['database']['port']

# Get the database name
database = config['database']['database']

# Get the database user
user = config['database']['user']

# Get the database password
password = config['database']['password']
schema_hired = {
    "id": "Int64",
    "name": "object",
    "datetime": "object",
    "department_id": "Int64",
    "job_id": "Int64"
}


schema_jobs = {
    "job_id": "Int64",
    "job": "object"
}
schema_department = {
    "department_id": "Int64",
    "department": "object"
}
schemas = [schema_hired, schema_jobs, schema_department]

connect_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8"
engine = create_engine(connect_string)
# Create the connection up and running


def get_all_files(dir: str) -> list:
    # Return all files names from the data dir without extension
    files = [f.replace(".csv", "")
             for f in os.listdir(dir) if not f.startswith('.')]
    return files


async def upload_csv(file: UploadFile = File(...)) -> str:
    # create a path to store the file in the data directory
    file_path = os.path.join("data", file.filename)

    # open the file and write the contents to the specified path
    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())

    return {"filename": file.filename}
# Create the service of reading the csv


def read_csv_cust(schema: str, file: str) -> pd.DataFrame:
    if "hired_employees" in file:
        df = pd.read_csv(file, names=schema_hired.keys(), dtype=schema_hired)
        df["datetime"] = pd.to_datetime(
            df["datetime"])  # ,parse_dates=["datetime"]
        return df
    df = pd.read_csv(file, names=schema.keys(), dtype=schema)
    return df


# Upload the file to the database (batch)
def upload_df_to_sql(df: pd.DataFrame, table_name: str) -> None:

    # Use the built-in function 'to_sql' to write the dataframe to the database
    df.to_sql(table_name, engine, if_exists="replace",
              index=False, chunksize=1000)
    print(f"{table_name} csv upload to database")
    return None


def add_constraints(table_name: str) -> None:
    # Use the engine to write the primary key constraint to the database
    add_pk(table_name)


def add_fk(table_name: str = "hired_employees") -> None:
    with engine.connect() as con:
        con.execute(sa.text(
            f"ALTER TABLE hired_employees ADD CONSTRAINT department_id FOREIGN KEY(department_id) references departments(department_id)"
        ))
        con.execute(sa.text(
            f"ALTER TABLE hired_employees ADD CONSTRAINT job_id FOREIGN KEY(job_id) references jobs(job_id)"
        ))


def add_pk(table_name: str) -> None:
    with engine.connect() as con:
        if ("hired_employees" not in table_name):
            con.execute(
                sa.text(f"alter table {table_name} add primary key ({table_name[:-1]}_id);"))
            print(
                f"alter table {table_name} add primary key ({table_name[:-1]}_id);")
            return None
        else:
            con.execute(
                sa.text(f"alter table hired_employees add primary key (id);"))
            return None


def batch_upload():

    # Read all the csv files in the 'data' directory
    file_paths = [
        f"../data/{filename}.csv" for filename in get_all_files("../data")]
    # Re arrange the file paths in order to execute first with 'jobs' and 'departments' and after the
    # 'hired_employees' since the last one depends on the others.
    file_paths[1], file_paths[-1] = file_paths[-1], file_paths[1]
    [upload_df_to_sql(read_csv_cust(schema=schema, file=file_path), Path(
        file_path).stem) for file_path, schema in zip(file_paths, schemas)]
    # Add the constraints to the sql database
    [add_constraints(Path(file_path).stem)for file_path in file_paths]
    # and in the case of 'hired_employees' we add also a foreign key constraint
    add_fk()


def employees_x_job_department_2021():
    query = """
        select  
            d.department,
		    j.job,
            QUARTER(h.`datetime`) as quarter_id,
            count(0) as hires
        from hired_employees h
        inner join jobs j 
            on h.job_id = j.job_id
        inner join departments d
            on h.department_id = d.department_id
        where year(h.datetime) = 2021
        group by d.department,j.job,  QUARTER(h.`datetime`)
        order by d.department,d.department ASC;"""
    with engine.connect() as con:
        result = con.execute(sa.text(query))
        keys = ["department", "job", "quarter_id", "hires"]
        rows = [dict(zip(keys, row)) for row in result.fetchall()]
    return rows


def duplicates_tb() -> int:
    query = """select
                    count(*)
                from
                    hired_employees
                where
                    id in ( select id from
                            ( select
                                id,
                                row_number() over (partition by id order by id) as rn
                                from
                                hired_employees
                                ) t
                where
                rn > 1
                );"""
    with engine.connect() as con:
        result = con.execute(sa.text(query))
        return result.fetchone()[0]


def number_hireds_by_department():
    query = """
       with base as(
				select 
                d.department_id,
                d.department,
                count(*) as number_hired
				from hired_employees h
				inner join departments d
					on h.department_id = d.department_id
				where year(h.datetime) = 2021
				group by d.department_id)
        select
	        *
	    from base
	    where number_hired > (
			        select avg(number_hired)
			        from base
                    )"""
    with engine.connect() as con:
        result = con.execute(sa.text(query))
        keys = ["id_department", "department", "hires"]
        rows = [dict(zip(keys, row)) for row in result.fetchall()]
    return rows

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pathlib import Path

import services as _services

def batch_upload():
    file_paths = [f"data/{filename}.csv" for filename in _services.get_all_files("data")]
    #Re arrange the file paths in order to execute first with 'jobs' and 'departments' and after the
    # 'hired_employees' since the last one depends on the others.
    file_paths[1], file_paths[-1] = file_paths[-1], file_paths[1]

    # Bring all the files and upload to the sql database
    [_services.upload_df_to_sql(_services.read_csv(file_path),Path(file_path).stem ) for file_path in file_paths]
    [_services.add_constraints(Path(file_path).stem)for file_path in file_paths]


#Create the app
app = FastAPI()

@app.get("/test_connection",response_class = JSONResponse)
async def test_connection():
    data = {"message": "Connection working"}
    headers = {"X-Test": "test"}
    return JSONResponse(content=data, status_code=200, headers=headers)
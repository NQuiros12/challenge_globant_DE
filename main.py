from fastapi import FastAPI
from fastapi import UploadFile, File
from fastapi.responses import JSONResponse
from typing import List
import services as _services
import shutil
# Create the app

app = FastAPI()


@app.get("/test_connection", response_class=JSONResponse)
async def test_connection():
    data = {"message": "Connection working"}
    headers = {"X-Test": "test"}
    return JSONResponse(content=data, status_code=200, headers=headers)


@app.post("/upload_csv")
def upload(files: List[UploadFile] = File(...)):
    for file in files:
        with open("./data/"+file.filename, "wb") as buffer:
            shutil.copyfileobj(fsrc=file.file, fdst=buffer)
    data = {"message": f"{file.filename} successfully uploaded"}
    return JSONResponse(content=data, status_code=200)


@app.get("/batch_sql")
async def batch_sql():
    _services.batch_upload()
    data = {"message": "Batch successfully uploaded"}
    return JSONResponse(content = data, status_code = 200)

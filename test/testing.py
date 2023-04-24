from fastapi.testclient import TestClient
import os
import sys
import requests
# Add the parent directory to sys.path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)
from main import app
data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app', 'data'))
import services
client = TestClient(app)

def test_test_connection():
    response = client.get("/test_connection")
    assert response.status_code == 200
    assert response.json() == {"message": "Connection working"}

def test_upload_csv():
    url = 'http://localhost:8000/upload_csv'
    files = {'files': ('test.csv', open('./test/test.csv', 'rb'), 'text/csv')}
    response = requests.post(url, files=files)
    assert response.status_code == 200

def test_duplicates_sql():
    assert services.duplicates_tb() == 0
def test_has_rows():
    assert services.table_has_rows("hired_employees") == 1
    assert services.table_has_rows("jobs") == 1
    assert services.table_has_rows("departments") == 1
def test_query1():
    response = client.get("/query1")
    assert response.status_code == 200

def test_query2():
    response = client.get("/query2")
    assert response.status_code == 200
    # Add assertions for the expected results of the query

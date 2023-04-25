import requests
from services import delete_all_tables
delete_all_tables()
url = 'http://localhost:8000/upload_csv'
    #Test the upload_csv endpoint
files = {'files': ('hired_employees.csv', open('./test/hired_employees.csv', 'rb'), 'text/csv'),
             'files': ('jobs.csv', open('./test/jobs.csv', 'rb'), 'text/csv'),
             'files': ('departments.csv', open('./test/departments.csv', 'rb'), 'text/csv')
             }
response = requests.post(url, files=files)
requests.get("http://localhost:8000/batch_sql")


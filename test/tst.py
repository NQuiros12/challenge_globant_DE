import requests

url = 'http://localhost:8000/upload_csv'
files = {'files': ('test.csv', open('./test.csv', 'rb'), 'text/csv')}
response = requests.post(url, files=files)

print(response.status_code)
print(response.json())

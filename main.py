import services as _services

import os

uri = "data/hired_employees_t.csv"
table_name = os.path.basename(uri).replace(".csv","")
_services.upload_csv(_services.read_csv(uri), table_name)

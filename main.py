import services as _services
file_paths = [f"data/{filename}.csv" for filename in _services.get_all_files("data")]
#Re arrange the file paths in order to execute first with 'jobs' and 'departments' and after the
# 'hired_employees' since the last one depends on the others.
file_paths[1], file_paths[-1] = file_paths[-1], file_paths[1]
from pathlib import Path
# Bring all the files and upload to the sql database
[_services.upload_csv(_services.read_csv(file_path),Path(file_path).stem ) for file_path in file_paths]
[_services.add_constraints(Path(file_path).stem)for file_path in file_paths]
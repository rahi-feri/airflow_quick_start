
from datetime import datetime
import os

def extract_website_sales_data(your_input_value: int):
    # Simulate extraction from a website database or API
    print("Extracting website sales data...")
    # In a real scenario, this would involve database queries or API calls
    with open('/media/rahi/hdd5002/airflow/airflow_docker3/projects/project_sales_etl/file_inside.txt', 'w') as f:
        f.writelines(f'today is: {datetime.today()} and the input value is {your_input_value}')
    pass

arr = os.listdir()
print(arr)


extract_website_sales_data(10)
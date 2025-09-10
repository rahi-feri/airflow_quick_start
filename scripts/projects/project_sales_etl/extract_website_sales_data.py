from datetime import datetime
import sys
from zoneinfo import ZoneInfo
import os 
# dir_path = os.path.dirname(os.path.realpath(__file__))

path = '/opt/airflow/scripts'

# get argument which passed to bash command
kwargs={arg.split(':')[0]:arg.split(':')[1] for arg in sys.argv if ':' in arg}
# print(args, sys.argv)

def extract_website_sales_data(your_a: int, your_b: int):
    # Simulate extraction from a website database or API
    print("Extracting website sales data...")
    # In a real scenario, this would involve database queries or API calls
    return_value = f'today is: {datetime.now()} and the input value is {your_a} and {your_b} ==> {kwargs}'
    with open(f'{path}/file_inside.txt', 'a') as f:
        f.writelines(f'\n{return_value}')

    pass

extract_website_sales_data(**kwargs)


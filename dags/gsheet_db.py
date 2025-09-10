from airflow.decorators import dag, task
from pendulum import datetime, duration

def on_failure(context):
    '''
        sending message, email, slack message, ...
    '''
    msg = context['task_instance'] + 'working'
    with open(f'scripts/errors.txt', 'w') as f:
        f.writelines(msg)

@dag(
    start_date=datetime(2025, 1, 1, 10, 30),
    schedule=duration(days=1),
    catchup=False,
    tags=['gsheet', 'etl'],
    default_args={
        'retries': 0,
        'retry_delay': duration(minutes=1),
        'max_retry_delay': duration(hours=1)
        },
    dagrun_timeout=duration(minutes=20),
    # max_consecutive_failed_dag_runs=2, # if there is more than 2 fails in a row, pause the dag
    max_active_runs=1, # 1 execution at the same time is allowed
    on_failure_callback=on_failure
)
def gsheet_to_db():
    
    @task
    def read_data_from_gsheet():
        raise Exception('sorry')
        pass

    @task
    def write_to_database():
        pass

    read_data_from_gsheet() >> write_to_database()

gsheet_to_db()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta

default_args = {
    'owner': 'UserExample',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'tags': ['etl']
}

with DAG('ETL_CSV_Postgres', 
         default_args=default_args,
         description='Extract, Transform, Data load Postgres',
         schedule_interval=None) as dag:

    def _extractApi():
        import requests
        #Generate a mock api at https://www.mockaroo.com
        url = 'your url'
        headers = {'X-API-Key': 'your API key'}
        response = requests.get(url, headers=headers)
        with open('/tmp/users_sales.csv', 'wb') as file:
            file.write(response.content)
            file.close()
    
    def _transformData():
        import pandas as pd
        df = pd.read_csv('/tmp/users_sales.csv')
        df.drop(columns=['id'], inplace=True)
        df.columns=[i.lower() for i in df.columns]     
        df = df.rename(columns={'date':'date_'})
        df.to_csv('/tmp/filter_data.csv')
        print(df.head())

        

    def _filterData():
        import pandas as pd
        df = pd.read_csv('/tmp/filter_data.csv')
        fromd = '2021-12-31'
        tod='2023-01-01'
        df = df[(df['date_']>fromd)&(df['date_']<tod)]
        df.to_csv('/tmp/sales_2022.csv', sep='\t', index=False, header=False)
        df.to_csv('sales_2022.csv')	
        print(df.head())
    
    def _loadData():   
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgresConnection')
        pg_hook.bulk_load(table="sales_db", tmp_file='/tmp/sales_2022.csv')
    
    extractDataAPi = PythonOperator(
        task_id='extractDataAPi',
        python_callable=_extractApi
    )
    
    
    transformData = PythonOperator(
        task_id='transformData',
        python_callable=_transformData
    )

    filterData = PythonOperator(
        task_id='filterData',
        python_callable=_filterData
    )
    
    createTablePostgres = PostgresOperator(
        task_id='createTablePostgres',
        postgres_conn_id='postgresConnection',
        sql='sql/create_postgres_table.sql'
    )
    
    loadDataPostgres = PythonOperator(
        task_id='loadDataPostgres',
        python_callable=_loadData
    )
    
    [extractDataAPi] >> transformData >> filterData >> createTablePostgres >> loadDataPostgres
    
            
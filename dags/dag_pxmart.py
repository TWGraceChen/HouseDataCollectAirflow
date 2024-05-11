from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sql,notifier
from datetime import datetime
import requests
import json,time

###########schema###########
schema = {
   "pxmart":{
        "id":"int",
        "code":"int",
        "city":"char(10)",
        "area":"char(10)",
        "street":"varchar(60)",
        "name":"varchar(30)",
        "description":"varchar(100)",
        "address":"varchar(200)",
        "startDate":"char(5)",
        "endDate":"char(5)",
        "phone":"char(15)",
        "location":"point",
        "services":"varchar(200)"
    }
}
############################
def recreateTable(table_name): 
    print("drop and create table:",table_name)
    # connect to postgresdb
    print("Connect to postgresdb")
    pg_hook = PostgresHook(postgres_conn_id='postgresdb')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # drop table if exist
    drop_sql = sql.dropstate(table_name)
    cursor.execute(drop_sql)

    create_sql = sql.createstate(table_name,schema[table_name])
    cursor.execute(create_sql)
    connection.commit()    
    cursor.close()
    connection.close()



def etl(table_name):
    # connect to postgresdb
    print("Connect to postgresdb")
    pg_hook = PostgresHook(postgres_conn_id='postgresdb')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()


    # request data
    url = "https://www.pxmart.com.tw/api/stores"
    response = requests.request("POST", url)
    stores = json.loads(response.content)['data']
    for store in stores:
        id = store['id']
        code = store['attributes']['code']
        city = store['attributes']['city']
        area = store['attributes']['area']
        street = store['attributes']['street']
        name = store['attributes']['name']
        description = store['attributes']['description']
        address = store['attributes']['address']
        startDate = store['attributes']['startDate']
        endDate = store['attributes']['endDate']
        phone = store['attributes']['phone']
        location = "point({},{})".format(store['attributes']['longitude'],store['attributes']['latitude'])
        servises_set = []
        for service in store['attributes']['services']['data']:
            servises_set.append(service['attributes']['title'])
        services = ",".join(servises_set)
        # insert data
        insert_sql = "insert into {} (id,code,city,area,street,name,description,address,startDate,endDate,phone,location,services) values ({},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}',{},'{}')".format(table_name,id,code,city,area,street,name,description,address,startDate,endDate,phone,location,services)
        cursor.execute(insert_sql)

    connection.commit()    
    cursor.close()
    connection.close()

    


with DAG(
    dag_id = "pxmart",
    description='全聯座標',
    start_date = datetime(2024, 1, 1),
    schedule_interval="@weekly",
    #on_failure_callback=notifier.FailNotifier(message="RUN: {{ logical_date }}"),
    catchup = False
) as dag:
    recreate_table = PythonOperator(
        task_id="recreate_table",
        op_kwargs={"table_name":"pxmart"},
        python_callable=recreateTable
    )
    etl = PythonOperator(
        task_id="etl",
        op_kwargs={"table_name":"pxmart"},
        python_callable=etl
    )

    recreate_table  >> etl


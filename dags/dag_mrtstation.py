from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sql,notifier
from datetime import datetime
import requests
from io import StringIO
import csv

###########schema###########
schema = {
    "mrtstation":{
        "id":"int",
        "station":"varchar(50)",
        "stationid":"varchar(3)",
        "location":"point",
        "accessibility": "boolean",
        "updatetime": "timestamp default current_timestamp"
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
    # request data
    url = "	https://data.taipei/api/dataset/cfa4778c-62c1-497b-b704-756231de348b/resource/307a7f61-e302-4108-a817-877ccbfca7c1/download"
    req = requests.get(url)
    url_content = req.content.decode('big5')
    f = StringIO(url_content)
    reader = csv.reader(f, delimiter=',')
    
    # connect to postgresdb
    print("Connect to postgresdb")
    pg_hook = PostgresHook(postgres_conn_id='postgresdb')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    # process data
    skip_header = True
    for row in reader:
        if skip_header:
            skip_header = False
            continue
        id = row[0]
        station = row[1]
        stationid = row[2]
        location = "point({},{})".format(row[3],row[4])
        accessibility =  True if row[5] == "是" else False
        
        # insert data
        insert_sql = "insert into {} (id,station,stationid,location,accessibility) values ({},'{}','{}',{},'{}')".format(table_name,id,station,stationid,location,accessibility)
        cursor.execute(insert_sql)
    
    connection.commit()    
    cursor.close()
    connection.close()



with DAG(
    dag_id = "mrtstation",
    description='臺北捷運車站出入口座標',
    start_date = datetime(2024, 1, 1),
    schedule_interval="@weekly",
    on_failure_callback=notifier.FailNotifier(message="RUN: {{ logical_date }}"),
    catchup = False
) as dag:
    recreate_table = PythonOperator(
        task_id="recreate_table",
        op_kwargs={"table_name":"mrtstation"},
        python_callable=recreateTable
    )


    etl = PythonOperator(
        task_id="etl",
        op_kwargs={"table_name":"mrtstation"},
        python_callable=etl
    )

    recreate_table  >> etl



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sql,notifier
from datetime import datetime
import requests
import xml.etree.ElementTree as ET

###########schema###########
schema = {
    "shopseven":{
        "cityid":"char(2)",
        "city":"char(10)",
        "towmid":"char(2)",
        "townname":"char(10)",
        "townlocation":"point",
        "poiid":"int",
        "poiname":"varchar(20)",
        "location":"point",
        "poitelno":"varchar(20)",
        "poifaxno":"varchar(20)",
        "poiaddress":"varchar(200)",
        "poiimg":"varchar(2000)",
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
    # connect to postgresdb
    print("Connect to postgresdb")
    pg_hook = PostgresHook(postgres_conn_id='postgresdb')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    url = "https://emap.pcsc.com.tw/EMapSDK.aspx"
    citylist = {'01':'台北市','02':'基隆市','03':'新北市','04':'桃園市','05':'新竹市','06':'新竹縣','07':'苗栗縣','08':'台中市','10':'彰化縣','11':'南投縣','12':'雲林縣','13':'嘉義市','14':'嘉義縣','15':'台南市','17':'高雄市','19':'屏東縣','20':'宜蘭縣','21':'花蓮縣','22':'台東縣','23':'澎湖縣','24':'連江縣','25':'金門縣'}    
    for cityid in citylist:
        city = citylist[cityid]
        # get town in city
        payload={'commandid': 'GetTown','cityid': cityid}
        response = requests.request("POST", url,data=payload)
        townxml = ET.fromstring(response.text)
        for x in townxml.findall('GeoPosition'):
            towmid = x.find("TownID").text
            townname = x.find("TownName").text
            townlocation = "point({},{})".format(float(x.find("X").text) / 1000000,float(x.find("Y").text) / 1000000)

            # get shop in town
            payload={'commandid': 'SearchStore','city': city,'town': townname}
            response = requests.request("POST", url, data=payload)
            shopxml = ET.fromstring(response.text)
            for y in shopxml.findall('GeoPosition'):
                poiid = y.find("POIID").text.strip()
                poiname = y.find("POIName").text
                location = "point({},{})".format(float(y.find("X").text) / 1000000,float(y.find("Y").text) / 1000000)
                poitelno = y.find("Telno").text.strip()
                poifaxno = y.find("FaxNo").text.strip()
                poiaddress = y.find("Address").text
                poiimg = y.find("StoreImageTitle").text
                # insert data
                insert_sql = "insert into {} (cityid,city,towmid,townname,townlocation,poiid,poiname,location,poitelno,poifaxno,poiaddress,poiimg) values ('{}','{}','{}','{}',{},{},'{}',{},'{}','{}','{}','{}')".format(table_name,cityid,city,towmid,townname,townlocation,poiid,poiname,location,poitelno,poifaxno,poiaddress,poiimg)
                cursor.execute(insert_sql)
    
    connection.commit()    
    cursor.close()
    connection.close()



with DAG(
    dag_id = "shopseven",
    description='便利商店7-11座標座標',
    start_date = datetime(2024, 1, 1),
    schedule_interval="@weekly",
    on_failure_callback=notifier.FailNotifier(message="RUN: {{ logical_date }}"),
    catchup = False
) as dag:
    recreate_table = PythonOperator(
        task_id="recreate_table",
        op_kwargs={"table_name":"shopseven"},
        python_callable=recreateTable
    )
    etl = PythonOperator(
        task_id="etl",
        op_kwargs={"table_name":"shopseven"},
        python_callable=etl
    )

    recreate_table  >> etl




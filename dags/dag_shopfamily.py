from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sql,notifier
from datetime import datetime
import requests
import json,time

###########schema###########
schema = {
     "shopfamily":{
        "city":"char(20)",
        "town":"char(10)",
        "name":"varchar(30)",
        "tel":"varchar(20)",
        "postel":"varchar(20)",
        "location": "point",
        "addr":"varchar(200)",
        "serid":"float",
        "pkey":"varchar(10)",
        "oldpkey":"varchar(10)",
        "post":"char(3)",
        "services":"varchar(200)",
        "road":"varchar(50)",
        "twoice":"boolean",
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

    citylist = ['台北市','基隆市','新北市','桃園市','新竹市','新竹縣','苗栗縣','台中市','彰化縣','南投縣','雲林縣','嘉義市','嘉義縣','台南市','高雄市','屏東縣','宜蘭縣','花蓮縣','台東縣','澎湖縣','連江縣','金門縣']
    for city in citylist:
        # get town in city
        print("city:",city)
        url = "https://api.map.com.tw/net/familyShop.aspx?searchType=ShowTownList&type=&city="+city+"&fun=storeTownList&key=6F30E8BF706D653965BDE302661D1241F8BE9EBC"
        headers = {'Referer': 'https://www.family.com.tw/'}
        response = requests.request("GET", url, headers=headers)
        obj = json.loads(response.text[14:-1])
        for townobj in obj:
            # get shop in town
            post = townobj["post"]
            town = townobj["town"]
            print("town:",town)

            
            # retry 3 times
            def getshops(city,town):
                url = "https://api.map.com.tw/net/familyShop.aspx?searchType=ShopList&type=&city="+city+"&area="+town+"&road=&fun=showStoreList&key=6F30E8BF706D653965BDE302661D1241F8BE9EBC"
                headers = {'Referer': 'https://www.family.com.tw/'}
                response = requests.request("GET", url, headers=headers)
                shopobj = json.loads(response.text[14:-1])
                return shopobj

            retry_times = 3
            success = False
            for retry in range(retry_times):
                try:
                    shopobj = getshops(city,town)
                    success = True
                    break
                except Exception as e:
                    print("retry after 2 seconds...")
                    time.sleep( 2 )

            ## try last time (expose fail message)
            if not success:
                shopobj = getshops(city,town)

            for shop in shopobj:
                name = shop["NAME"]
                tel = shop["TEL"]
                postel = shop["POSTel"]
                location = "point({},{})".format(shop["px"],shop["py"])
                addr = shop["addr"]
                serid = shop["SERID"]
                pkey = shop["pkey"]
                oldpkey = shop["oldpkey"]
                shop_post = shop["post"]
                services = shop["all"]
                road = shop["road"]
                twoice = True if shop["twoice"] == "Y" else False

                # insert data
                insert_sql = "insert into {} (city,town,name,tel,postel,location,addr,serid,pkey,oldpkey,post,services,road,twoice) values ('{}','{}','{}','{}','{}',{},'{}',{},'{}','{}','{}','{}','{}',{})".format(table_name,city,town,name,tel,postel,location,addr,serid,pkey,oldpkey,shop_post,services,road,twoice)
                cursor.execute(insert_sql)

    connection.commit()    
    cursor.close()
    connection.close()

    




with DAG(
    dag_id = "shopfamily",
    description='便利商店全家座標',
    start_date = datetime(2024, 1, 1),
    schedule_interval="@weekly",
    on_failure_callback=notifier.FailNotifier(message="RUN: {{ logical_date }}"),
    catchup = False
) as dag:
    recreate_table = PythonOperator(
        task_id="recreate_table",
        op_kwargs={"table_name":"shopfamily"},
        python_callable=recreateTable
    )
    etl = PythonOperator(
        task_id="etl",
        op_kwargs={"table_name":"shopfamily"},
        python_callable=etl
    )

    recreate_table  >> etl




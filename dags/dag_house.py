from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowFailException
from datetime import datetime
import requests, zipfile, io, csv, os, sys, shutil
import sql,notifier

###########schema###########
schema = {
    "house_buy":{
        "col1":"varchar(200)", "col2":"varchar(200)", "col3":"varchar(200)", "col4":"varchar(2000)", "col5":"varchar(200)", "col6":"varchar(200)", "col7":"varchar(200)", "col8":"varchar(200)", "col9":"varchar(200)", "col10":"varchar(200)", "col11":"varchar(200)", "col12":"varchar(200)", "col13":"varchar(200)", "col14":"varchar(200)", "col15":"varchar(200)", "col16":"varchar(200)", "col17":"varchar(200)", "col18":"varchar(200)", "col19":"varchar(200)", "col20":"varchar(200)", "col21":"varchar(200)", "col22":"varchar(200)", "col23":"varchar(200)", "col24":"varchar(200)", "col25":"varchar(200)", "col26":"varchar(200)", "col27":"varchar(200)", "col28":"varchar(2000)", "col29":"varchar(200)", "col30":"varchar(200)", "col31":"varchar(200)", "col32":"varchar(200)", "col33":"varchar(200)", "col34":"varchar(200)"
    },    
    "house_pre_buy":{
        "col1":"varchar(200)","col2":"varchar(200)","col3":"varchar(200)","col4":"varchar(200)","col5":"varchar(200)","col6":"varchar(200)","col7":"varchar(200)","col8":"varchar(200)","col9":"varchar(200)","col10":"varchar(200)","col11":"varchar(200)","col12":"varchar(200)","col13":"varchar(200)","col14":"varchar(200)","col15":"varchar(200)","col16":"varchar(200)","col17":"varchar(200)","col18":"varchar(200)","col19":"varchar(200)","col20":"varchar(200)","col21":"varchar(200)","col22":"varchar(200)","col23":"varchar(200)","col24":"varchar(200)","col25":"varchar(200)","col26":"varchar(200)","col27":"varchar(200)","col28":"varchar(200)","col29":"varchar(200)","col30":"varchar(200)","col31":"varchar(200)","col32":"varchar(200)"
    },
    "house_rent":{
        "col1":"varchar(200)","col2":"varchar(200)","col3":"varchar(200)","col4":"varchar(2000)","col5":"varchar(200)","col6":"varchar(200)","col7":"varchar(200)","col8":"varchar(200)","col9":"varchar(200)","col10":"varchar(200)","col11":"varchar(200)","col12":"varchar(200)","col13":"varchar(200)","col14":"varchar(200)","col15":"varchar(200)","col16":"varchar(200)","col17":"varchar(200)","col18":"varchar(200)","col19":"varchar(200)","col20":"varchar(200)","col21":"varchar(200)","col22":"varchar(200)","col23":"varchar(200)","col24":"varchar(200)","col25":"varchar(200)","col26":"varchar(200)","col27":"varchar(200)","col28":"varchar(200)","col29":"varchar(2000)","col30":"varchar(200)"
    }
}
############################


csv.field_size_limit(sys.maxsize)

def filladdress(city,town,address):
    if city in address and town in address:
        return address
    else:
        return city+town+address

def ymtodt(ym):
    if len(ym) < 7:
        return "2000-00-00"
    y = int(ym[:3]) + 1911
    return "{}-{}-{}".format(y,ym[3:5],ym[5:7])



def extract(path,**kwargs):
    print("logical_date:",kwargs['logical_date'])
    y = kwargs['logical_date'].year - 1911
    q = kwargs['logical_date'].quarter
    period = "%sS%s"%(y,q)
    print("period:"+period)
    
    # request zip file
    url = "https://plvr.land.moi.gov.tw//DownloadSeason?season=" + period + "&type=zip&fileName=lvr_landcsv.zip"
    print("url:",url)
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    # unzip and save
    output = "{}/{}".format(path,period)
    z.extractall(output)
    print("save csv:" + output)
    
    # push xcom
    ti = kwargs["ti"]
    ti.xcom_push(key="period", value=period)
    ti.xcom_push(key="path",value=output)


def transform(category,table_name,**kwargs):
    # pull xcom
    ti = kwargs["ti"]
    period = ti.xcom_pull(key="period",task_ids="extract")
    path = ti.xcom_pull(key="path",task_ids="extract")
    print("period:",period)
    print("category:",category)
    print("path:",path)

    # process data
    city = {"c":"基隆市","a":"臺北市","f":"新北市","h":"桃園市","o":"新竹市","j":"新竹縣","k":"苗栗縣","b":"臺中市","m":"南投縣","n":"彰化縣","p":"雲林縣","i":"嘉義市","q":"嘉義縣","d":"臺南市","e":"高雄市","t":"屏東縣","g":"宜蘭縣","u":"花蓮縣","v":"臺東縣","x":"澎湖縣","w":"金門縣","z":"連江縣"}
    outpath = path + "/output"
    
    # check output path exist
    if not os.path.isdir(outpath):
        print(outpath," not exist, create it.")
        try:
            os.mkdir(outpath)
        except Exception as e:
            print(e)
    
    outcsvfiles = []
    for k in city:
        print("city:"+city[k])
        # check csv file exist
        incsvname = "{}/{}_lvr_land_{}.csv".format(path,k,category)
        if not os.path.isfile(incsvname):
            continue
        
        # open output csv
        outcsvname = "house_{}_{}_{}.csv".format(period,category,k)
        with open(outpath+"/"+outcsvname, 'w', newline='',errors='ignore') as outcsv:
            print("save csv name:",outcsvname)
            outcsvfiles.append(outcsvname)
            writer = csv.writer(outcsv, delimiter='\t')
            
            # open resource csv
            with open(incsvname, newline='', encoding='utf8') as incsv:
                print("open csv:",incsvname)
                rows = csv.reader(incsv)
                ignore = 2
                for row in rows:
                    if ignore > 0:
                        ignore -= 1
                        continue
                    address = filladdress(city[k],row[0],row[2])
                    new_row = [city[k]] + row[0:2] + [address] + row[3:7] + [ymtodt(row[7])] + row[8:14]+[ymtodt(row[14])] + row[15:]
                    new_row = [cell.replace("\t", " ") if isinstance(cell, str) else cell for cell in new_row]
                    if len(new_row) != len(schema[table_name]):
                        print("invalid content:",new_row)
                    else:
                        writer.writerow(new_row)

    # push xcom
    ti.xcom_push(key="period", value=period)
    ti.xcom_push(key="path",value=path)
    ti.xcom_push(key="table_name",value=table_name)
    ti.xcom_push(key="files",value={"name":outcsvfiles})

def load(category,**kwargs):
    # pull xcom
    ti = kwargs["ti"]
    period = ti.xcom_pull(key="period",task_ids=category+".transform")
    path = ti.xcom_pull(key="path",task_ids=category+".transform")
    table_name = ti.xcom_pull(key="table_name",task_ids=category+".transform")
    files = ti.xcom_pull(key="files",task_ids=category+".transform")["name"]

    outpath = path + "/output"


    # connect to postgresdb
    print("Connect to postgresdb")
    pg_hook = PostgresHook(postgres_conn_id='postgresdb')
    
    # create table
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    create_sql = sql.createstate(table_name,schema[table_name])
    print("create table if not exists:",table_name)
    cursor.execute(create_sql)
    connection.commit()
    cursor.close()
    connection.close()

    # insert data
    fail_files = []
    for file in files:
        print("bulk insert:",file)
        filepath = "{}/{}".format(outpath,file)
        try:
            pg_hook.bulk_load(table_name,filepath)
        except Exception as e:
            fail_files.append({"name":file,"error":e})
    
    # push xcom
    ti.xcom_push(key="path",value=path)

    if len(fail_files) > 0:
        for errorfile in fail_files:
            print("====="+errorfile["name"]+"=====")
            print(errorfile["error"])
        raise AirflowFailException("load failed.")


def clear(**kwargs):
    # pull xcom
    ti = kwargs["ti"]
    path = ti.xcom_pull(key="path",task_ids = 'extract')
    
    # remove folder
    shutil.rmtree(path)



with DAG(
    dag_id = "house",
    description='實價登錄數據',
    start_date = datetime(2012, 1, 1),
    schedule_interval="0 0 2 1,4,7,10 *",
    catchup=True,
    on_failure_callback=notifier.FailNotifier(message="RUN: {{ logical_date }}")
) as dag:
    extract = PythonOperator(
        task_id="extract",
        op_kwargs={"path": "/opt/airflow/data/house"},
        python_callable=extract,
        provide_context=True
    )
    
    # a:不動產買賣
    with TaskGroup(group_id='a') as tga: 
        transform_task = PythonOperator(
            task_id="transform",
            op_kwargs={"category": "a","table_name":"house_buy"},
            python_callable=transform
        )
        load_task = PythonOperator(
            task_id="load",
            op_kwargs={"category": "a"},
            python_callable=load
        )
        transform_task >> load_task
    
    # b:預售屋買賣
    with TaskGroup(group_id='b') as tgb:
        transform_task = PythonOperator(
            task_id="transform",
            op_kwargs={"category": "b","table_name":"house_pre_buy"},
            python_callable=transform
        )
        load_task = PythonOperator(
            task_id="load",
            op_kwargs={"category": "b"},
            python_callable=load
        )
        transform_task >> load_task
    
    # c:不動產租賃
    with TaskGroup(group_id='c') as tgc:
        transform_task = PythonOperator(
            task_id="transform",
            op_kwargs={"category": "c","table_name":"house_rent"},
            python_callable=transform
        )
        load_task = PythonOperator(
            task_id="load",
            op_kwargs={"category": "c"},
            python_callable=load
        )
        transform_task >> load_task

    
    clear = PythonOperator(
        task_id="clear",
        python_callable=clear,
        trigger_rule=TriggerRule.ALL_SUCCESS   
    )

    extract >> [tga,tgb,tgc] >> clear
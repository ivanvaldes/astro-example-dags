from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago
import os
from pymongo import MongoClient
from pandas import DataFrame
from google.cloud import bigquery
import pandas as pd
import numpy as np

default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_connect_mongo():

    CONNECTION_STRING ="mongodb+srv://atlas:T6.HYX68T8Wr6nT@cluster0.enioytp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)

    return client

def transform_date(text):
    text = str(text)
    d = text[0:10]
    return d

def get_group_status(text):
    text = str(text)
    if text =='CLOSED':
        d='END'
    elif text =='COMPLETE':
        d='END'
    else :
        d='TRANSIT'
    return d
    
def load_orders_process():
    print("Load orders!")
    client = bigquery.Client(project='my-first-project-411501')
    query_string = """
    drop table if exists `my-first-project-411501.dep_raw.orders` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)
    print("Borrar orders!")

    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["orders"] 
    orders = collection_name.find({})  
    orders_df = DataFrame(orders)
    dbconnect.close()

    orders_df['_id'] = orders_df['_id'].astype(str)
    orders_df['order_date']  = orders_df['order_date'].map(transform_date)
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], format='%Y-%m-%d').dt.date

    orders_df.dtypes

    orders_rows=len(orders_df)
    if orders_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.orders"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            orders_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla orders')

def load_order_items_process():
    print("Load order_items!")
    client = bigquery.Client(project='my-first-project-411501')
    query_string = """
    drop table if exists `my-first-project-411501.dep_raw.order_items` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)
    print("Borrar order_items!")

    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["order_items"] 
    order_items = collection_name.find({})  
    order_items_df = DataFrame(order_items)
    dbconnect.close()

    order_items_df['_id'] = order_items_df['_id'].astype(str)

    order_items_df['order_date']  = order_items_df['order_date'].map(transform_date)
    order_items_df['order_date'] = pd.to_datetime(order_items_df['order_date'], format='%Y-%m-%d').dt.date

    order_items_df.dtypes

    order_items_rows=len(order_items_df)
    if order_items_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.order_items"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            order_items_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')

def load_products_process():
    print(f"Load PRODUCTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["products"] 
    products = collection_name.find({})
    products_df = DataFrame(products)
    dbconnect.close()
    products_df['_id'] = products_df['_id'].astype(str)
    products_df['product_description'] = products_df['product_description'].astype(str)
    products_rows=len(products_df)
    print(f" Se obtuvo  {products_rows}  Filas")
    products_rows=len(products_df)
    if products_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')
        table_id =  "my-first-project-411501.dep_raw.products"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_description", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("product_image", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            products_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla productos')

def load_customers_process():
    print(f"Load CUSTOMERS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["customers"] 

    customers = collection_name.find({})
    customers_df = DataFrame(customers)
    dbconnect.close()

    customers_df['_id'] = customers_df['_id'].astype(str)

    customers_rows=len(customers_df)
    if customers_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.customers"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("customer_fname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_lname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_email", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_password", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_street", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_state", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_zipcode", bigquery.enums.SqlTypeNames.INTEGER),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            customers_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla customers')

def load_categories_process():
    print(f"Load CATEGORIES")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["categories"] 
    categories = collection_name.find({})
    categories_df = DataFrame(categories)
    dbconnect.close()
    categories_df = categories_df.drop(columns=['_id'])

    categories_rows=len(categories_df)
    if categories_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.categories"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            categories_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla categories')

def load_departaments_process():
    print(f"Load DEPARTAMENTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["departments"] 
    departments = collection_name.find({})
    departments_df = DataFrame(departments)
    dbconnect.close()
    departments_df = departments_df.drop(columns=['_id'])

    departments_rows=len(departments_df)
    if departments_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.departments"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("department_name", bigquery.enums.SqlTypeNames.STRING)
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            departments_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla departments')

def capa_master_process():
    client = bigquery.Client(project='my-first-project-411501')
    query_string = """
    drop table if exists `my-first-project-411501.dep_raw.master_order` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)
    print("Borrar master_order!")
    
    
    sql = """
        SELECT *
        FROM `my-first-project-411501.dep_raw.order_items`
    """
    m_order_items_df = client.query(sql).to_dataframe()
    sql_2 = """
        SELECT *
        FROM `my-first-project-411501.dep_raw.orders`
    """
    m_orders_df = client.query(sql_2).to_dataframe()
    df_join = m_orders_df.merge(m_order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
    df_join
    df_master=df_join[[ 'order_id', 'order_date_x', 'order_customer_id',
       'order_status',  'order_item_id',
       'order_item_order_id', 'order_item_product_id', 'order_item_quantity',
       'order_item_subtotal', 'order_item_product_price']]
    df_master=df_master.rename(columns={"order_date_x":"order_date"})
    df_master['order_status_group']  = df_master['order_status'].map(get_group_status)
    df_master['order_date'] = df_master['order_date'].astype(str)
    df_master['order_date'] = pd.to_datetime(df_master['order_date'], format='%Y-%m-%d').dt.date
    df_master
    df_master.dtypes
    df_master_rows=len(df_master)
    if df_master_rows>0 :
        client = bigquery.Client(project='my-first-project-411501')

        table_id =  "my-first-project-411501.dep_raw.master_order"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_status_group", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')

    print('orden_item_subtotal_mn')

    headers_files = {
        'tipocambio':["fecha","compra","venta","error"]
        }
    dwn_url_tipcambio='https://www.sunat.gob.pe/a/txt/tipoCambio.txt'
    tipcambio_df = pd.read_csv(dwn_url_tipcambio, names=headers_files['tipocambio'], sep='|')
    tipcambio_df.head()
    list_t = tipcambio_df.values.tolist()
    vari=list_t[0][1]
    vari
    df_master['order_item_subtotal_mn'] = df_master['order_item_subtotal'] * vari

    table = client.get_table(table_id)
    # Verificar si la columna ya existe en el esquema
    if table and "order_item_subtotal_mn" not in [field.name for field in table.schema]:
        print('Existe SI')
        # Obtener el esquema actual de la tabla
        current_schema = table.schema

        # Agregar la nueva columna al esquema
        new_schema = current_schema + [
            bigquery.SchemaField("order_item_subtotal_mn", bigquery.enums.SqlTypeNames.FLOAT)
        ]

        # Crear una nueva tabla con el nuevo esquema
        new_table = bigquery.Table(table_id, schema=new_schema)

        # Crear la nueva tabla en BigQuery
        new_table = client.update_table(new_table, ["schema"])

        # Continuar con el código original para cargar la tabla
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )  
        job.result()  # Esperar a que se complete el trabajo.

        table = client.get_table(table_id)  # Hacer una solicitud de API.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        print("Nueva columna 'order_item_subtotal_mn' agregada al esquema.")
    else:
        # Continuar con el código original para cargar la tabla
        print('Existe NO')
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )  
        job.result()  # Esperar a que se complete el trabajo.

        table = client.get_table(table_id)  # Hacer una solicitud de API.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

def load_bi_process():
    client = bigquery.Client(project='my-first-project-411501')
    query_string = """
    create or replace table `my-first-project-411501.dep_raw.bi_orders` as
    SELECT 
    order_date,c.category_name ,d.department_name 
    , sum (a.order_item_subtotal) order_item_subtotal
    , sum (a.order_item_quantity) order_item_quantity
    FROM `my-first-project-411501.dep_raw.master_order` a
    inner join  `my-first-project-411501.dep_raw.products` b on
    a.order_item_product_id=b.product_id
    inner join `my-first-project-411501.dep_raw.categories` c on
    b.product_category_id=c.category_id
    inner join `my-first-project-411501.dep_raw.departments` d on
    c.category_department_id=d.department_id
    group by order_date,c.category_name ,d.department_name
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)

def load_segment_process():
    client = bigquery.Client(project='my-first-project-411501')
    query_string = """
    create or replace table `my-first-project-411501.dep_raw.client_segment` as
    select customer_id,category_name , order_item_subtotal from
    (
    SELECT customer_id,category_name,order_item_subtotal,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_item_subtotal DESC) AS rank_ 
    FROM (
        SELECT 
        d.customer_id ,c.category_name 
        , sum (a.order_item_subtotal) order_item_subtotal
        FROM `my-first-project-411501.dep_raw.master_order` a
        inner join  `my-first-project-411501.dep_raw.products` b on
        a.order_item_product_id=b.product_id
        inner join `my-first-project-411501.dep_raw.categories` c on
        b.product_category_id=c.category_id
        inner join `my-first-project-411501.dep_raw.customers` d on
        a.order_customer_id=d.customer_id
        group by 1,2
    )
    )
    where rank_=1
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)

def load_personal_mongodb_process():
    client_bq = bigquery.Client(project='my-first-project-411501')

    # Configuración de conexión a MongoDB
    PERSONAL_MONGODB_CONNECTION_STRING ="mongodb+srv://ivmigliore:ivmigliore@cluster0.ko3sfg6.mongodb.net/"
    client_mongo = MongoClient(PERSONAL_MONGODB_CONNECTION_STRING) # Ajusta la URI de conexión según tu configuración
    db = client_mongo['dbTest']  # Reemplaza 'tu_base_de_datos' con el nombre de tu base de datos en MongoDB
    collection = db['myFirstCollection']  # Nombre de la colección en MongoDB

    # Ejecutar la consulta en BigQuery
    query_string = """
    select customer_id,category_name , order_item_subtotal from
    (
    SELECT customer_id,category_name,order_item_subtotal,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_item_subtotal DESC) AS rank_ 
    FROM (
        SELECT 
        d.customer_id ,c.category_name 
        , sum (a.order_item_subtotal) order_item_subtotal
        FROM `my-first-project-411501.dep_raw.master_order` a
        inner join  `my-first-project-411501.dep_raw.products` b on
        a.order_item_product_id=b.product_id
        inner join `my-first-project-411501.dep_raw.categories` c on
        b.product_category_id=c.category_id
        inner join `my-first-project-411501.dep_raw.customers` d on
        a.order_customer_id=d.customer_id
        group by 1,2
    )
    )
    where rank_=1
    """
    query_job = client_bq.query(query_string)
    rows = list(query_job.result())

    # Insertar resultados en MongoDB
    for row in rows:
        document = {
            'customer_id': row['customer_id'],
            'category_name': row['category_name'],
            'order_item_subtotal': row['order_item_subtotal']
        }
        collection.insert_one(document)

    print("Datos insertados en MongoDB correctamente.")

with DAG(
    dag_id="final_project",
    schedule='0 4 * * 1',  # Ejecutar los lunes a las 04:00
    start_date=days_ago(2), 
    default_args=default_args
) as dag:
    step_load_orders = PythonOperator(
        task_id='Load_orders',
        python_callable=load_orders_process,
        dag=dag
    )
    step_load_order_items = PythonOperator(
        task_id='Load_order_items',
        python_callable=load_order_items_process,
        dag=dag
    )
    step_load_products = PythonOperator(
        task_id='Load_products',
        python_callable=load_products_process,
        dag=dag
    )
    step_load_customers = PythonOperator(
        task_id='Load_customers',
        python_callable=load_customers_process,
        dag=dag
    )
    step_load_categories = PythonOperator(
        task_id='Load_categories',
        python_callable=load_categories_process,
        dag=dag
    )
    step_load_departaments = PythonOperator(
        task_id='Load_departaments',
        python_callable=load_departaments_process,
        dag=dag
    )
    step_capa_master = PythonOperator(
        task_id='capa_master',
        python_callable=capa_master_process,
        dag=dag
    )
    step_load_bi = PythonOperator(
        task_id='Load_bi',
        python_callable=load_bi_process,
        dag=dag
    )
    step_load_segment = PythonOperator(
        task_id='Load_segment',
        python_callable=load_segment_process,
        dag=dag
    )
    step_load_personal_mongodb = PythonOperator(
        task_id='Load_personal_mongodb',
        python_callable=load_personal_mongodb_process,
        dag=dag
    )

    step_load_orders>>step_load_order_items>>step_load_products>>step_load_customers>>step_load_categories>>step_load_departaments>>step_capa_master>>step_load_bi>>step_load_segment>>step_load_personal_mongodb

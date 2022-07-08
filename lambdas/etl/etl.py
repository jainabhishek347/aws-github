import sys
import logging
import pandas as pd

import psycopg2
import numpy as np
import psycopg2.extras as extras

# import extract, load_db, load_basket, table_script
# from db_utils import get_db_connection

import hashlib


LOGGER = logging.getLogger()

FIELDNAMES =['timestamp','store','customer_name',
             'basket_items','total_price','cash_or_card','card_number']

FILENAME = 'chesterfield.csv'

DATABASE ="dev"

USER ='awsuser'

PASSWORD ='Qaz_8964'

HOST ='redshift-cluster-2.cko8iozzt0ly.us-east-1.redshift.amazonaws.com'

PORT ='5439'


def get_db_connection():
# def get_db_connection_old():
    conn, cur = None, None
    try:
        print('connecting to database')
        conn = psycopg2.connect(
            database=DATABASE,
            user=USER,
            password=PASSWORD, 
            host=HOST,
            port=5439
        )
        cur = conn.cursor()
        return conn, cur

    except Exception as error:
        print(error)


logger = logging.getLogger()

CONN, CUR = get_db_connection()

## hashing values function
def hash_value(x):
    """
    - Hashes x with hashlib.sha256
    """
    if x != 'nan':
        return hashlib.sha256(x.encode()).hexdigest()
    else:
        return None
    
# creating Clean customers_table **hashed**
def unique_customers_table(data):
    unhashed_cus_df = data[["customer_name", "card_number"]].drop_duplicates()
    hashed_cus_df = unhashed_cus_df.applymap(lambda x: hash_value(str(x)))
    return hashed_cus_df

### Transform basket  ###

def fetch_products(data):
    """
    - Returns a df with all products and details in the raw data
    - Must be transformed
    """
    #Split the basket_items col so that each row is a list
    items_series = data['basket_items'].apply(lambda x: x.split(", "))
    
    #Load this pd.Series object into a pd.DataFrame. Unwanted column - dropped after transformation
    products_df = pd.DataFrame(items_series, columns=['basket_items'])

    #Explode contents of each order so that every item in an order is a separate row in the df
    products_df = products_df.explode('basket_items')
    
    return products_df

## creating products function
def create_products_df(DATA):
    """
    - Returns a df which transforms the unique products and details
    """
    products_df = fetch_products(DATA)

    #Get unique products
    products_df = products_df.drop_duplicates(ignore_index=True)
    
    product_names, product_flavours, product_prices = [], [], []

    for product in products_df['basket_items']:
        details = product.split(' - ')
        #Append name and price (always first and last elements of details)
        product_name = f'{details[0]}'
        product_names.append(product_name)

        product_price = f'{details[-1]}'
        product_prices.append(product_price)

        #Handle flavours
        if 'Flavoured' in product:
            #Append flavour
            product_flavour = f'{details[1]}'
            product_flavours.append(product_flavour)
        else:
            #Append 'Original'
            product_no_flavour = f'Original'
            product_flavours.append(product_no_flavour)
        
    #Populate products_df with new columns
    products_df['product_name'] = product_names
    products_df['product_flavour'] = product_flavours
    products_df['product_price'] = product_prices

    #Drop unwanted column
    products_df = products_df.drop('basket_items', axis=1)
    return products_df

def create_orders_df(data, conn = CONN, cursor=CUR):
    """
    - Returns a df containing orders and the accompanying information
    - branch_id and cust_id columns rely on data which has to be loaded into 
    the db first (for the queries)
    """
    orders_df_without_ids = data[['timestamp', 'store', 'customer_name', 'cash_or_card', 'card_number']]
    
    #Check for duplicates
    orders_df_without_ids = orders_df_without_ids.drop_duplicates()
    
    
    #Query branch_ids and cust_ids from their tables and populate into orders_df
    branch_vals = [val for val in orders_df_without_ids['store']]
    branch_ids = []
    for branch_val in branch_vals:
        sql = \
            f'''
            SELECT store_id
            FROM store_df
            WHERE store = '{branch_val}'
            '''
        cursor.execute(sql)
        record = cursor.fetchone()
        #Returns a tuple with id at idx = 0
        branch_ids.append(record[0])
    
    cust_vals = [val for val in orders_df_without_ids['customer_name']]
    cust_ids = []
    for cust_val in cust_vals:
        sql = \
            f'''
            SELECT customer_id
            FROM customer_df
            WHERE customer_name = '{hash_value(cust_val)}'
            '''
        cursor.execute(sql)
        record = cursor.fetchone()
        cust_ids.append(record[0])
    
    conn.close()
    
    #Make new df with the new columns
    orders_df = pd.DataFrame(orders_df_without_ids, columns=['time_stamp', 'branch_id', 'cust_id', 'payment_type', 'total_price'])
    
    #Populate id columns with queried values
    orders_df['branch_id'] = branch_ids
    orders_df['cust_id'] = cust_ids

    return orders_df

def create_basket_df(conn, cursor, data):
    """
    - Returns a df containing individual products from each order
    - cols: order_id, product_id
    """
    products_df = fetch_products(data)

    #Create order_id for every product in each order
    products_df['order_id'] = products_df.index

    #Names and flavours of all individual products from every order
    product_names = []

    #TODO: refactor this? Repeating code from create_products_df()
    for product in products_df['basket_items']:
        details = product.split(' - ')
        if 'Flavoured' in product:
            product_and_flavour = f'{details[0]} {details[1]}'
            product_names.append(product_and_flavour)
        else:
            product_no_flavour = f'{details[0]} Original'
            product_names.append(product_no_flavour)

    #Query products table to get all the product_names and product_ids
    sql = \
        '''
        SELECT product_id, product_name, product_flavour
        FROM products_df
        '''
    cursor.execute(sql)

    #List of tuples where each tuple is a row in products table
    products = cursor.fetchall()

    #Dict - keys: product_names, values: product_ids (from products table)
    products_dict = {}

    for product in products:
        product_name = f'{product[1]} {product[2]}'
        product_id = str(product[0])
        products_dict[product_name] = product_id

    #Get product_ids from products_dict
    product_ids = [products_dict.get(product_name) for product_name in product_names]

    #Create dict to be loaded into df which is then loaded to db
    basket_dict = {
        'product_id': product_ids,
        'order_id': products_df['order_id']
    }

    basket_df = pd.DataFrame(basket_dict)
    
    return basket_df



def basket_df(data):
    basket_df = create_basket_df()
    order_df = create_orders_df()
    customer_id = order_df['cust_id']
    store_id = order_df['branch_id']
    time_stamp = data['timestamp']

    basket_df['customer_id'] = customer_id
    basket_df['store_id'] = store_id
    basket_df['time_stamp'] = time_stamp
    return basket_df

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
  

import logging
import psycopg2
import numpy as np
import psycopg2.extras as extras

logger = logging.getLogger()
  
def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    logger.info(f'{df} DataFrame Inserted Successfully to Database!')
    print("the dataframe is inserted")


def db_create_tables(conn, cur):    
    ## create customer table ## as ** customer_df
    create_customer_table = '''CREATE TABLE IF NOT EXISTS customer_df(
                            customer_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            customer_name   TEXT,
                            card_number     text                          
                            );'''
    
    ## create store table ## as ** store_df
    create_store_table = '''CREATE TABLE IF NOT EXISTS  store_df(
                            store_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            store        TEXT
                            );'''
    
    ## create basket table ## as ** basket_df                     
    create_basket_table = '''CREATE TABLE IF NOT EXISTS  basket_df(
                            order_id         integer,
                            product_id       integer,
                            customer_id      integer,
                            store_id         integer,
                            time_stamp       text,
                            constraint fk_product
                                foreign key (product_id) 
                                REFERENCES products_df (product_id),
                            constraint fk_customer
                                foreign key (customer_id) 
                                REFERENCES customer_df (customer_id),
                            constraint fk_store
                                foreign key (store_id) 
                                REFERENCES store_df (store_id)
                            );'''
    
    ## create products table ## as ** basket_df                     
    create_products_table = '''CREATE TABLE IF NOT EXISTS products_df(
                            product_id      INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            product_name    TEXT,
                            product_flavour    TEXT,    
                            product_price    TEXT
                            );'''
    
    # executing tables 
    cur.execute(f' {create_customer_table}{create_store_table}{create_products_table}{create_basket_table}')
    print('tables have been created!!')
    conn.commit()


def csv_to_df(DATA):
    
    # DATA = pd.read_csv(FILENAME, names = FIELDNAMES)

    LOGGER.info('Creating Products DataFrame!')
    products_df = extract.create_products_df(DATA)

    LOGGER.info('Creating Customer DataFrame!')
    customer_df = extract.unique_customers_table(DATA)

    LOGGER.info('Creating Store DataFrame!')
    store_df = pd.DataFrame(DATA['store'].unique(), columns=['store'])

    return products_df, customer_df, store_df

def lambda_handler(event, context):

    print('=============================')
    print(event)

    conn, cursor = get_db_connection()
    db_create_tables(conn, cursor)

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]

    print(bucket_name, s3_file_name)
    
    df = pd.read_csv(f's3://{bucket_name}/{s3_file_name}', sep=',')
    print (df.head())

    DATA = pd.read_csv(f's3://{bucket_name}/{s3_file_name}', names = FIELDNAMES)
    print(DATA)

    conn, cursor = get_db_connection()

    LOGGER.info('Creating tables in database if tables not exists!')
    tables = table_script.db_create_tables(conn, cursor)

    LOGGER.info('Creating and Fetching Data from DataFrames!')
    
    products_df, customer_df, store_df = csv_to_df(DATA)

    basket_df = load_basket.create_basket_df(conn, cursor, DATA)

    LOGGER.info('Inserting Data into Database..')
    load_basket.execute_values(conn, basket_df, 'basket_df')
    load_db.execute_values(conn, customer_df, 'customer_df')
    load_db.execute_values(conn, products_df, 'products_df')
    load_db.execute_values(conn, store_df, 'store_df')

    context = {
        'status' : 200,
        'message' : 'Success',
    }
    return context


lambda_handler('', '   ')


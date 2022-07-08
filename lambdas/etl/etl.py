import sys
import logging
import pandas as pd

import psycopg2
import numpy as np
import psycopg2.extras as extras

# import extract, load_db, load_basket, table_script
from db_utils import get_db_connection

LOGGER = logging.getLogger()

FIELDNAMES =['timestamp','store','customer_name',
             'basket_items','total_price','cash_or_card','card_number']

FILENAME = 'chesterfield.csv'


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
    db_create_tables(conn, cur)

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


# handler('' , '')


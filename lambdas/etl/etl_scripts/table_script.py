import logging

import psycopg2

logger = logging.getLogger()


def db_create_tables(conn, cur):    
    ## create customer table ## as ** customer_df
    create_customer_table = '''CREATE TABLE IF NOT EXISTS customer_df(
                            customer_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            customer_name	TEXT,
                            card_number	    text                          
                            );'''
    
    ## create store table ## as ** store_df
    create_store_table = '''CREATE TABLE IF NOT EXISTS  store_df(
                            store_id     INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            store     	 TEXT
                            );'''
    
    ## create basket table ## as ** basket_df                     
    create_basket_table = '''CREATE TABLE IF NOT EXISTS  basket_df(
                            order_id         integer,
                            product_id     	 integer,
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
                            product_name	TEXT,
                            product_flavour    TEXT,  	
                            product_price	 TEXT
                            );'''
    
    # executing tables 
    cur.execute(f' {create_customer_table}{create_store_table}{create_products_table}{create_basket_table}')
    print('tables have been created!!')
    conn.commit()




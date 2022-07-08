import logging
import hashlib
import psycopg2
import pandas as pd
from utils.db_utils import get_db_connection

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






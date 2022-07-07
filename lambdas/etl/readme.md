# ETL Pipeline
The ETL Pipeline was created using AWS Lambda and deployed on Docker with scripts written in Python.
The ETL Pipeline takes raw data in form of CSV, transforms it, and systematically stores it into the PostgresSQL database.
Pandas library is used for transforming the data so that it can be appropriately stored in the database.
For this particular ETL Pipeline, the database contains four tables:
  1) Customer Table (customer_df): Stores information about the Customer including attributes like customer ID, Customer Name, etc.
  2) Product Table (product_df): Stores information about the Product including attributes like Product ID, Product Name, etc.
  3) Store Table (store_df): Stores information about the Store including attributes like Store Name, Product Name, etc.
  4) Basket Table (basket_df): Stores information about the basket created for an order having attributes like Product ID, Store ID, etc.
  
To enable the creation of multiple pipelines in the future, a GitHub action workflow is created.

## How to integrate GitHub action in your project

 ### 1. Pipeline Configuration 
To integrate GitHub actions in another project you have to copy the whole. Github folder to your repo, 
you can add test cases and edit the approver user for the production environment in the pipeline,

 ### 2. Deployment
Through AWS CLI, AWS is configured using credentials (AWS ACCESS KEY ID and Secret Key).
Subsequently, we log into Docker.
Finally, a repository is created to be deployed on docker.


## How ETL pipeline works
1. The raw CSV file is loaded into AWS S3.
2. The loaded data is taken from S3 and preprocessed using Pandas library in Python.
3. Based on triggers, AWS Lambda functions are invoked which store the data in the PostgresSQL database.
        

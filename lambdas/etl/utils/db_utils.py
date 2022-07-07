import psycopg2
import psycopg2.extras as extras

# import redshift_connector

from configs import settings


def get_db_connection():
# def get_db_connection_old():
	conn, cur = None, None
	try:
		print('connecting to database')
		conn = psycopg2.connect(
			database=settings.DATABASE,
			user=settings.USER,
			password=settings.PASSWORD, 
			host=settings.HOST,
			port=5439
		)
		cur = conn.cursor()
		return conn, cur

	except Exception as error:
		print(error)

	
# def get_db_connection_old():	
# # def get_db_connection():
# 	conn, cur = None, None
# 	try:
# 		print('connecting to redshift database')
		
# 		conn = redshift_connector.connect(
# 		    host='redshift-cluster-1.cko8iozzt0ly.us-east-1.redshift.amazonaws.com',
# 		    database='dev',
# 		    user='awsuser',
# 		    password='Qaz_8964'
# 		 )


# 		cursor: redshift_connector.Cursor = conn.cursor()

# 		return conn, cursor

# 	except Exception as error:
# 		print(error)


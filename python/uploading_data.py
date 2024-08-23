import database_connection as db_conn
import psycopg2
import psycopg2.extras as extras

cur1, conn1 = db_conn.get_conn_postgresql()

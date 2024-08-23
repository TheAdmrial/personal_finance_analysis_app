import psycopg2

# target database PostgreSQL
def get_conn_postgresql():
    conn = psycopg2.connect(host = "localhost"
                            , database = 'fin_app'
                            , user = 'postgres'
                            , password = '')
    cur = conn.cursor()
    return cur, conn


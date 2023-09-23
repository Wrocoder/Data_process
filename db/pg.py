import psycopg2

conn = psycopg2.connect(database="my_project",
                        host="127.0.0.1",
                        user="admin",
                        password="admin",
                        port="5432")

cursor = conn.cursor()
cursor.execute("CREATE TABLE accounts ( \
                user_id serial PRIMARY KEY, \
                username VARCHAR ( 50 ) UNIQUE NOT NULL, \
                password VARCHAR ( 50 ) NOT NULL, \
                email VARCHAR ( 255 ) UNIQUE NOT NULL, \
                created_on TIMESTAMP NOT NULL, \
                last_login TIMESTAMP );")

conn.commit()


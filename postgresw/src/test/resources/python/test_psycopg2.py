import psycopg2

# can also use a standard URL format for the connection string:
#   psycopg2.connect('postgres://username:password@host:port/database')
with psycopg2.connect(user="root", password="playwithdata",
                    host='localhost',
                    port='5432',
                    dbname='postgresdb',
                    sslmode='disable'
                    ) as connection:
    connection.autocommit = True

    with connection.cursor() as cursor:
        # list all tables in this database
        cursor.execute('select * from schema:database')
        results = cursor.fetchall()
        print(results)

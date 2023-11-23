import pg8000

conn = pg8000.connect(host="localhost", port="5432", ssl_context=None, database="postgresdb", user="root", password="playwithdata")
conn.autocommit = True

cursor = conn.cursor()

cursor.execute("""
    select * from schema:database
""")

results = cursor.fetchall()
for row in results:
    for column in row:
        print(str(column) + '\n')

conn.close()

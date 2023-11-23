import asyncio
import asyncpg
# import nest_asyncio

# Apply nest_asyncio to enable nested event loops in Jupyter-notebook
# nest_asyncio.apply()

async def run():
    conn = await asyncpg.connect(host="localhost", port="5432", ssl='disable',
                                 database="postgresdb", user="root", password="playwithdata")
    values = await conn.fetch(''' select * from schema:database ''')

    # Print out the results
    i = 1
    for row in values:
        print(i, row)
        i = i + 1

    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())

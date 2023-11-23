from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    'postgresql+pg8000://root:playwithdata@localhost:5432/postgresdb',
    isolation_level="AUTOCOMMIT",
    connect_args={'ssl_context': None}
)
df = pd.read_sql_query("select * from schema:database", con=engine)
print(df)
engine.dispose()

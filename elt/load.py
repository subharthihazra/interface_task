import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


df = pd.read_excel("./files/Merged_Report5.xlsx")

print(df.head())

db_url = os.getenv("DATABASE_URL")

engine = create_engine(db_url)

df.to_sql("merged_data", engine, if_exists="replace", index=False)

print("Done.")

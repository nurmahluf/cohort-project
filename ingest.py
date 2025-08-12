import os
import duckdb
import pandas as pd

CSV_PATH = os.path.join("data", "Merged_master_file.csv")
DB_PATH  = "cohorts.duckdb"


df = pd.read_csv(CSV_PATH)
df.columns = [c.strip() for c in df.columns]

con = duckdb.connect(DB_PATH)
con.execute("DROP TABLE IF EXISTS raw")
con.register("df_view", df)
con.execute("CREATE TABLE raw AS SELECT * FROM df_view")

#  detect study column name safely
cols = con.execute("PRAGMA table_info('raw')").df()["name"].tolist()
candidates = ["Study", "Study Name", "study", "StudyName", "Study_Name"]
study_cols = [c for c in candidates if c in cols]

if not study_cols:
    raise RuntimeError(
        f"Could not find a study column. Available columns:\n{cols}"
    )


parts = []
for sc in study_cols:
    parts.append(f'SELECT DISTINCT "{sc}" AS study_name FROM raw WHERE "{sc}" IS NOT NULL')

view_sql = " CREATE OR REPLACE VIEW studies AS \n" + " \nUNION\n ".join(parts)
con.execute(view_sql)

con.close()
print(" Created cohorts.duckdb with table 'raw' and view 'studies'")

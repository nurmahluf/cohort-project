import duckdb

DB_PATH = "cohorts.duckdb"
con = duckdb.connect(DB_PATH)

# show all studioes 
print("\n=== Available studies ===")
print(con.execute("SELECT * FROM studies ORDER BY study_name").df())

# to Detect the correct study column name
cols = con.execute("PRAGMA table_info('raw')").df()["name"].tolist()
study_col = None
for cand in ["Study", "Study Name", "study", "StudyName", "Study_Name"]:
    if cand in cols:
        study_col = cand
        break

if study_col:

    first_study = con.execute("SELECT study_name FROM studies LIMIT 1").fetchone()[0]
    df = con.execute(f'SELECT * FROM raw WHERE "{study_col}" = ?', [first_study]).df()
    print(f"\n=== First 5 rows from cohort: {first_study} ===")
    print(df.head())
else:
    print("\n No study column found in raw table")

con.close()



from typing import Dict, Iterable, Optional
import duckdb
import pandas as pd


def build_db_from_csv(csv_path: str, db_path: str = "cohorts.duckdb", table: str = "raw") -> None:
    """Full rebuild: create/replace the DuckDB table from a CSV. Simple & safe for small data."""
    con = duckdb.connect(db_path)
    try:
        df = pd.read_csv(csv_path)
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")

        cols = con.execute(f"PRAGMA table_info('{table}')").df()["name"].tolist()
        study_col = "Study Name" if "Study Name" in cols else ("Study" if "Study" in cols else None)
        if study_col:
            con.execute("DROP VIEW IF EXISTS studies")
            con.execute(f"""
                CREATE VIEW studies AS
                SELECT DISTINCT "{study_col}" AS study_name
                FROM {table}
                WHERE "{study_col}" IS NOT NULL
            """)
    finally:
        con.close()


def append_rows_from_csv(csv_path: str, db_path: str = "cohorts.duckdb", table: str = "raw") -> None:
    """Append new rows (no dedup). Use when the CSV only contains brand-new patients."""
    con = duckdb.connect(db_path)
    try:
        df_new = pd.read_csv(csv_path)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df_new LIMIT 0")
        con.execute(f"INSERT INTO {table} SELECT * FROM df_new")
    finally:
        con.close()


def upsert_rows_from_csv(csv_path: str,
                         db_path: str = "cohorts.duckdb",
                         table: str = "raw",
                         id_cols: Iterable[str] = ("Image/Patient ID", "Study Name")) -> None:
    """
    Basic UPSERT: delete rows that have same keys (id_cols), then insert new rows.
    Simple and beginner-friendly; good enough for small data.
    """
    con = duckdb.connect(db_path)
    try:
        df_new = pd.read_csv(csv_path)
        con.register("incoming", df_new)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM incoming LIMIT 0")

        conditions = " AND ".join([f't."{c}" = i."{c}"' for c in id_cols])
        con.execute(f"""DELETE FROM {table} t USING incoming i WHERE {conditions}""")
        con.execute(f"""INSERT INTO {table} SELECT * FROM incoming""")
    finally:
        con.close()


 

def list_studies(db_path: str = "cohorts.duckdb") -> pd.DataFrame:
    con = duckdb.connect(db_path)
    try:
        return con.execute("SELECT * FROM studies ORDER BY study_name").df()
    finally:
        con.close()


def query_cohort(study_name: str,
                 db_path: str = "cohorts.duckdb",
                 table: str = "raw",
                 filters: Optional[Dict[str, object]] = None,
                 organs: Optional[Iterable[str]] = None,
                 kinds: Optional[Iterable[str]] = None,
                 limit: Optional[int] = None) -> pd.DataFrame:
    """Beginner version of your demo: pick a study, optional filters, optional metric columns."""
    con = duckdb.connect(db_path)
    try:
        cols = con.execute(f"PRAGMA table_info('{table}')").df()["name"].tolist()
        study_col = "Study Name" if "Study Name" in cols else ("Study" if "Study" in cols else None)
        if not study_col:
            raise RuntimeError("No 'Study Name' or 'Study' column found.")

        # WHERE parts
        where_parts = [f'"{study_col}" = ?']
        params = [study_name]

        if filters:
            for col, val in filters.items():
                if isinstance(val, tuple) and len(val) == 2:
                    op, v = val
                    where_parts.append(f'"{col}" {op} ?')
                    params.append(v)
                else:
                    where_parts.append(f'"{col}" = ?')
                    params.append(val)

        where_sql = " AND ".join(where_parts) if where_parts else "1=1"

        # SELECT columns
        if organs or kinds:
            def matches(name: str) -> bool:
                n = name.lower()
                ok_org = any(o.lower() in n for o in organs) if organs else True
                ok_kind = any(k.lower() in n for k in kinds) if kinds else True
                return ok_org and ok_kind

            keep = [c for c in cols if matches(c)]
            base = [study_col]
            for c in ["Image/Patient ID", "Patient ID", "Image ID", "ID"]:
                if c in cols and c not in base:
                    base.append(c)
                    break
            select_cols = base + keep
            select_sql = ", ".join(f'"{c}"' for c in select_cols)
        else:
            select_sql = "*"

        limit_sql = f" LIMIT {int(limit)}" if limit else ""
        sql = f'SELECT {select_sql} FROM {table} WHERE {where_sql}{limit_sql}'
        return con.execute(sql, params).df()
    finally:
        con.close()


def save_to_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)


def save_to_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, index=False)

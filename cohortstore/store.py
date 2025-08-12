# cohort_store.py
from __future__ import annotations

import duckdb
from typing import Any, Dict, Iterable, List, Optional, Tuple


class CohortStore:
    """
    Lightweight query layer over a DuckDB table of cohort data.

    Example:
        cs = CohortStore("cohorts.duckdb", table="raw")
        df = (
            cs.reset()
              .select_cohort("UK Biobank")
              .filter(Sex="f", Age=(">", 70))
              .metrics(organs=["Liver"], kinds=["Volume", "SUVMean"])
              .to_pandas()
        )

    API:
        - list_studies() -> pandas.DataFrame
        - select_cohort(name: str) -> CohortStore
        - filter(**columns) -> CohortStore
            e.g. filter(Sex='f', Age=('>', 70))
            tuple format is (op, value) where op in {'>','>=','<','<=','=','!=','LIKE'}
        - metrics(organs=[...], kinds=[...]) -> CohortStore
            picks columns by name tokens (case-insensitive),
            organs AND (OR over kinds) logic
        - to_pandas() / to_polars()
        - query_sql() -> str
        - reset() -> CohortStore
    """

    _ALLOWED_OPS: Tuple[str, ...] = (">", ">=", "<", "<=", "=", "!=", "LIKE")

    def __init__(self, db_path: str = "cohorts.duckdb", table: str = "raw") -> None:
        self.db_path = db_path
        self.table = table
        self.con = duckdb.connect(db_path)
        self._study_col = self._detect_study_col()
        self._where_parts: List[str] = []
        self._params: List[Any] = []
        self._select_cols: Optional[List[str]] = None  

    # internals 

    def _detect_study_col(self) -> str:
        cols = (
            self.con.execute(f'PRAGMA table_info("{self.table}")')
            .df()["name"]
            .tolist()
        )
        for c in ("Study", "Study Name", "study", "StudyName", "Study_Name"):
            if c in cols:
                return c
        raise RuntimeError(f"No study column found in table '{self.table}'.")

    @staticmethod
    def _quote_ident(name: str) -> str:
        """Safely double-quote a DuckDB identifier."""
        return '"' + name.replace('"', '""') + '"'

    def _build_sql(self) -> Tuple[str, List[Any]]:
        where_sql = " AND ".join(self._where_parts) if self._where_parts else "1=1"

        if self._select_cols is None:
            select = "*"
        else:
            select = ", ".join(self._quote_ident(c) for c in self._select_cols)

        sql = f"SELECT {select} FROM {self._quote_ident(self.table)} WHERE {where_sql}"
        return sql, list(self._params)

    #  Basic ops 

    def reset(self) -> "CohortStore":
        """Clear previous selections/filters."""
        self._where_parts, self._params, self._select_cols = [], [], None
        return self

    def list_studies(self):
        """
        Return a DataFrame of the available studies.
        Expects a table named `studies` with a `study_name` column.
        """
        return self.con.execute("SELECT * FROM studies ORDER BY study_name").df()

    def select_cohort(self, name: str) -> "CohortStore":
        self._where_parts.append(f"{self._quote_ident(self._study_col)} = ?")
        self._params.append(name)
        return self

    def filter(self, **columns: Dict[str, Any]) -> "CohortStore":
        """
        Column filters:
            filter(Sex='f', Age=('>', 70))

        Tuple values use (op, value) where op in {'>','>=','<','<=','=','!=','LIKE'}.
        """
        for col, val in columns.items():
            col_q = self._quote_ident(col)
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                op_u = str(op).upper()
                if op_u not in self._ALLOWED_OPS:
                    raise ValueError(
                        f"Unsupported operator '{op}' for column '{col}'. "
                        f"Allowed: {self._ALLOWED_OPS}"
                    )
                self._where_parts.append(f"{col_q} {op_u} ?")
                self._params.append(v)
            else:
                self._where_parts.append(f"{col_q} = ?")
                self._params.append(val)
        return self

    def metrics(
        self,
        organs: Optional[Iterable[str]] = None,
        kinds: Optional[Iterable[str]] = None,
    ) -> "CohortStore":
        """
        Pick measurement columns whose names contain tokens (case-insensitive).

        Uses AND between groups and OR within each group:
            organs=['Liver'] and kinds=['Volume','SUVMean']
        -> matches columns that contain 'Liver' AND (either 'Volume' OR 'SUVMean').

        The detected study column and a single ID-like column are always included if present.
        """
        colnames: List[str] = (
            self.con.execute(f'PRAGMA table_info("{self.table}")').df()["name"].tolist()
        )

        organs_list = list(organs) if organs else []
        kinds_list = list(kinds) if kinds else []

        def matches(name: str) -> bool:
            n = name.lower()
            ok_org = any(o.lower() in n for o in organs_list) if organs_list else True
            ok_kind = any(k.lower() in n for k in kinds_list) if kinds_list else True
            return ok_org and ok_kind

        keep: List[str] = [c for c in colnames if matches(c)]

        # Always include study/id column(s) if present
        base_cols: List[str] = [self._study_col] if self._study_col in colnames else []

        for candidate in ("Image/Patient ID", "Patient ID", "Image ID", "ID"):
            if candidate in colnames and candidate not in base_cols:
                base_cols.append(candidate)
                break

        # De-duplicate while keeping order
        seen = set()
        ordered: List[str] = []
        for c in base_cols + keep:
            if c not in seen:
                seen.add(c)
                ordered.append(c)

        self._select_cols = ordered
        return self

    # Materialization 

    def to_pandas(self):
        sql, params = self._build_sql()
        return self.con.execute(sql, params).df()

    def to_polars(self):
        """Optional fast path if you installed polars."""
        try:
            import polars as pl  
        except Exception as e:  
            raise RuntimeError(
                "Polars is not installed. Run `pip install polars` to use to_polars()."
            ) from e

        sql, params = self._build_sql()
        pdf = self.con.execute(sql, params).df()
        return pl.from_pandas(pdf)

    #  Convenience 

    def query_sql(self) -> str:
     sql, _ = self._build_sql()   # <-- add the parentheses here
     return sql

    #  Context management 

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass

    def __enter__(self) -> "CohortStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
def save_csv(self, path: str):
    """Materialize current query to CSV."""
    self.to_pandas().to_csv(path, index=False)  # uses pandas to_csv

def save_parquet(self, path: str):
    """Materialize current query to Parquet (columnar, compressed)."""
    self.to_pandas().to_parquet(path)  # uses pandas to_parquet (PyArrow backend)

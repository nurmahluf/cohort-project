from cohortstore import CohortStore

store = CohortStore("cohorts.duckdb")

print("\n=== Studies ===")
print(store.list_studies())

first_study = store.list_studies().iloc[0]["study_name"]

# Example A: cohort + simple filters 
df_a = (
    store.reset()
         .select_cohort(first_study)
         
         .to_pandas()
)
print(f"\n=== Rows for cohort: {first_study} (first 5) ===")
print(df_a.head())

#  Example B: select measurement columns by pattern
df_b = (
    store.reset()
         .select_cohort(first_study)
         .metrics(organs=["Liver"], kinds=["Volume","SUVMean"]) 
         .to_pandas()
)
print("\n=== Liver Volume/SUVMean sample (first 5) ===")
print(df_b.head())

#  the generated SQL 
print("\nSQL used for Example B:")
print(store.query_sql())

print("\n=== All column names in 'raw' table ===")
all_cols = store.con.execute("PRAGMA table_info('raw')").df()["name"].tolist()
print(all_cols)

# Example: get Liver Volume + SUVMean columns for the first study
from cohort_store import CohortStore

store = CohortStore("cohorts.duckdb")
first_study = store.list_studies().iloc[0]["study_name"]

df_liver = (
    store.reset()
         .select_cohort(first_study)
         .metrics(organs=["Liver"], kinds=["Volume", "SUVMean"]) 
         .to_pandas()
)
print("\n=== Liver (Volume or SUVMean) sample ===")
print(df_liver.head())

df_filtered = (
    store.reset()
         .select_cohort(first_study)
         .filter(Sex='f', **{"Age Scan":('>', 70)})   
         .metrics(organs=["Liver"], kinds=["Volume","SUVMean"])
         .to_pandas()
)
print("\n=== Females with Age Scan > 70, Liver metrics ===")
print(df_filtered.head())

df_filtered.to_csv("filtered_liver_metrics.csv", index=False)
df_filtered.to_parquet("filtered_liver_metrics.parquet")

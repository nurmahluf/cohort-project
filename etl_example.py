# Simple demo of the wrappers

from etl_wrappers import (
    build_db_from_csv,
    list_studies,
    query_cohort,
    save_to_csv,
    save_to_parquet,
)

# Build DB once
build_db_from_csv(csv_path="data/Merged_master_file.csv", db_path="cohorts.duckdb")

#Show studies
studies = list_studies("cohorts.duckdb")
print("\n=== Studies ===")
print(studies)

first = studies.iloc[0]["study_name"]

#Ex: females Age Scan > 70, liver Volume or SUVMean
df = query_cohort(
    study_name=first,
    db_path="cohorts.duckdb",
    filters={"Sex": "f", "Age Scan": (">", 70)},
    organs=["Liver"],
    kinds=["Volume", "SUVMean"],
)
print("\n=== Result sample ===")
print(df.head())

save_to_csv(df, "out_filtered_liver_metrics.csv")
save_to_parquet(df, "out_filtered_liver_metrics.parquet")
print("\nDone.")
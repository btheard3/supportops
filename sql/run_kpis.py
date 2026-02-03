import duckdb
import pandas as pd
from pathlib import Path

# Paths
DATA_PATH = Path("../data/raw/dataset-tickets-multi-lang-4-20k.csv")
OUTPUT_DIR = Path("../powerbi/data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

# Load CSV as a table
con.execute(f"""
CREATE OR REPLACE TABLE dataset_tickets AS
SELECT *
FROM read_csv_auto('{DATA_PATH}')
""")

# Helper to run query + export
def run_kpi(sql_file, output_name):
    sql = Path(sql_file).read_text()
    df = con.execute(sql).df()
    df.to_csv(OUTPUT_DIR / output_name, index=False)
    print(f"Saved {output_name}")

# Run KPIs
run_kpi("kpi_ticket_volume.sql", "kpi_ticket_volume.csv")
run_kpi("kpi_priority_mix.sql", "kpi_priority_mix.csv")
run_kpi("kpi_queue_workload_concentration.sql", "kpi_queue_concentration.csv")
run_kpi("kpi_ticket_type_mix.sql", "kpi_ticket_type_mix.csv")
run_kpi("kpi_top_recurring_issues.sql", "kpi_top_issues.csv")

con.close()
from pathlib import Path
import pandas as pd
import duckdb


BASE = Path(__file__).parent

BRONZE_DIR = BASE / "lakehouse" / "bronze"
SILVER_DIR = BASE / "lakehouse" / "silver"
GOLD_DIR   = BASE / "lakehouse" / "gold"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

RAW_CSV        = BRONZE_DIR / "orders_raw.csv"
SILVER_PARQUET = SILVER_DIR / "orders_clean.parquet"
GOLD_PARQUET   = GOLD_DIR / "daily_revenue.parquet"



# Bronze

def create_bronze_data():
    rows = [
        {"order_id": 1, "ts": "2026-01-01 10:03:00", "country": "TR", "product": "Keyboard", "qty": 1,  "unit_price": 1200.0},
        {"order_id": 2, "ts": "2026-01-01 11:10:00", "country": "TR", "product": "Mouse",    "qty": 2,  "unit_price": 350.0},
        {"order_id": 3, "ts": "2026-01-01 12:45:00", "country": "",   "product": "Monitor",  "qty": 1,  "unit_price": 4200.0},  
        {"order_id": 4, "ts": "2026-01-02 09:15:00", "country": "DE", "product": "Monitor",  "qty": -1, "unit_price": 4200.0},  
        {"order_id": 5, "ts": "2026-01-02 14:22:00", "country": "TR", "product": "Laptop",   "qty": 1,  "unit_price": None},    
        {"order_id": 6, "ts": "2026-01-03 08:01:00", "country": "US", "product": "Keyboard", "qty": 3,  "unit_price": 1100.0},
        {"order_id": 7, "ts": "2026-01-03 17:40:00", "country": "TR", "product": "Monitor",  "qty": 1,  "unit_price": 4000.0},
    ]
    pd.DataFrame(rows).to_csv(RAW_CSV, index=False)
    print(f"[BRONZE] Raw CSV created -> {RAW_CSV}")



# Silver
def bronze_to_silver():
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        CREATE OR REPLACE TABLE orders_clean AS
        SELECT
            CAST(order_id AS BIGINT)                         AS order_id,
            CAST(ts AS TIMESTAMP)                            AS ts,
            NULLIF(TRIM(country), '')                        AS country,
            TRIM(product)                                    AS product,
            CAST(qty AS INTEGER)                             AS qty,
            CAST(unit_price AS DOUBLE)                       AS unit_price,
            CAST(qty AS DOUBLE) * CAST(unit_price AS DOUBLE) AS revenue
        FROM read_csv_auto('{RAW_CSV.as_posix()}')
        WHERE qty > 0
          AND unit_price IS NOT NULL
          AND TRIM(country) <> ''
    """)
    con.execute(f"""
        COPY orders_clean
        TO '{SILVER_PARQUET.as_posix()}'
        (FORMAT PARQUET);
    """)
    print(f"[SILVER] Clean Parquet created -> {SILVER_PARQUET}")



# Gold
def silver_to_gold():
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        CREATE OR REPLACE TABLE daily_revenue AS
        SELECT
            CAST(date_trunc('day', ts) AS DATE) AS day,
            country,
            COUNT(*) AS order_count,
            SUM(qty) AS units_sold,
            ROUND(SUM(revenue), 2) AS total_revenue
        FROM read_parquet('{SILVER_PARQUET.as_posix()}')
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """)
    con.execute(f"""
        COPY daily_revenue
        TO '{GOLD_PARQUET.as_posix()}'
        (FORMAT PARQUET);
    """)
    print(f"[GOLD] Aggregate Parquet created -> {GOLD_PARQUET}")



# Lakehouse Analytic
def run_analytics():
    con = duckdb.connect(database=":memory:")

    print("\n=== GOLD: Daily Revenue ===")
    print(
        con.execute(
            f"SELECT * FROM read_parquet('{GOLD_PARQUET.as_posix()}')"
        ).df().to_string(index=False)
    )

    print("\n=== SILVER: Top Products ===")
    print(
        con.execute(f"""
            SELECT
                product,
                SUM(qty) AS units,
                ROUND(SUM(revenue), 2) AS revenue
            FROM read_parquet('{SILVER_PARQUET.as_posix()}')
            GROUP BY 1
            ORDER BY revenue DESC;
        """).df().to_string(index=False)
    )

    print("\n=== KPI ===")
    print(
        con.execute(f"""
            SELECT
                ROUND(SUM(revenue), 2) AS total_revenue,
                COUNT(*)               AS clean_orders,
                ROUND(AVG(revenue), 2) AS avg_order_value
            FROM read_parquet('{SILVER_PARQUET.as_posix()}');
        """).df().to_string(index=False)
    )






if __name__ == "__main__":
    create_bronze_data()
    bronze_to_silver()
    silver_to_gold()
    run_analytics()

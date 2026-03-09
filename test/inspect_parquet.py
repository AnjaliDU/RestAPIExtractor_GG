import pandas as pd
from pathlib import Path

PARQUET_PATH = "../out/INTELEX_EU/LOCATIONS/LOCATIONS_1001.parquet"
EXCEL_PATH = "../out/INTELEX_EU/LOCATIONS/LOCATIONS_1001.xlsx"


def main():
    df = pd.read_parquet(PARQUET_PATH)

    print("=== BASIC INFO ===")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    null_count = df.isna().sum()
    null_pct = (null_count / len(df) * 100).round(2)
    distinct = df.nunique(dropna=True)

    summary = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_values": distinct,
    })

    print("\n=== SUMMARY ===")
    print(summary)

    # --- Excel export ---
    Path(EXCEL_PATH).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(EXCEL_PATH, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="data", index=False)
        summary.to_excel(writer, sheet_name="summary")

        # numeric stats u poseban sheet (ako postoje)
        numeric_df = df.select_dtypes(include="number")
        if not numeric_df.empty:
            numeric_df.describe().T.to_excel(writer, sheet_name="numeric_stats")

    print(f"\nExcel exported to: {EXCEL_PATH}")


if __name__ == "__main__":
    main()

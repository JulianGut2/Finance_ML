import os
import pandas as pd

def normalize_esg(raw: pd.DataFrame, provider: str = "kaggle_esg") -> pd.DataFrame:
    df = raw.copy()

    df.columns = [c.strip().lower() for c in df.columns]

    df = df.rename(columns = {
        "total_score" : "total_esg",
        "last_processing_date" : "as_of_date"
        })

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["as_of_date"] = pd.to_datetime(
        df["as_of_date"],
        dayfirst = True,
        errors = "coerce"
    )

    df["provider"] = provider

    cols = [
        "ticker",
        "as_of_date",
        "total_esg",
        "environment_score",
        "social_score",
        "governance_score",
        "total_grade",
        "provider",
    ]

    cols = [c for c in cols if c in df.columns]

    output = df[cols].drop_duplicates().reset_index(drop = True)

    return output

def fetch_esg_from_csv(
        csv_path: str,
        cache_dir: str = "data/raw",
        force_refresh: bool = False,
        provider: str = "kaggle_esg"
) -> pd.DataFrame:
    os.makedirs(cache_dir, exist_ok = True)
    cache_path = os.path.join(cache_dir,f"esg_{provider}.parquet")

    if (not force_refresh) and os.path.exists(cache_path):
        print(f"[cache hit] {cache_path}")

    raw = pd.read_csv(csv_path)
    tidy = normalize_esg(raw, provider = provider)

    tidy.to_parquet(cache_path, index = False)
    print(f"[saved] {cache_path} rows = {len(tidy)}")
    
    return tidy
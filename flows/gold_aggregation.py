from io import BytesIO
from pathlib import Path

from prefect import flow, task

from flows.config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client
import pandas as pd
import numpy as np


def _read_silver_csv(client, name: str) -> pd.DataFrame:
    resp = client.get_object(BUCKET_SILVER, name)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(BytesIO(data))


@task(name="gold_aggregations", retries=1)
def gold_aggregations(clients_obj: str = "clients.csv", achats_obj: str = "achats.csv") -> dict:
    """
    Load silver data, compute KPIs, create fact/dimension tables and temporal aggregations,
    and store results as CSVs in the gold bucket.
    Returns a dict with object names written to gold.
    """
    client = get_minio_client()

    # Read silver tables
    df_clients = _read_silver_csv(client, clients_obj)
    df_achats = _read_silver_csv(client, achats_obj)

    # Ensure typed columns
    df_clients['id_client'] = pd.to_numeric(df_clients['id_client'], errors='coerce').astype('Int64')
    if 'date_inscription' in df_clients.columns:
        df_clients['date_inscription'] = pd.to_datetime(df_clients['date_inscription'], errors='coerce')

    df_achats['id_achat'] = pd.to_numeric(df_achats['id_achat'], errors='coerce').astype('Int64')
    df_achats['id_client'] = pd.to_numeric(df_achats['id_client'], errors='coerce').astype('Int64')
    df_achats['date_achat'] = pd.to_datetime(df_achats['date_achat'], errors='coerce')
    df_achats['montant'] = pd.to_numeric(df_achats['montant'], errors='coerce')

    # Build dimensions
    dim_client = df_clients[['id_client', 'nom', 'email', 'pays', 'date_inscription']].drop_duplicates(subset=['id_client']).reset_index(drop=True)

    # Date dimension
    df_dates = df_achats[['date_achat']].dropna().drop_duplicates().reset_index(drop=True)
    df_dates['date'] = df_dates['date_achat'].dt.date
    df_dates['day'] = df_dates['date_achat'].dt.day
    df_dates['week'] = df_dates['date_achat'].dt.isocalendar().week
    df_dates['month'] = df_dates['date_achat'].dt.month
    df_dates['year'] = df_dates['date_achat'].dt.year
    dim_date = df_dates[['date', 'day', 'week', 'month', 'year']].drop_duplicates().reset_index(drop=True)

    # Fact table: purchases
    fact_purchases = df_achats.merge(dim_client[['id_client', 'pays']], on='id_client', how='left')
    fact_purchases = fact_purchases[['id_achat', 'id_client', 'date_achat', 'montant', 'produit', 'pays']]

    # KPIs
    total_revenue = fact_purchases['montant'].sum()
    total_transactions = fact_purchases.shape[0]
    avg_order_value = fact_purchases['montant'].mean() if total_transactions > 0 else 0

    # Temporal aggregations
    fact_purchases['date'] = fact_purchases['date_achat'].dt.date
    agg_daily = fact_purchases.groupby('date').agg(
        transactions=('id_achat','count'),
        revenue=('montant','sum'),
        avg_ticket=('montant','mean')
    ).reset_index()

    agg_month = fact_purchases.copy()
    agg_month['year_month'] = fact_purchases['date_achat'].dt.to_period('M')
    agg_month = agg_month.groupby('year_month').agg(revenue=('montant','sum')).reset_index()
    agg_month['year_month'] = agg_month['year_month'].astype(str)
    agg_month = agg_month.sort_values('year_month')

    # Growth rate month-over-month
    agg_month['revenue_prev'] = agg_month['revenue'].shift(1)
    agg_month['pct_change'] = ((agg_month['revenue'] - agg_month['revenue_prev']) / agg_month['revenue_prev']).replace([np.inf, -np.inf], np.nan)

    # Revenue by country
    revenue_by_country = fact_purchases.groupby('pays').agg(revenue=('montant','sum'), transactions=('id_achat','count')).reset_index()

    # Statistical distributions per product
    stats_by_product = fact_purchases.groupby('produit').agg(
        count=('id_achat','count'),
        revenue=('montant','sum'),
        mean=('montant','mean'),
        median=('montant', lambda x: x.median()),
        std=('montant','std')
    ).reset_index()

    # Prepare outputs to write to gold bucket
    outputs = {
        'dim_client.csv': dim_client,
        'dim_date.csv': dim_date,
        'fact_purchases.csv': fact_purchases,
        'agg_daily.csv': agg_daily,
        'agg_monthly.csv': agg_month,
        'revenue_by_country.csv': revenue_by_country,
        'stats_by_product.csv': stats_by_product,
        'kpis.csv': pd.DataFrame([{
            'total_revenue': total_revenue,
            'total_transactions': total_transactions,
            'avg_order_value': avg_order_value
        }])
    }

    # Ensure gold bucket exists
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    written = {}
    for name, df in outputs.items():
        out = BytesIO()
        df.to_csv(out, index=False)
        out.seek(0)
        client.put_object(BUCKET_GOLD, name, out, length=out.getbuffer().nbytes)
        written[name] = f"{BUCKET_GOLD}/{name}"

    print(f"Gold objects written: {list(written.keys())}")
    return written


@flow(name="gold Aggregation Flow")
def gold_aggregation_flow(clients_obj: str = 'clients.csv', achats_obj: str = 'achats.csv') -> dict:
    """
    Orchestrator flow for generating gold artifacts from silver.
    """
    return gold_aggregations(clients_obj, achats_obj)


if __name__ == '__main__':
    res = gold_aggregation_flow()
    print('gold aggregation complete:', res)
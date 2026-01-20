from io import BytesIO
from pathlib import Path
import importlib.util
import sys

import pandas as pd
import plotly.express as px
import streamlit as st


def load_flows_config():
    cfg_path = Path(__file__).resolve().parents[1] / "flows" / "config.py"
    spec = importlib.util.spec_from_file_location("flows_config", str(cfg_path))
    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    return cfg


@st.cache_resource
def get_minio_client_and_bucket():
    cfg = load_flows_config()
    client = cfg.get_minio_client()
    bucket = cfg.BUCKET_GOLD
    return client, bucket


def read_csv_from_bucket(client, bucket, name: str) -> pd.DataFrame:
    resp = client.get_object(bucket, name)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(BytesIO(data))


def main():
    st.set_page_config(page_title="Gold Dashboard", layout="wide")
    st.title("Dashboard — Données Gold")

    client, bucket = get_minio_client_and_bucket()

    # lister les objets dans le bucket gold
    try:
        objects = [obj.object_name for obj in client.list_objects(bucket, recursive=False)]
    except Exception as e:
        st.error(f"Impossible de lister les objets dans le bucket '{bucket}': {e}")
        return

    if not objects:
        st.info(f"Aucun objet trouvé dans le bucket '{bucket}'")
        return

    st.sidebar.header("Sélection des fichiers")
    selected = st.sidebar.multiselect("Fichiers gold", options=objects, default=objects)

    for name in selected:
        with st.expander(name, expanded=True):
            try:
                df = read_csv_from_bucket(client, bucket, name)
            except Exception as e:
                st.error(f"Erreur lecture '{name}': {e}")
                continue

            st.write(df.head(100))

            
            if name.lower().startswith("kpis"):
                if not df.empty:
                    row = df.iloc[0]
                    cols = st.columns(3)
                    cols[0].metric("Revenu total", f"{row.get('total_revenue', 0):.2f}")
                    cols[1].metric("Nombre total de transactions", int(row.get('total_transactions', 0)))
                    cols[2].metric("Valeur moyenne des commandes", f"{row.get('avg_order_value', 0):.2f}")

            if name.lower().startswith("agg_daily") or name.lower().startswith("agg_") and 'daily' in name.lower():
                if 'date' in df.columns and 'revenue' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    fig = px.line(df, x='date', y='revenue', title='Revenu quotidien')
                    st.plotly_chart(fig, use_container_width=True)

            if name.lower().startswith("agg_monthly") or name.lower().startswith("agg_") and 'month' in name.lower():
                if 'year_month' in df.columns and 'revenue' in df.columns:
                    df['year_month'] = pd.to_datetime(df['year_month'].astype(str))
                    fig = px.bar(df, x='year_month', y='revenue', title='Revenu mensuel')
                    st.plotly_chart(fig, use_container_width=True)

            if name.lower().startswith("revenue_by_country") or 'pays' in df.columns:
                if 'pays' in df.columns and 'revenue' in df.columns:
                    fig = px.bar(df.sort_values('revenue', ascending=False), x='pays', y='revenue', title='Revenu par pays')
                    st.plotly_chart(fig, use_container_width=True)

            if name.lower().startswith("stats_by_product") or 'produit' in df.columns:
                if 'produit' in df.columns and 'revenue' in df.columns:
                    fig = px.bar(df.sort_values('revenue', ascending=False), x='produit', y='revenue', title='Revenu par produit')
                    st.plotly_chart(fig, use_container_width=True)

            
            if name.lower().startswith('fact_purchases') or 'id_achat' in df.columns:
                if 'date_achat' in df.columns and 'montant' in df.columns:
                    df['date_achat'] = pd.to_datetime(df['date_achat'])
                    df['date'] = df['date_achat'].dt.date
                    daily = df.groupby('date').agg(revenue=('montant','sum')).reset_index()
                    daily['date'] = pd.to_datetime(daily['date'])
                    fig = px.line(daily, x='date', y='revenue', title='Revenu (recalculé)')
                    st.plotly_chart(fig, use_container_width=True)

    st.sidebar.markdown("---")
    st.sidebar.write(f"Bucket: **{bucket}**")


if __name__ == '__main__':
    main()

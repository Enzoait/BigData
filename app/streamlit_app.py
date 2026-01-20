from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import requests
from flows.config import ENDPOINTS_LIST
from datetime import datetime


@st.cache_data(ttl=60)
def get_data_from_endpoint(endpoint: str):
    """Return tuple: (DataFrame, latest_written_at: datetime|None, api_time: datetime|None)"""
    try:
        url = f"http://localhost:5000/{endpoint}"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            payload = response.json()
            # on vérifie si c une liste brute ou un dict avec data + timestamps
            if isinstance(payload, list):
                return pd.DataFrame(payload), None, None
            data = payload.get('data', [])
            latest_ts = payload.get('latest_written_at')
            api_time = payload.get('api_time')
            try:
                latest_dt = pd.to_datetime(latest_ts) if latest_ts else None
            except Exception:
                latest_dt = None
            try:
                api_dt = pd.to_datetime(api_time) if api_time else None
            except Exception:
                api_dt = None
            return pd.DataFrame(data), latest_dt, api_dt
        else:
            st.error(f"Erreur {response.status_code} pour l'endpoint '{endpoint}'")
    except Exception as e:
        st.error(f"Erreur lors de la requête vers '{endpoint}': {e}")
    return pd.DataFrame(), None, None


def main():
    st.set_page_config(page_title="Dashboard", layout="wide")
    st.title("Dashboard")

    endpoints = ENDPOINTS_LIST if isinstance(ENDPOINTS_LIST, list) else list(ENDPOINTS_LIST)

    # chargement des données (renvoie aussi timestamps pour mesurer la fraîcheur)
    agg_daily, agg_daily_latest, agg_daily_api = get_data_from_endpoint('agg_daily') if 'agg_daily' in endpoints else (pd.DataFrame(), None, None)
    agg_monthly, agg_monthly_latest, agg_monthly_api = get_data_from_endpoint('agg_monthly') if 'agg_monthly' in endpoints else (pd.DataFrame(), None, None)
    revenue_by_country, revenue_by_country_latest, revenue_by_country_api = get_data_from_endpoint('revenue_by_country') if 'revenue_by_country' in endpoints else (pd.DataFrame(), None, None)
    stats_by_product, stats_by_product_latest, stats_by_product_api = get_data_from_endpoint('stats_by_product') if 'stats_by_product' in endpoints else (pd.DataFrame(), None, None)
    kpis_df, kpis_latest, kpis_api = get_data_from_endpoint('kpis') if 'kpis' in endpoints else (pd.DataFrame(), None, None)
    fact_purchases, fact_purchases_latest, fact_purchases_api = get_data_from_endpoint('fact_purchase') if 'fact_purchase' in endpoints or 'fact_purchases' in endpoints else (pd.DataFrame(), None, None)

    # construire un mini tableau de fraîcheur (client vs serveur)
    now = pd.to_datetime(datetime.utcnow())
    freshness_rows = []
    def add_freshness(name, latest, api_t):
        if latest is None:
            freshness_rows.append({'endpoint': name, 'latest_written_at': None, 'server_lag_s': None, 'client_lag_s': None})
            return
        api_dt = pd.to_datetime(api_t) if api_t is not None else None
        latest_dt = pd.to_datetime(latest)
        server_lag = (api_dt - latest_dt).total_seconds() if api_dt is not None else None
        client_lag = (now - latest_dt).total_seconds()
        freshness_rows.append({'endpoint': name, 'latest_written_at': latest_dt, 'server_lag_s': server_lag, 'client_lag_s': client_lag})

    add_freshness('agg_daily', agg_daily_latest, agg_daily_api)
    add_freshness('agg_monthly', agg_monthly_latest, agg_monthly_api)
    add_freshness('revenue_by_country', revenue_by_country_latest, revenue_by_country_api)
    add_freshness('stats_by_product', stats_by_product_latest, stats_by_product_api)
    add_freshness('kpis', kpis_latest, kpis_api)
    add_freshness('fact_purchase', fact_purchases_latest, fact_purchases_api)

    freshness_df = pd.DataFrame(freshness_rows)
    # affiche le refresh dans la sidebar
    st.sidebar.subheader('Refresh')
    if not freshness_df.empty:
        disp = freshness_df.copy()
        if 'latest_written_at' in disp.columns:
            # s'assurer que c une datetime
            disp['latest_written_at'] = pd.to_datetime(disp['latest_written_at'], errors='coerce')
            disp['latest_written_at'] = disp['latest_written_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
            disp['latest_written_at'] = disp['latest_written_at'].fillna('N/A')
        st.sidebar.table(disp)

    # filtres
    st.sidebar.header("Filtres")

    # selectionneur de dates
    date_min, date_max = None, None
    if not agg_daily.empty and 'date' in agg_daily.columns:
        agg_daily['date'] = pd.to_datetime(agg_daily['date'])
        date_min = agg_daily['date'].min().date()
        date_max = agg_daily['date'].max().date()
    elif not fact_purchases.empty and 'date_achat' in fact_purchases.columns:
        fact_purchases['date_achat'] = pd.to_datetime(fact_purchases['date_achat'])
        date_min = fact_purchases['date_achat'].min().date()
        date_max = fact_purchases['date_achat'].max().date()

    if date_min and date_max:
        dr = st.sidebar.date_input("Période", value=(date_min, date_max), min_value=date_min, max_value=date_max)
    else:
        dr = None

    # filtre pour les pays et produits
    countries = revenue_by_country['pays'].dropna().unique().tolist() if not revenue_by_country.empty and 'pays' in revenue_by_country.columns else []
    products = stats_by_product['produit'].dropna().unique().tolist() if not stats_by_product.empty and 'produit' in stats_by_product.columns else []
    country_sel = st.sidebar.multiselect('Pays', options=countries, default=countries[:5])
    product_sel = st.sidebar.multiselect('Produits', options=products, default=products[:5])

    st.subheader('KPIs')
    kpi_cols = st.columns(4)
    if not kpis_df.empty:
        row = kpis_df.iloc[0]
        kpi_cols[0].metric('Revenu total', f"{row.get('total_revenue', 0):.2f}€")
        kpi_cols[1].metric('Nombre transactions', int(row.get('total_transactions', 0)))
        kpi_cols[2].metric('Panier moyen', f"{row.get('avg_order_value', 0):.2f}€")
        kpi_cols[3].metric('Clients uniques', int(row.get('unique_customers', 0)))
    else:
        if not fact_purchases.empty and 'montant' in fact_purchases.columns:
            total_rev = fact_purchases['montant'].sum()
            total_tx = len(fact_purchases)
            avg = total_rev / total_tx if total_tx else 0
            kpi_cols[0].metric('Revenu total', f"{total_rev:.2f}")
            kpi_cols[1].metric('Nombre transactions', int(total_tx))
            kpi_cols[2].metric('Panier moyen', f"{avg:.2f}")
        else:
            kpi_cols[0].write('Aucune donnée KPIs')

    st.markdown('---')

    st.subheader('Tendances du revenu')
    if not agg_daily.empty and {'date', 'revenue'}.issubset(agg_daily.columns):
        df = agg_daily.copy()
        df['date'] = pd.to_datetime(df['date'])
        if dr:
            df = df[(df['date'].dt.date >= dr[0]) & (df['date'].dt.date <= dr[1])]
        fig = px.line(df.sort_values('date'), x='date', y='revenue', title='Revenu quotidien')
        st.plotly_chart(fig, use_container_width=True)
    elif not fact_purchases.empty and {'date_achat', 'montant'}.issubset(fact_purchases.columns):
        df = fact_purchases.copy()
        df['date_achat'] = pd.to_datetime(df['date_achat'])
        df['date'] = df['date_achat'].dt.date
        if dr:
            df = df[(df['date_achat'].dt.date >= dr[0]) & (df['date_achat'].dt.date <= dr[1])]
        daily = df.groupby('date').agg(revenue=('montant', 'sum')).reset_index()
        daily['date'] = pd.to_datetime(daily['date'])
        fig = px.line(daily.sort_values('date'), x='date', y='revenue', title='Revenu quotidien (recalculé)')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Aucune donnée de tendance disponible')
# Monthly
    if not agg_monthly.empty and {'year_month', 'revenue'}.issubset(agg_monthly.columns):
        dfm = agg_monthly.copy()
        dfm['year_month'] = pd.to_datetime(dfm['year_month'].astype(str))
        figm = px.bar(dfm.sort_values('year_month'), x='year_month', y='revenue', title='Revenu mensuel')
        st.plotly_chart(figm, use_container_width=True)

    st.markdown('---')

    cols = st.columns(2)
    with cols[0]:
        st.subheader('Top Pays')
        if not revenue_by_country.empty and {'pays', 'revenue'}.issubset(revenue_by_country.columns):
            dfc = revenue_by_country.copy()
            if country_sel:
                dfc = dfc[dfc['pays'].isin(country_sel)]
            figc = px.bar(dfc.sort_values('revenue', ascending=False), x='pays', y='revenue', title='Revenu par pays')
            st.plotly_chart(figc, use_container_width=True)
        else:
            st.info('Aucune donnée pays')

    with cols[1]:
        st.subheader('Top Produits')
        if not stats_by_product.empty and {'produit', 'revenue'}.issubset(stats_by_product.columns):
            dfp = stats_by_product.copy()
            if product_sel:
                dfp = dfp[dfp['produit'].isin(product_sel)]
            figp = px.bar(dfp.sort_values('revenue', ascending=False).head(20), x='produit', y='revenue', title='Revenu par produit')
            st.plotly_chart(figp, use_container_width=True)
        else:
            st.info('Aucune donnée produit')

    st.markdown('---')

    st.subheader('Transactions récentes')
    if not fact_purchases.empty:
        dfp = fact_purchases.copy()
        # ne montre que les 100 plus récentes
        if 'date_achat' in dfp.columns:
            dfp['date_achat'] = pd.to_datetime(dfp['date_achat'])
            dfp = dfp.sort_values('date_achat', ascending=False)
        st.dataframe(dfp.head(100))
    else:
        st.info('Aucune transaction disponible')


if __name__ == '__main__':
    main()

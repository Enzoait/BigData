from io import BytesIO
from pymongo import MongoClient, UpdateOne
from prefect import flow, task
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from flows.config import MONGO_URI, DATABASE_NAME, BUCKET_GOLD, get_minio_client
from pathlib import Path
from typing import List, Optional
from datetime import datetime

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[DATABASE_NAME]


def _read_gold_csv(client, name: str) -> pd.DataFrame:
    resp = client.get_object(BUCKET_GOLD, name)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return pd.read_csv(BytesIO(data))


@task(name="transform_to_mongo_docs", retries=1)
def transform_to_mongo_docs(gold_files: Optional[List[str]] = None) -> dict:
    """
    Read the specified CSV files from the gold bucket and prepare documents
    to insert into MongoDB.
    """
    client = get_minio_client()

    if gold_files is None:
        gold_files = [
            'dim_client.csv',
            'dim_date.csv',
            'fact_purchases.csv',
            'agg_daily.csv',
            'agg_monthly.csv',
            'revenue_by_country.csv',
            'stats_by_product.csv',
            'kpis.csv',
        ]

    def clean_df(df: pd.DataFrame) -> pd.DataFrame:
        # espaces blancs
        df = df.apply(lambda s: s.str.strip() if s.dtype == 'object' else s)
        # normalisation des dates
        for col in df.columns:
            if 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception:
                    pass
        # convertion des colonnes numériques
        numeric_names = {'montant', 'amount', 'revenue', 'transactions', 'count', 'mean', 'std'}
        for col in df.columns:
            if col.lower() in numeric_names:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        # détecter les colonnes qui serviront de clef primaire
        id_cols = [c for c in df.columns if c.lower().endswith('id') or c.lower().startswith('id_') or c.lower() == 'id']
        if len(id_cols) == 1:
            id_col = id_cols[0]
            # ici renomme la colone avec _id
            df = df.rename(columns={id_col: '_id'})
            try:
                df['_id'] = pd.to_numeric(df['_id'], errors='coerce').astype('Int64')
            except Exception:
                pass
        else:
            # vérifie que c bien en int64 pour les ids
            if '_id' in df.columns:
                try:
                    df['_id'] = pd.to_numeric(df['_id'], errors='coerce').astype('Int64')
                except Exception:
                    pass

        return df

    def normalize(df: pd.DataFrame) -> list:
        docs = []
        for row in df.to_dict(orient='records'):
            doc = {}
            for k, v in row.items():
                if pd.isna(v):
                    doc[k] = None
                elif k == '_id':
                    try:
                        doc[k] = int(v)
                    except Exception:
                        doc[k] = v
                else:
                    if hasattr(v, 'item'):
                        try:
                            doc[k] = v.item()
                        except Exception:
                            doc[k] = v
                    else:
                        doc[k] = v
            docs.append(doc)
        return docs

    result = {}
    for name in gold_files:
        try:
            df = _read_gold_csv(client, name)
        except Exception:
            # skip files that don't exist or fail to read
            continue
        df = clean_df(df)
        collection = Path(name).stem
        unique_key = '_id' if '_id' in df.columns else None
        result[name] = {
            'collection': collection,
            'unique_key': unique_key,
            'docs': normalize(df),
        }

    return result


@task(name="insert_data_to_mongodb", retries=1)
def insert_data_to_mongodb(data: dict) -> None:
    for _, value in data.items():
        collection = mongo_db[value['collection']]
        unique_key = value['unique_key']
        docs = value['docs']

        if not docs:
            continue

        # calcul du temps de refresh
        for doc in docs:
            doc['_written_at'] = datetime.utcnow()

        if unique_key:
            ops = [
                UpdateOne(
                    {unique_key: doc[unique_key]},
                    {'$set': doc},
                    upsert=True,
                )
                for doc in docs
            ]
            if ops:
                collection.bulk_write(ops)
        else:
            # no unique key: try insert_many (unordered) and ignore duplicate errors
            try:
                collection.insert_many(docs, ordered=False)
            except Exception:
                # fallback: insert documents one by one to avoid aborting on errors
                for doc in docs:
                    try:
                        collection.insert_one(doc)
                    except Exception:
                        continue


@flow(name="Transform gold CSV → MongoDB")
def transform_to_mongo_docs_flow(gold_files: Optional[List[str]] = None):
    data = transform_to_mongo_docs(gold_files)
    insert_data_to_mongodb(data)


if __name__ == '__main__':
    transform_to_mongo_docs_flow()

from io import BytesIO
from pathlib import Path

from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_SOURCES, get_minio_client
import pandas as pd
import numpy as np

@task(name="upload_to_sources", retries=2)
def upload_csv_to_souces(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to MinIO sources bucket.

    Args:
        file_path: Path to local CSV file
        object_name: Name of object in MinIO

    Returns:
        Object name in MinIO
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"Uploaded {object_name} to {BUCKET_SOURCES}")
    return object_name

@task(name="copy_to_silver", retries=2)
def copy_to_silver_layer(object_name: str) -> str:
    """
    Copy data from sources to silver bucket (raw data lake layer).

    Args:
        object_name: Name of object to copy

    Returns:
        Object name in silver layer
    """

    return object_name


def _clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    # Supprimer les espaces blancs
    df = df.copy()
    df['nom'] = df['nom'].astype(str).str.strip()
    df['email'] = df['email'].astype(str).str.strip().str.lower()

    # pour les dates
    df['date_inscription'] = pd.to_datetime(df['date_inscription'], errors='coerce')

    # Normaliser les pays
    country_map = {
        'Netherland': 'Netherlands',
        'Netherland ': 'Netherlands',
        'UK': 'United Kingdom'
    }
    df['pays'] = df['pays'].astype(str).str.strip().replace(country_map)

    # les rows null
    df = df.dropna(subset=['id_client'])

    df = df.sort_values('date_inscription').drop_duplicates(subset=['id_client'], keep='last')

    return df

def _clean_achats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # pour les dates
    df['date_achat'] = pd.to_datetime(df['date_achat'], errors='coerce')

    # Coerce montant
    df['montant'] = pd.to_numeric(df['montant'], errors='coerce')

    # null
    df = df.dropna(subset=['id_achat', 'id_client', 'date_achat', 'montant'])

    # Supprimer les montants négatifs
    df = df[df['montant'] >= 0]

    # Supprimer les valeurs aberrantes extrêmes en utilisant la règle IQR (3*IQR)
    q1 = df['montant'].quantile(0.25)
    q3 = df['montant'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 3 * iqr
    upper_bound = q3 + 3 * iqr
    df = df[(df['montant'] >= lower_bound) & (df['montant'] <= upper_bound)]

    df = df.sort_values('date_achat').drop_duplicates(subset=['id_achat'], keep='last')

    return df


@task(name="transform_and_store_silver", retries=2)
def transform_and_store_silver(object_name: str) -> str:
    """
    Read object from sources bucket, apply silver transformations and store result in silver bucket.
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))

    original_count = len(df)

    if 'clients' in object_name.lower() or 'client' in object_name.lower():
        df_clean = _clean_clients(df)
    elif 'achat' in object_name.lower() or 'achats' in object_name.lower():
        df_clean = _clean_achats(df)
    else:
        df = df.dropna(how='all')
        df_clean = df.drop_duplicates()

    cleaned_count = len(df_clean)

    missing_per_col = df_clean.isna().sum().to_dict()
    dup_count = original_count - cleaned_count
    print(f"Object: {object_name} - original rows: {original_count}, cleaned rows: {cleaned_count}, duplicates_removed: {dup_count}")
    print(f"Missing per column after cleaning: {missing_per_col}")

    out_bytes = BytesIO()
    df_clean.to_csv(out_bytes, index=False)
    out_bytes.seek(0)

    client.put_object(
        BUCKET_SILVER,
        object_name,
        out_bytes,
        length=out_bytes.getbuffer().nbytes
    )

    print(f"Transformed and uploaded {object_name} to {BUCKET_SILVER}")
    return object_name

@flow(name="silver Ingestion Flow")
def silver_ingestion_flow(data_dir: str = "./data/sources") -> dict:
    """
    Main flow: Upload CSV files to sources and copy to silver layer.

    Args:
        data_dir: Directory containing source CSV files

    Returns:
        Dictionary with ingested file names
    """
    data_path = Path(data_dir)

    clients_file = str(data_path / "clients.csv")
    achats_file = str(data_path / "achats.csv")

    clients_name = upload_csv_to_souces(clients_file, "clients.csv")
    achats_name = upload_csv_to_souces(achats_file, "achats.csv")

    silver_clients = transform_and_store_silver(clients_name)
    silver_achats = transform_and_store_silver(achats_name)

    return {
        "clients": silver_clients,
        "achats": silver_achats
    }

if __name__ == "__main__":
    result = silver_ingestion_flow()
    print(f"silver ingestion complete: {result}")
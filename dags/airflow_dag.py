"""
=================================================

Milestone 3

Nama  : Rajib Kurniawa
Batch : FTDS-032-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch.
Adapun dataset yang dipakai adalah dataset mengenai Customer Shopping Behavior yang berisi data aktivitas pembelian pelanggan.
Program ini akan mengekstrak data dari PostgreSQL, melakukan pembersihan (data cleaning & normalisasi nama kolom),
kemudian memuat data hasil transformasi ke Elasticsearch agar dapat divisualisasikan melalui Kibana Dashboard.

Objective:
Sebagai Data Analyst di Rjb Retail, saya menganalisis data perilaku pelanggan untuk mengetahui pola pembelian berdasarkan demografi, kategori produk,
serta metode pembayaran. Hasil analisis ini digunakan untuk memberikan rekomendasi strategi pemasaran dan pengelolaan stok agar penjualan lebih optimal,
meningkatkan efektivitas promosi, mempertahankan pelanggan, serta mendorong peningkatan penjualan secara keseluruhan.

=================================================
"""

# Import Libraries

from airflow import DAG
import pandas as pd
import datetime as dt
from datetime import timedelta
from elasticsearch import Elasticsearch, helpers

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

# untuk connect postgre dengan python
import psycopg2 as db


# Function Section

def extract_data():
    """
    Fungsi ini digunakan untuk mengekstrak data mentah dari PostgreSQL
    dan menyimpannya ke dalam file CSV untuk tahap transformasi berikutnya.

    Tahapan:
    1. Membuat koneksi ke database PostgreSQL dengan credential yang sesuai.
    2. Mengambil seluruh isi tabel "table_m3".
    3. Menyimpan hasil ekstraksi ke file "P2M3_Rajib_Kurniawan_data_raw.csv".

    Output:
    File CSV berisi data mentah (raw data) dari database PostgreSQL.
    """
    conn_string = (
        "dbname='milestone3' "
        "host='postgres' "
        "user='airflow' "
        "password='airflow' "
        "port = 5432"
    )
    conn = db.connect(conn_string)

    table_m3 = pd.read_sql("select * from table_m3", conn)

    table_m3.to_csv("/opt/airflow/dags/P2M3_Rajib_Kurniawan_data_raw.csv")
    print("-------table Saved------")


def transform():
    """
    Fungsi ini digunakan untuk melakukan transformasi dan pembersihan data
    hasil ekstraksi dari PostgreSQL.

    Proses meliputi:
    - Menghapus data duplikat.
    - Menghapus data dengan missing values.
    - Menormalkan nama kolom menjadi huruf kecil (lowercase).
    - Mengganti spasi/simbol pada nama kolom menjadi underscore (_).

    Output:
    File CSV baru berisi data yang telah dibersihkan dan siap dimuat ke Elasticsearch.
    """

    table_m3 = pd.read_csv("/opt/airflow/dags/P2M3_Rajib_Kurniawan_data_raw.csv")

    # Drop Missing Value
    table_m3 = table_m3.dropna()
    # drop duplicated
    table_m3 = table_m3.drop_duplicates()
    # Normalisasi Nama kolom
    table_m3.columns = (
        table_m3.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    table_m3.to_csv(
        "/opt/airflow/dags/P2M3_Rajib_Kurniawan_data_clean.csv", index=False
    )
    print("-------Data Saved------")


def load_data():
    """
    Fungsi ini digunakan untuk memuat data hasil transformasi ke dalam Elasticsearch.

    Langkah-langkah:
    1. Membaca data bersih dari file CSV.
    2. Menghubungkan ke Elasticsearch container yang berjalan di Docker.
    3. Melakukan proses upload menggunakan metode bulk ke index "milestone3".

    Output:
    Data bersih berhasil diupload ke Elasticsearch dan siap digunakan di Kibana.
    """

    table_m3 = pd.read_csv(
        "/opt/airflow/dags/P2M3_Rajib_Kurniawan_data_clean.csv", index_col=0
    )
    es = Elasticsearch("http://elasticsearch:9200")

    actions = [
        {"_index": "milestone3", "_id": i, "_source": r.to_dict()}
        for i, r in table_m3.iterrows()  # or for i,r in df.iterrows()
    ]

    response = helpers.bulk(es, actions)
    print(response)


# DAG Section

default_args = {
    "owner": "Rjbkrwn",
    "start_date": dt.datetime(2024, 11, 2, 14) - dt.timedelta(hours=7),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "milestone3",
    default_args=default_args,
    schedule_interval="10-30/10 9 * * 6",  # '0 * * * *'
    catchup=False,
) as dag:

    print_starting = BashOperator(
        task_id="starting", bash_command='echo "I am reading the CSV now....."'
    )

    extractData = PythonOperator(
        task_id="extract_from_milestone3", python_callable=extract_data
    )

    transformData = PythonOperator(task_id="transform_data", python_callable=transform)

    loadData = PythonOperator(task_id="load_data", python_callable=load_data)

    print_stop = BashOperator(
        task_id="stopping", bash_command='echo "I done converting the CSV"'
    )

# Definisikan Task
print_starting >> extractData >> transformData >> loadData >> print_stop

import psycopg2
import pandas as pd
import os

def load_to_postgres():
    # PostgreSQL bağlantı parametreleri (docker-compose'dan alındı)
    DB_HOST = "postgres"
    DB_NAME = "airflow"
    DB_USER = "airflow"
    DB_PASS = "airflow"

    # Temizlenmiş veri dosyasının mutlak yolu
    clean_file_path = '/opt/airflow/data/crypto_clean.csv'

    # 1. Veritabanına Bağlanma
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        print("PostgreSQL veritabanına başarıyla bağlanıldı.")

        # 2. Veri Çerçevesini Okuma
        df = pd.read_csv(clean_file_path)

        # 3. Tabloyu Oluşturma (Önce varsa silinir)
        create_table_query = """
        DROP TABLE IF EXISTS crypto_data;
        CREATE TABLE crypto_data (
            coin_id VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            name VARCHAR,
            price NUMERIC,
            market_cap BIGINT,
            volume BIGINT,
            last_updated_at TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        print("Tablo oluşturma başarılı.")

        # 4. Verileri Yükleme
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO crypto_data (coin_id, symbol, name, price, market_cap, volume, last_updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_id) DO NOTHING;
            """
            cur.execute(insert_query, tuple(row))
        
        conn.commit()
        print(f"PostgreSQL'e {len(df)} satır veri başarıyla yüklendi.")

    except (Exception, psycopg2.Error) as error:
        print(f"Veritabanı işlemi hatası: {error}")
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print("PostgreSQL bağlantısı kapatıldı.")

if __name__ == "__main__":
    load_to_postgres()

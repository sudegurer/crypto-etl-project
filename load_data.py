import psycopg2
import pandas as pd
import logging

# Logging yapılandırması
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_postgres():
    """Temizlenmiş veriyi okur ve Dimension/Fact tablolarına yükler."""
    
    DB_HOST = "postgres"
    DB_NAME = "airflow"
    DB_USER = "airflow"
    DB_PASS = "airflow"
    clean_file_path = '/opt/airflow/data/crypto_clean.csv'

    logging.info("--- [Load] Dimension/Fact Modelleme Aşaması Başladı ---")
    conn = None
    
    try:
        # 1. Veritabanına Bağlanma
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        logging.info("PostgreSQL veritabanına başarıyla bağlanıldı.")

        # 2. Veri Çerçevesini Okuma
        df = pd.read_csv(clean_file_path)
        logging.info(f"Veritabanına yüklenecek satır sayısı: {len(df)}")
        
        # =========================================================
        # A) DIMENSION TABLOSU (dim_coin)
        # Sadece coin'e ait sabit bilgileri (id, symbol, name) içerir
        # =========================================================
        
        # 3. Dim Tablosunu Oluşturma
        create_dim_table_query = """
        DROP TABLE IF EXISTS dim_coin CASCADE; -- Fact tablolarını da etkileyeceği için CASCADE ekledik
        CREATE TABLE dim_coin (
            coin_id VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            name VARCHAR
        );
        """
        cur.execute(create_dim_table_query)
        logging.info("Veritabanında 'dim_coin' dimension tablosu oluşturuldu.")
        
        # 4. Dim Tablosuna Veri Yükleme
        df_dim = df[['coin_id', 'symbol', 'name']].drop_duplicates(subset=['coin_id'])
        
        for index, row in df_dim.iterrows():
            insert_dim_query = """
            INSERT INTO dim_coin (coin_id, symbol, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (coin_id) DO NOTHING;
            """
            cur.execute(insert_dim_query, (row['coin_id'], row['symbol'], row['name']))
        
        logging.info(f"Dim tablosuna {len(df_dim)} eşsiz coin bilgisi yüklendi.")


        # =========================================================
        # B) FACT TABLOSU (fact_crypto_metrics)
        # Metrikleri (price, volume, market_cap) ve zamanı içerir
        # =========================================================
        
        # 5. Fact Tablosunu Oluşturma
        create_fact_table_query = """
        DROP TABLE IF EXISTS fact_crypto_metrics;
        CREATE TABLE fact_crypto_metrics (
            fact_id SERIAL PRIMARY KEY,
            coin_id VARCHAR REFERENCES dim_coin(coin_id), -- Foreign Key bağlantısı
            price NUMERIC,
            market_cap BIGINT,
            volume BIGINT,
            last_updated_at TIMESTAMP WITH TIME ZONE
        );
        """
        cur.execute(create_fact_table_query)
        logging.info("Veritabanında 'fact_crypto_metrics' fact tablosu oluşturuldu.")

        # 6. Fact Tablosuna Veri Yükleme
        df_fact = df[['coin_id', 'price', 'market_cap', 'volume', 'last_updated_at']].copy()
        
        for index, row in df_fact.iterrows():
            insert_fact_query = """
            INSERT INTO fact_crypto_metrics (coin_id, price, market_cap, volume, last_updated_at)
            VALUES (%s, %s, %s, %s, %s);
            """
            cur.execute(insert_fact_query, tuple(row))
        
        logging.info(f"Fact tablosuna {len(df_fact)} metrik kaydı yüklendi.")

        # 7. İşlemi Onaylama
        conn.commit()

    except psycopg2.Error as error:
        logging.error(f"PostgreSQL bağlantı/işlem hatası: {error}")
        if conn: conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Genel hata oluştu: {e}")
        raise
        
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            logging.info("PostgreSQL bağlantısı kapatıldı.")
            
    logging.info("--- [Load] Dimension/Fact Modelleme Aşaması Tamamlandı ---")

if __name__ == "__main__":
    load_to_postgres()

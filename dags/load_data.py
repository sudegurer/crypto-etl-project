import psycopg2
import pandas as pd
import logging

# Logging yapılandırması
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_postgres():
    """Temizlenmiş veriyi okur ve PostgreSQL veritabanına yükler."""
    
    DB_HOST = "postgres"
    DB_NAME = "airflow"
    DB_USER = "airflow"
    DB_PASS = "airflow"
    clean_file_path = '/opt/airflow/data/crypto_clean.csv'

    logging.info("--- [Load] Veritabanı Yükleme Aşaması Başladı ---")
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

        # 3. Tabloyu Oluşturma (Varsa silinir)
        create_table_query = """
        DROP TABLE IF EXISTS crypto_data;
        CREATE TABLE crypto_data (
            coin_id VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            name VARCHAR,
            price NUMERIC,
            market_cap BIGINT,
            volume BIGINT,
            last_updated_at TIMESTAMP WITH TIME ZONE
        );
        """
        cur.execute(create_table_query)
        logging.info("Veritabanında 'crypto_data' tablosu yeniden oluşturuldu.")

        # 4. Verileri Yükleme
        for index, row in df.iterrows():
            insert_query = """
            INSERT INTO crypto_data (coin_id, symbol, name, price, market_cap, volume, last_updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_id) DO NOTHING;
            """
            cur.execute(insert_query, tuple(row))
        
        conn.commit()
        logging.info(f"PostgreSQL'e {len(df)} satır veri başarıyla yüklendi.")

    except psycopg2.Error as error:
        logging.error(f"PostgreSQL bağlantı/işlem hatası: {error}")
        if conn: conn.rollback()
        raise
    except FileNotFoundError:
        logging.error(f"Temizlenmiş veri dosyası bulunamadı: {clean_file_path}")
        raise
    except Exception as e:
        logging.error(f"Genel hata oluştu: {e}")
        raise
        
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            logging.info("PostgreSQL bağlantısı kapatıldı.")
            
    logging.info("--- [Load] Veritabanı Yükleme Aşaması Tamamlandı ---")


if __name__ == "__main__":
    load_to_postgres()

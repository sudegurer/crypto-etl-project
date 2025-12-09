import pandas as pd
import logging

# Logging yapılandırması
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data():
    """Ham kripto verilerini okur, temizler ve yeni bir CSV olarak kaydeder."""
    
    input_path = '/opt/airflow/data/crypto_raw.csv'
    output_path = '/opt/airflow/data/crypto_clean.csv'
    
    logging.info("--- [Transform] Veri Dönüştürme Aşaması Başladı ---")
    
    try:
        # Mutlak yoldan ham veriyi okuma
        df = pd.read_csv(input_path)
        logging.info(f"Okunan ham veri satır sayısı: {len(df)}")
        
        # Gereksiz Kolonları Çıkarma ve Yeniden Adlandırma
        columns_to_keep = [
            'id', 
            'symbol', 
            'name', 
            'current_price', 
            'market_cap', 
            'total_volume',
            'last_updated'
        ]
        
        df_clean = df[columns_to_keep].copy()
        
        df_clean.columns = [
            'coin_id', 
            'symbol', 
            'name', 
            'price', 
            'market_cap', 
            'volume',
            'last_updated_at'
        ]
        
        # Veri Tiplerini Dönüştürme
        df_clean['last_updated_at'] = pd.to_datetime(df_clean['last_updated_at'], utc=True)
        logging.info("Veri tipleri ve kolon isimleri başarıyla dönüştürüldü.")
        
        # İşlenmiş Veriyi Mutlak Yola Kaydetme
        df_clean.to_csv(output_path, index=False)
        logging.info(f"Temizlenmiş veri {len(df_clean)} satır olarak kaydedildi: {output_path}")

    except FileNotFoundError:
        logging.error(f"Dosya bulunamadı: {input_path}. Lütfen 'Extract' adımının çalıştığından emin olun.")
        raise
    except Exception as e:
        logging.error(f"Dönüştürme sırasında beklenmedik bir hata oluştu: {e}")
        raise

    logging.info("--- [Transform] Veri Dönüştürme Aşaması Tamamlandı ---")

if __name__ == "__main__":
    transform_data()

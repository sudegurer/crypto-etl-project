import requests
import pandas as pd
import logging

# Logging yapılandırması
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data():
    """CoinGecko API'den kripto verilerini çeker ve CSV olarak kaydeder."""
    
    API_URL = "https://api.coingecko.com/api/v3/coins/markets"
    
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 100,
        'page': 1,
        'sparkline': 'false'
    }
    
    output_path = '/opt/airflow/data/crypto_raw.csv'
    
    logging.info("--- [Extract] Veri Çekme Aşaması Başladı ---")
    try:
        logging.info(f"API'den ilk {params['per_page']} coin verisi çekiliyor...")
        response = requests.get(API_URL, params=params)
        response.raise_for_status() # HTTP hatalarını yakala
        
        data = response.json()
        df = pd.DataFrame(data)
        
        # Veriyi kaydetme
        df.to_csv(output_path, index=False)
        logging.info(f"API'den {len(df)} satır veri başarıyla çekildi.")
        logging.info(f"Ham veri başarıyla kaydedildi: {output_path}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API bağlantı hatası oluştu: {e}")
        raise
    except Exception as e:
        logging.error(f"Beklenmedik bir hata oluştu: {e}")
        raise
        
    logging.info("--- [Extract] Veri Çekme Aşaması Tamamlandı ---")

if __name__ == "__main__":
    fetch_data()

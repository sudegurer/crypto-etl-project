import pandas as pd

def transform_data():
    
    # Mutlak yoldan ham veriyi okuma
    df = pd.read_csv('/opt/airflow/data/crypto_raw.csv')
    
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
    df_clean['last_updated_at'] = pd.to_datetime(df_clean['last_updated_at'])
    
    # İşlenmiş Veriyi Mutlak Yola Kaydetme
    df_clean.to_csv('/opt/airflow/data/crypto_clean.csv', index=False)
    
if __name__ == "__main__":
    transform_data()

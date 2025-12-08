import requests
import pandas as pd

def fetch_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    data = requests.get(url).json()
    df = pd.DataFrame(data)
    
    # Yerel 'data' klasörüne kaydetmek için '../data/' yolunu kullanıyoruz.
    df.to_csv('/opt/airflow/data/crypto_raw.csv', index=False) 

if __name__ == "__main__":
    fetch_data()

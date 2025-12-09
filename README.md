# Kripto Verileri ETL Pipeline (Extract-Transform-Load)
Bu proje, Python, Docker ve PostgreSQL kullanarak CoinGecko API'den popüler kripto para verilerini günlük olarak çekmek, temizlemek ve analiz için yapılandırılmış bir veritabanına yüklemek üzere tasarlanmış uçtan uca bir veri mühendisliği pipeline'ıdır.

##  Proje Amacı ve Problem Tanımı

**Amaç:** Finansal analiz, tahmin modelleri veya iş zekası (BI) raporları için güvenilir, güncel ve temiz bir kripto veri kaynağı oluşturmak.

**Çözülen Problemler:**
1.  **Dağınık Veri Kaynağı:** CoinGecko API'den gelen ham JSON verisinin tutarsız yapısını standart bir tablo formatına dönüştürmek.
2.  **Veri Sürdürülebilirliği:** Veri akışını manuel çalıştırmak yerine, otomatik ve tekrarlanabilir bir altyapı oluşturmak.

##  Mimari ve Teknolojiler

Proje, üç ana aşamadan oluşur (ETL) ve bu adımlar modüler Python scriptleri ile yönetilir. 

[Image of Simple ETL Pipeline Diagram]


### Kullanılan Teknolojiler

| Kategori | Teknoloji | Amaç |
| :--- | :--- | :--- |
| **Programlama** | Python | ETL mantığı, veri işleme (Pandas) ve API çağrıları (Requests) |
| **Veri Kaynağı** | CoinGecko API | Güncel kripto para piyasası verisi |
| **Veritabanı** | PostgreSQL | Temizlenmiş (Gold Layer) verinin kalıcı olarak depolanması |
| **Altyapı** | Docker & Docker Compose | Servislerin izole ve tekrarlanabilir bir ortamda çalıştırılması |
| **Bağlantı** | Psycopg2 | Python'dan PostgreSQL'e güvenli bağlantı |

##  Pipeline Akışı (ETL)

Pipeline, aşağıdaki adımları sırayla tamamlar:

1.  **Extract (`fetch_crypto_data.py`):** CoinGecko API'ye bağlanır, ilk 100 kripto paranın anlık verilerini çeker ve `data/crypto_raw.csv` dosyasına kaydeder.
2.  **Transform (`transform_data.py`):** Ham CSV dosyasını okur, gereksiz kolonları çıkarır, veri tiplerini temizler ve sonuçları `data/crypto_clean.csv` dosyasına yazar.
3.  **Load (`load_data.py`):** Temizlenmiş CSV dosyasını okur, **Docker Compose** ortamında çalışan PostgreSQL veritabanına bağlanır ve `crypto_data` tablosuna yükler.

##  Kurulum ve Çalıştırma

### Ön Gereksinimler

* Docker ve Docker Compose
* Python 3.8+

### Adım Adım Kurulum

1.  **Depoyu Klonlama:**
    ```bash
    git clone [https://github.com/sudegurer/crypto-etl-project.git](https://github.com/sudegurer/crypto-etl-project.git)
    cd crypto-etl-project
    ```
2.  **Servisleri Başlatma (Altyapı):**
    ```bash
    docker compose up -d
    ```
3.  **ETL Akışını Çalıştırma:**
    *(Altyapı sorunları nedeniyle manuel çalıştırma yolu kullanılmıştır.)*
    ```bash
    docker run --rm \
        --network [PROJECT_NAME]_default \
        -v $(pwd)/dags:/opt/airflow/dags \
        -v $(pwd)/data:/opt/airflow/data \
        apache/airflow:2.8.0-python3.8 \
        python /opt/airflow/dags/fetch_crypto_data.py && \
    # (Diğer adımlar benzer şekilde çalıştırılır...)
    ```
4.  **Veritabanını Kontrol Etme:**
    ```bash
    docker exec -it [POSTGRES_CONTAINER_ID] psql -U airflow -d airflow
    SELECT * FROM crypto_data LIMIT 5;
    \q
    ```

##  Geliştirilen Beceriler ve Öğrenilenler

* **Veri Mühendisliği Pratiği:** Uçtan uca bir ETL hattını tasarlama ve uygulama.
* **Docker ve Orkestrasyon:** Servisler arası ağ iletişimi (Network Bridging), Volume (Birleştirme) ve konteyner yönetiminde derinleşme.
* **Hata Ayıklama (Debugging):** Docker ortamında karşılaşılan **"could not translate host name 'postgres'"** ve **GitHub kimlik doğrulama (403)** hatalarını giderme.
* **Modüler Kodlama:** Her ETL aşamasını ayrı, test edilebilir Python modülleri olarak yapılandırma.

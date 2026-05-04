# 🪙 CryptoWatch: Monitor Pasar Aset Digital
**Proyek Big Data | Platform Edukasi Kripto Real-Time**

---

## 📌 Deskripsi Proyek

Platform edukasi kripto yang menampilkan **dashboard real-time harga aset digital beserta konteks berita** untuk investor pemula. Sistem ini membangun pipeline data end-to-end mulai dari ingestion, storage, processing, hingga serving layer.

### Pertanyaan Bisnis
> *"Pada jam berapa harga kripto paling volatile? Dan apakah berita yang muncul sejalan dengan pergerakan harga?"*

---

## 🗂️ Arsitektur Sistem

```
CoinGecko API          CoinDesk RSS
     │                      │
     ▼                      ▼
[producer_api.py]    [producer_rss.py]
     │                      │
     ▼                      ▼
Kafka Topic:           Kafka Topic:
crypto-api             crypto-rss
     │                      │
     └──────────┬───────────┘
                ▼
     [consumer_to_hdfs.py]
                │
                ▼
         HDFS Storage
    /data/crypto/api/
    /data/crypto/rss/
                │
                ▼
      [Spark analysis.py]
                │
                ▼
    /data/crypto/hasil/
    dashboard/data/spark_results.json
                │
                ▼
       Flask Dashboard
       localhost:5000
```

---

## 📡 Sumber Data

| Komponen | Detail |
|----------|--------|
| **API Real-time** | CoinGecko Simple Price API (gratis, tanpa API key) |
| **Endpoint** | `https://api.coingecko.com/api/v3/simple/price` |
| **Parameter** | `ids`, `vs_currencies`, `include_24hr_change` |
| **Data** | Harga USD/IDR + % perubahan 24 jam untuk BTC, ETH, BNB |
| **Interval Polling** | Setiap 60 detik (rate limit: 30 req/menit gratis) |
| **RSS Feed** | `https://www.coindesk.com/arc/outboundfeeds/rss/` |
| **Backup RSS** | `https://cointelegraph.com/rss` |

---

## 🔧 Kafka Topics

| Topic | Key | Isi |
|-------|-----|-----|
| `crypto-api` | Simbol koin (BTC, ETH, BNB) | Data harga real-time dari CoinGecko |
| `crypto-rss` | Hash 8 karakter dari URL artikel | Data artikel berita kripto |

---

## 📁 Struktur HDFS

```
/data/crypto/
├── api/      ← File JSON dari topic crypto-api
├── rss/      ← File JSON dari topic crypto-rss
└── hasil/    ← Output analisis Spark
```

---

## 🧩 Komponen yang Dibangun

### Komponen 1 — Apache Kafka: Ingestion Layer

**Tujuan:** Pintu masuk semua data dari sumber eksternal sebelum disimpan ke HDFS.

**Yang dibangun:**
- Setup Kafka menggunakan Docker Compose (materi P8)
- 2 Kafka topic: `crypto-api` dan `crypto-rss`
- `producer_api.py` — polling CoinGecko setiap 60 detik
- `producer_rss.py` — polling RSS CoinDesk setiap 5 menit

**Struktur JSON — `crypto-api`:**
```json
{
  "symbol": "BTC",
  "price_usd": 65000.00,
  "price_idr": 1050000000,
  "change_24h": 2.35,
  "timestamp": "2026-04-20T14:30:00Z"
}
```

**Struktur JSON — `crypto-rss`:**
```json
{
  "article_id": "a1b2c3d4",
  "title": "Bitcoin Hits New High...",
  "link": "https://coindesk.com/...",
  "summary": "Bitcoin reached...",
  "published": "2026-04-20T13:00:00Z",
  "timestamp": "2026-04-20T14:30:00Z"
}
```

**Hint teknis:**
- Library: `pip install kafka-python feedparser`
- Producer: gunakan `enable_idempotence=True` dan `acks="all"`
- RSS: parsing dengan `feedparser.parse(url)` — field: `entry.title`, `entry.link`, `entry.summary`, `entry.published`
- Hindari duplikat RSS dengan menyimpan set ID yang sudah dikirim

---

### Komponen 2 — HDFS: Storage Layer

**Tujuan:** Consumer membaca Kafka dan menyimpan ke HDFS sebagai *single source of truth*.

**Yang dibangun:**
- Setup Hadoop via Docker Compose (materi P4)
- Struktur direktori HDFS: `/data/crypto/api/`, `/data/crypto/rss/`, `/data/crypto/hasil/`
- `consumer_to_hdfs.py` — konsumsi kedua topic, buffer data, simpan ke HDFS tiap 2–5 menit

**Hint teknis:**
- `KafkaConsumer` dengan `group_id` unik, `auto_offset_reset="earliest"`
- Nama file HDFS gunakan timestamp: `2026-04-20_14-30.json`
- Strategi penyimpanan:
  - **Opsi A (Mudah):** Simpan lokal → `subprocess.run(["hdfs", "dfs", "-put", file_lokal, path_hdfs])`
  - **Opsi B (Lebih baik):** `pip install hdfs` → tulis langsung ke HDFS
- Gunakan `threading` untuk membaca 2 topic secara paralel
- Consumer juga menyimpan salinan lokal di:
  - `dashboard/data/live_api.json`
  - `dashboard/data/live_rss.json`

---

### Komponen 3 — Apache Spark: Processing Layer

**Tujuan:** Membaca data dari HDFS dan menghasilkan insight bermakna dari data historis.

**Yang dibangun:**
- `spark/analysis.py` atau `spark/analysis.ipynb`
- Membaca dari HDFS (bukan file lokal)
- 3 analisis wajib
- Output disimpan ke HDFS dan `dashboard/data/spark_results.json`

#### 📊 Analisis 1 — Statistik Harga per Koin
```sql
-- groupBy symbol
SELECT symbol,
       AVG(price_usd)    AS rata_rata,
       MAX(price_usd)    AS tertinggi,
       MIN(price_usd)    AS terendah,
       STDDEV(price_usd) AS std_deviasi
FROM crypto_api
GROUP BY symbol
```
> **Interpretasi:** Menunjukkan distribusi harga dan tingkat dispersi setiap koin selama periode pengamatan.

#### 📊 Analisis 2 — Volatilitas per Jam
```sql
-- Rata-rata nilai absolut change_24h per jam
SELECT HOUR(TO_TIMESTAMP(timestamp)) AS jam,
       AVG(ABS(change_24h))           AS avg_volatilitas
FROM crypto_api
GROUP BY jam
ORDER BY avg_volatilitas DESC
```
> **Interpretasi:** Mengidentifikasi jam-jam dengan pergerakan harga paling ekstrem — menjawab pertanyaan bisnis utama.

#### 📊 Analisis 3 — Volume Berita per Jam
```sql
-- Jumlah artikel RSS per jam
SELECT HOUR(TO_TIMESTAMP(timestamp)) AS jam,
       COUNT(*)                       AS jumlah_artikel
FROM crypto_rss
GROUP BY jam
ORDER BY jumlah_artikel DESC
```
> **Interpretasi:** Menunjukkan jam paling aktif berita kripto — dapat dikorelasikan dengan jam volatilitas tertinggi.

**Hint teknis Spark:**
```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

df_api = spark.read.option("multiLine", True).json("hdfs://namenode:8020/data/crypto/api/")
df_rss = spark.read.option("multiLine", True).json("hdfs://namenode:8020/data/crypto/rss/")

df_api.createOrReplaceTempView("crypto_api")
df_rss.createOrReplaceTempView("crypto_rss")

# Konversi ke JSON lokal untuk dashboard
result.toPandas().to_json("dashboard/data/spark_results.json", orient="records")
```

---

### Komponen 4 — Dashboard: Serving Layer

**Tujuan:** Tampilan web yang menggabungkan output Spark (historis) dengan data terbaru dari consumer (live).

**Yang dibangun:**
- `dashboard/app.py` — Flask web app di `localhost:5000`
- 3 panel utama di halaman index
- Auto-refresh setiap 30–60 detik

**Panel Dashboard:**

| Panel | Sumber Data | Konten |
|-------|-------------|--------|
| **Panel 1** | `spark_results.json` | Hasil analisis Spark: statistik, volatilitas per jam |
| **Panel 2** | `live_api.json` | Tabel harga live BTC/ETH/BNB + % perubahan (merah/hijau) |
| **Panel 3** | `live_rss.json` | 5 berita terbaru dari CoinDesk |

**Endpoint Flask:**
```python
@app.route('/api/data')
def get_data():
    return jsonify({
        "spark": load_json("spark_results.json"),
        "live":  load_json("live_api.json"),
        "news":  load_json("live_rss.json")
    })
```

**Auto-refresh JavaScript:**
```javascript
setInterval(() => {
  fetch('/api/data')
    .then(res => res.json())
    .then(data => updateDashboard(data));
}, 30000); // setiap 30 detik
```

**Hint teknis:**
- `pip install flask`
- Consumer (Komponen 2) wajib menulis salinan lokal ke `dashboard/data/` agar dashboard bisa membaca

---

## 📂 Struktur Direktori Proyek

```
cryptowatch/
├── docker-compose.yml        ← Kafka + Hadoop setup
├── producer_api.py           ← Producer CoinGecko API
├── producer_rss.py           ← Producer RSS CoinDesk
├── consumer_to_hdfs.py       ← Consumer → HDFS + local copy
├── spark/
│   └── analysis.py           ← Spark processing & analisis
└── dashboard/
    ├── app.py                ← Flask web app
    ├── templates/
    │   └── index.html        ← Halaman utama dashboard
    └── data/
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```

---

## ▶️ Cara Menjalankan

```bash
# 1. Jalankan infrastruktur
docker-compose up -d

# 2. Buat direktori HDFS
hdfs dfs -mkdir -p /data/crypto/api /data/crypto/rss /data/crypto/hasil

# 3. Jalankan producers (terminal terpisah)
python producer_api.py
python producer_rss.py

# 4. Jalankan consumer
python consumer_to_hdfs.py

# 5. Jalankan analisis Spark (setelah data terkumpul)
spark-submit spark/analysis.py

# 6. Jalankan dashboard
cd dashboard && python app.py
# Buka: http://localhost:5000
```

---

## 📦 Dependencies

```bash
pip install kafka-python feedparser hdfs flask
```

---

## 📝 Catatan

- Hormati rate limit CoinGecko: maks 30 request/menit untuk tier gratis
- Pastikan consumer sudah berjalan cukup lama sebelum menjalankan Spark agar data cukup untuk analisis
- Jika CoinDesk RSS tidak dapat diakses, gunakan backup: `https://cointelegraph.com/rss`
- Analisis Spark tidak perlu membaca langsung dari Kafka — semua data sudah di HDFS

# 🪙 CryptoWatch — Panduan Setup Infrastruktur
**Dari nol sampai semua container running**

> Tested on: WSL Ubuntu 24.04, Docker 27.5.1, Docker Compose v2.40.3

---

## Prasyarat

Pastikan sudah terinstall:
- Docker Desktop (dengan WSL integration aktif)
- Python 3.x + pip

---

## STEP 1 — Buat Struktur Folder Project

```bash
mkdir -p ~/cryptowatch/{spark,dashboard/{templates,data}}
cd ~/cryptowatch
touch producer_api.py producer_rss.py consumer_to_hdfs.py
touch spark/analysis.py
touch dashboard/app.py dashboard/templates/index.html
touch docker-compose.yml .env
ls -R
```

---

## STEP 2 — Buat `docker-compose.yml`

```bash
cat > ~/cryptowatch/docker-compose.yml << 'EOF'
networks:
  crypto-net:
    driver: bridge

services:
  # ─── ZOOKEEPER ───────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    networks: [crypto-net]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  # ─── KAFKA ───────────────────────────────────────────
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    networks: [crypto-net]
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  # ─── HADOOP NAMENODE ─────────────────────────────────
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    networks: [crypto-net]
    ports:
      - "9870:9870"
      - "8020:8020"
    volumes:
      - namenode_data:/hadoop/dfs/name
    environment:
      - CLUSTER_NAME=cryptowatch
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false

  # ─── HADOOP DATANODE ─────────────────────────────────
  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    networks: [crypto-net]
    depends_on: [namenode]
    volumes:
      - datanode_data:/hadoop/dfs/data
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
      - HDFS_CONF_dfs_replication=1

  # ─── SPARK ───────────────────────────────────────────
  spark:
    image: apache/spark:3.5.3
    container_name: spark
    networks: [crypto-net]
    depends_on: [namenode]
    ports:
      - "8080:8080"
      - "4040:4040"
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
    volumes:
      - ./spark:/opt/spark-apps
      - ./dashboard/data:/opt/dashboard-data

volumes:
  namenode_data:
  datanode_data:
EOF
echo "✅ docker-compose.yml selesai"
```

---

## STEP 3 — Install Python Dependencies

```bash
pip3 install kafka-python feedparser hdfs flask requests --break-system-packages
```

---

## STEP 4 — Pull Image Spark (lakukan sebelum compose up)

```bash
docker pull apache/spark:3.5.3
```

> ⏳ Proses pull semua image membutuhkan waktu 2–5 menit tergantung koneksi internet.

---

## STEP 5 — Jalankan Semua Container

```bash
cd ~/cryptowatch
docker compose up -d
```

Tunggu sekitar 40 detik, lalu cek status:

```bash
sleep 40
docker compose ps
```

**Output yang diharapkan:**

```
NAME        IMAGE                                             STATUS
datanode    bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8  Up (healthy)
kafka       confluentinc/cp-kafka:7.5.0                      Up
namenode    bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8  Up (healthy)
spark       apache/spark:3.5.3                               Up
zookeeper   confluentinc/cp-zookeeper:7.5.0                  Up
```

Semua 5 container harus berstatus **Up** ✅

---

## Catatan Penting

| Hal | Keterangan |
|-----|------------|
| Image Spark | Gunakan `apache/spark:3.5.3` — `bitnami/spark` sudah dihapus dari Docker Hub |
| Image Hadoop | Gunakan `bde2020/hadoop-namenode/datanode` — sudah include auto-format HDFS |
| File `.env` | **Tidak diperlukan** dengan image bde2020 — konfigurasi lewat environment di compose |
| Attribute `version` | Tidak perlu ditulis di docker-compose.yml (obsolete di Compose v2) |

---

## Troubleshooting

**Container namenode/datanode tidak muncul di `docker compose ps`:**
```bash
# Cek log error
docker compose logs namenode --tail=30
```

**Ingin reset total dan mulai ulang:**
```bash
docker compose down -v   # hapus container + volume
docker compose up -d     # start ulang dari awal
```

**Cek apakah Kafka sudah siap menerima koneksi:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

lanjutan 

# Buat direktori HDFS
docker exec namenode hdfs dfs -mkdir -p /data/crypto/api
docker exec namenode hdfs dfs -mkdir -p /data/crypto/rss
docker exec namenode hdfs dfs -mkdir -p /data/crypto/hasil
docker exec namenode hdfs dfs -chmod -R 777 /data/crypto
docker exec namenode hdfs dfs -ls /data/crypto

# Buat Kafka topics
docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic crypto-api \
  --partitions 1 \
  --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic crypto-rss \
  --partitions 1 \
  --replication-factor 1

docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

Anggota 2:

Berikut adalah **panduan langkah demi langkah** untuk **Anggota 2** yang bertanggung jawab mengembangkan `producer_api.py` — yaitu producer yang mengambil data harga real-time dari CoinGecko API dan mengirimkannya ke topic Kafka `crypto-api`.

---

## 📌 Tugas Anggota 2
✅ Membuat script Python `producer_api.py` yang:
- Melakukan polling ke CoinGecko API setiap **60 detik** (atau interval yang sudah ditentukan)
- Mengirim data harga BTC, ETH, BNB (USD & IDR, serta persen perubahan 24 jam) ke topic Kafka `crypto-api`
- Menggunakan library `kafka-python` dengan konfigurasi `enable_idempotence=True` dan `acks="all"`
- Menghormati **rate limit** API (maks 30 request/menit) – cukup dengan interval 60 detik sudah aman
- Menyusun JSON sesuai spesifikasi

---

## 🧰 Prasyarat (Harus sudah siap dari anggota 1)
Sebelum menjalankan script producer, pastikan infrastruktur berikut sudah **running** dan **siap**:
- Kafka broker tersedia di `localhost:9092`
- Topic `crypto-api` sudah dibuat (oleh anggota 1)
- Semua container Docker (zookeeper, kafka, namenode, datanode) berstatus `Up`

Cek dengan:
```bash
docker compose ps
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
# harus muncul "crypto-api"
```

---

## 📁 Langkah 1 – Siapkan environment Python

```bash
cd ~/cryptowatch   # masuk ke folder proyek
python3 -m venv venv
source venv/bin/activate
pip install kafka-python requests
```

> ⚠️ Jangan gunakan `--break-system-packages` jika pakai venv. Selalu pakai virtual environment.

---

## 📁 Langkah 2 – Buat file `producer_api.py`

Gunakan editor (nano, vim, atau VS Code):

```bash
nano producer_api.py
```

Salin kode di bawah ini:

```python
#!/usr/bin/env python3
# producer_api.py
# Mengambil data harga crypto dari CoinGecko dan mengirim ke Kafka topic 'crypto-api'

import json
import time
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# ---------- KONFIGURASI ----------
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'crypto-api'
POLLING_INTERVAL_SEC = 60   # 60 detik (rate limit aman)

# CoinGecko endpoint & parameter
URL = "https://api.coingecko.com/api/v3/simple/price"
PARAMS = {
    "ids": "bitcoin,ethereum,binancecoin",   # BTC, ETH, BNB
    "vs_currencies": "usd,idr",
    "include_24hr_change": "true"
}

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------- FUNCTION ----------
def create_kafka_producer():
    """Membuat KafkaProducer dengan pengaturan idempotent dan acks=all"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',                     # tunggu semua replica
            enable_idempotence=True,        # mencegah duplikasi
            retries=3
        )
        logger.info(f"KafkaProducer connected to {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"Gagal membuat KafkaProducer: {e}")
        raise

def fetch_crypto_prices():
    """Memanggil CoinGecko API dan mengembalikan list of dict per koin"""
    try:
        response = requests.get(URL, params=PARAMS, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Raw API response: {data}")

        # Mapping symbol dari API key ke symbol yang diminta
        # API mengembalikan: "bitcoin" -> {"usd":..., "idr":..., "usd_24h_change":...}
        mapping = {
            "bitcoin": "BTC",
            "ethereum": "ETH",
            "binancecoin": "BNB"
        }

        results = []
        now_iso = datetime.now(timezone.utc).isoformat()  # timestamp UTC

        for api_id, symbol in mapping.items():
            if api_id not in data:
                logger.warning(f"Data untuk {api_id} tidak ditemukan di response")
                continue

            coin_data = data[api_id]
            # Pastikan field yang diperlukan ada
            price_usd = coin_data.get("usd")
            price_idr = coin_data.get("idr")
            change_24h = coin_data.get("usd_24h_change")

            if price_usd is None or price_idr is None or change_24h is None:
                logger.warning(f"Field tidak lengkap untuk {symbol}: {coin_data}")
                continue

            record = {
                "symbol": symbol,
                "price_usd": float(price_usd),
                "price_idr": float(price_idr),
                "change_24h": float(change_24h),
                "timestamp": now_iso
            }
            results.append(record)

        return results
    except requests.exceptions.RequestException as e:
        logger.error(f"Error saat mengambil data dari CoinGecko: {e}")
        return []
    except Exception as e:
        logger.error(f"Error tidak terduga: {e}")
        return []

def send_to_kafka(producer, records):
    """Kirim setiap record ke Kafka topic dengan key = symbol"""
    for rec in records:
        key = rec["symbol"].encode('utf-8')
        try:
            future = producer.send(TOPIC, key=key, value=rec)
            # Blokir hingga terkirim (opsional, untuk memastikan)
            record_metadata = future.get(timeout=5)
            logger.info(f"Sent: {rec['symbol']} USD={rec['price_usd']} | offset={record_metadata.offset}")
        except Exception as e:
            logger.error(f"Gagal mengirim data {rec['symbol']}: {e}")

# ---------- MAIN LOOP ----------
def main():
    producer = create_kafka_producer()
    logger.info(f"Memulai producer API, akan mengirim ke topic '{TOPIC}' setiap {POLLING_INTERVAL_SEC} detik")
    try:
        while True:
            records = fetch_crypto_prices()
            if records:
                send_to_kafka(producer, records)
            else:
                logger.warning("Tidak ada data yang berhasil diambil dari API")
            logger.info(f"Menunggu {POLLING_INTERVAL_SEC} detik sebelum polling berikutnya...")
            time.sleep(POLLING_INTERVAL_SEC)
    except KeyboardInterrupt:
        logger.info("Producer dihentikan oleh user")
    finally:
        producer.close()
        logger.info("KafkaProducer ditutup")

if __name__ == "__main__":
    main()
```

> ✅ Simpan file (Ctrl+O, Enter, lalu Ctrl+X jika pakai nano).

---

## 📁 Langkah 3 – Uji coba producer secara manual

### 3.1 Cek topic Kafka sudah ada
```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092
# harus keluar "crypto-api"
```

### 3.2 Jalankan consumer console (untuk melihat pesan yang masuk)
Buka **terminal terpisah**:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic crypto-api --from-beginning
```

### 3.3 Jalankan producer
Di terminal utama (dengan venv aktif):
```bash
cd ~/cryptowatch
source venv/bin/activate
python producer_api.py
```

Setelah beberapa detik, Anda akan melihat log seperti:
```
INFO - Sent: BTC USD=...
INFO - Sent: ETH USD=...
INFO - Sent: BNB USD=...
```
Dan di terminal consumer akan muncul JSON seperti:
```json
{"symbol": "BTC", "price_usd": 65432.1, "price_idr": 1050000000, "change_24h": 2.35, "timestamp": "2026-04-27T10:15:30Z"}
```

**Tekan Ctrl+C** pada producer untuk menghentikan sementara (nanti akan dijalankan terus bersama anggota 3).

---

## 📁 Langkah 4 – Pastikan data sesuai spesifikasi

Periksa struktur JSON:
- ✅ `symbol` (string) : BTC, ETH, BNB
- ✅ `price_usd` (float)
- ✅ `price_idr` (float)
- ✅ `change_24h` (float) – bisa positif atau negatif
- ✅ `timestamp` (ISO format UTC) contoh: `2026-04-27T10:15:30.123456+00:00`

Jika ada perbedaan (misal `change_24h` None), perbaiki logika fetch.

---

## 📁 Langkah 5 – Integrasi dengan anggota lain

Setelah producer berjalan stabil:
- **Anggota 3** akan membuat `consumer_to_hdfs.py` yang membaca dari topic `crypto-api` (dan `crypto-rss`) lalu menyimpan ke HDFS.
- Anda tidak perlu mengubah apapun, cukup pastikan producer terus berjalan (bisa di background atau menggunakan `nohup`).

Untuk menjalankan producer dalam background:
```bash
nohup python producer_api.py > producer_api.log 2>&1 &
```

Untuk mematikannya lagi:
```bash
pkill -f producer_api.py
```

---

## 🧪 Langkah 6 – Debugging & Tips

| Masalah | Solusi |
|---------|--------|
| `NoBrokersAvailable` | Kafka belum siap. Pastikan container Kafka sudah running (`docker compose ps`), tunggu 20 detik lagi. |
| `throttling` dari API | Jangan polling kurang dari 60 detik. Rate limit CoinGecko free tier: 30 request/menit. |
| Field `usd_24h_change` tidak ada | Kadang API mengembalikan `usd_24h_change` hanya kalau diminta dengan `include_24hr_change=true` – sudah diatur. |
| Harga IDR terlalu besar | Gunakan float, tidak masalah. Nanti di dashboard bisa diformat dengan `.2f` atau ribuan separator. |
| Timezone `timestamp` | Gunakan UTC (zulu time) seperti di spesifikasi. Kode sudah menggunakan `datetime.now(timezone.utc)`. |

---

## 📤 Output yang diharapkan dari Anggota 2

Pada akhir tugas, Anggota 2 harus menyerahkan/mendemonstrasikan:
1. File `producer_api.py` yang sudah lengkap (seperti kode di atas)
2. Bukti bahwa producer berhasil mengirim data ke Kafka (bisa screenshot terminal consumer yang menampilkan JSON)
3. Tidak ada error rate limit, dan interval polling 60 detik berjalan konsisten
4. File tersebut siap dijalankan bersama anggota 3 dan anggota 4 nantinya

---

Dengan panduan ini, Anda (Anggota 2) bisa langsung mengimplementasikan `producer_api.py` tanpa perlu mengatur infrastruktur (sudah disiapkan anggota 1). Selamat mengerjakan! 🚀

Anggota 3:

#### LANJUTAN ANGGOTA 3

### Bikin producer_rss.py

cat > ~/cryptowatch/producer_rss.py << 'EOF'
import json
import time
import hashlib
import feedparser
from datetime import datetime, timezone
from kafka import KafkaProducer

# Config 
KAFKA_BROKER = "localhost:9092"
TOPIC        = "crypto-rss"
RSS_URL      = "https://www.coindesk.com/arc/outboundfeeds/rss/"
RSS_BACKUP   = "https://cointelegraph.com/rss"
INTERVAL     = 300  # 5 menit

# Init Producer 
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    enable_idempotence=True,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

# State: simpan ID yang sudah dikirim
sent_ids = set()

def make_article_id(url: str) -> str:
    """Hash 8 karakter dari URL sebagai unique ID artikel."""
    return hashlib.md5(url.encode()).hexdigest()[:8]

def fetch_and_send():
    feed = feedparser.parse(RSS_URL)

    # Fallback ke backup RSS kalau gagal
    if feed.bozo or len(feed.entries) == 0:
        print(f"⚠️  CoinDesk RSS gagal, coba backup...")
        feed = feedparser.parse(RSS_BACKUP)

    sent_count = 0
    for entry in feed.entries:
        article_id = make_article_id(entry.get("link", ""))

        # Skip kalau sudah pernah dikirim
        if article_id in sent_ids:
            continue

        payload = {
            "article_id": article_id,
            "title":      entry.get("title", ""),
            "link":       entry.get("link", ""),
            "summary":    entry.get("summary", "")[:500],  # batasi panjang
            "published":  entry.get("published", ""),
            "timestamp":  datetime.now(timezone.utc).isoformat(),
        }

        producer.send(TOPIC, key=article_id, value=payload)
        sent_ids.add(article_id)
        sent_count += 1
        print(f"  ✅ Sent: [{article_id}] {payload['title'][:60]}...")

    producer.flush()
    print(f"📰 Batch selesai — {sent_count} artikel baru dikirim | total sent: {len(sent_ids)}")

# Main Loop
if __name__ == "__main__":
    print(f"🚀 producer_rss.py started → topic: {TOPIC}")
    while True:
        try:
            fetch_and_send()
        except Exception as e:
            print(f"❌ Error: {e}")
        print(f"⏳ Tunggu {INTERVAL//60} menit...\n")
        time.sleep(INTERVAL)
EOF

### Bikin consumer_to_hdfs.py

cat > ~/cryptowatch/consumer_to_hdfs.py << 'EOF'

import json
import subprocess
import threading
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer

# Config 
KAFKA_BROKER   = "localhost:9092"
TOPIC_API      = "crypto-api"
TOPIC_RSS      = "crypto-rss"
FLUSH_INTERVAL = 180  
LOCAL_TMP_DIR  = "/tmp/cryptowatch"
HDFS_API_PATH  = "/data/crypto/api"
HDFS_RSS_PATH  = "/data/crypto/rss"

# Local copy untuk dashboard
DASHBOARD_DIR  = "./dashboard/data"
LIVE_API_FILE  = f"{DASHBOARD_DIR}/live_api.json"
LIVE_RSS_FILE  = f"{DASHBOARD_DIR}/live_rss.json"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
os.makedirs(DASHBOARD_DIR, exist_ok=True)

# Shared Buffer (thread-safe pakai lock) 
buffer_api = []
buffer_rss = []
lock = threading.Lock()

# Upload ke HDFS via subprocess 
def upload_to_hdfs(local_path: str, hdfs_path: str):
    filename = os.path.basename(local_path)
    subprocess.run(
        ["docker", "cp", local_path, f"namenode:/tmp/{filename}"],
        check=True
    )
    subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f",
         f"/tmp/{filename}", hdfs_path],
        check=True
    )
    print(f"  ☁️  Uploaded → {hdfs_path}/{filename}")

# Flush buffer ke HDFS + local copy
def flush_buffers():
    while True:
        threading.Event().wait(FLUSH_INTERVAL)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")

        with lock:
            # ── Flush API buffer ──
            if buffer_api:
                filename = f"api_{timestamp}.json"
                local_path = f"{LOCAL_TMP_DIR}/{filename}"
                with open(local_path, "w") as f:
                    json.dump(buffer_api, f, indent=2)
                # Simpan local copy untuk dashboard
                with open(LIVE_API_FILE, "w") as f:
                    json.dump(buffer_api[-50:], f, indent=2)  # 50 event terbaru
                try:
                    upload_to_hdfs(local_path, HDFS_API_PATH)
                except Exception as e:
                    print(f"❌ HDFS upload API gagal: {e}")
                print(f"💾 API flush: {len(buffer_api)} events → {filename}")
                buffer_api.clear()

            # ── Flush RSS buffer ──
            if buffer_rss:
                filename = f"rss_{timestamp}.json"
                local_path = f"{LOCAL_TMP_DIR}/{filename}"
                with open(local_path, "w") as f:
                    json.dump(buffer_rss, f, indent=2)
                # Simpan local copy untuk dashboard
                with open(LIVE_RSS_FILE, "w") as f:
                    json.dump(buffer_rss[-20:], f, indent=2)  # 20 artikel terbaru
                try:
                    upload_to_hdfs(local_path, HDFS_RSS_PATH)
                except Exception as e:
                    print(f"❌ HDFS upload RSS gagal: {e}")
                print(f"📰 RSS flush: {len(buffer_rss)} events → {filename}")
                buffer_rss.clear()

# Consumer thread: API topic
def consume_api():
    consumer = KafkaConsumer(
        TOPIC_API,
        bootstrap_servers=KAFKA_BROKER,
        group_id="cryptowatch-consumer-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"🎧 Listening on topic: {TOPIC_API}")
    for msg in consumer:
        with lock:
            buffer_api.append(msg.value)
        print(f"  📥 API: {msg.value.get('symbol')} | ${msg.value.get('price_usd')}")

# Consumer thread: RSS topic
def consume_rss():
    consumer = KafkaConsumer(
        TOPIC_RSS,
        bootstrap_servers=KAFKA_BROKER,
        group_id="cryptowatch-consumer-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"🎧 Listening on topic: {TOPIC_RSS}")
    for msg in consumer:
        with lock:
            buffer_rss.append(msg.value)
        print(f"  📰 RSS: {msg.value.get('title', '')[:60]}...")

# Main 
if __name__ == "__main__":
    print("🚀 consumer_to_hdfs.py started")
    print(f"   Flush interval: setiap {FLUSH_INTERVAL//60} menit")

    threads = [
        threading.Thread(target=consume_api,   daemon=True),
        threading.Thread(target=consume_rss,   daemon=True),
        threading.Thread(target=flush_buffers, daemon=True),
    ]
    for t in threads:
        t.start()

    # Keep main thread hidup
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n👋 Consumer dihentikan.")
EOF

### Jalanin producer_rss.py
# input
python producer_rss.py atau python3 producer_rss.py

# output
🚀 producer_rss.py started → topic: crypto-rss
  ✅ Sent: [a1b2c3d4] Bitcoin Hits New High...
  ✅ Sent: [e5f6g7h8] Ethereum Update...
📰 Batch selesai — 15 artikel baru dikirim | total sent: 15
⏳ Tunggu 5 menit...

### Verifikasi masuk ke kafka (lewat terminal 2, yang sebelumn nya biarin jalan)
# input
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic crypto-rss \
  --from-beginning \
  --max-messages 3

# output
{"article_id": "67cbd2ed", "title": "Signal in the age of infinite noise", "link": "https://www.coindesk.com/opinion/2026/04/27/signal-in-the-age-of-infinite-noise", "summary": "", "published": "Mon, 27 Apr 2026 13:45:02 +0000", "timestamp": "2026-04-27T14:01:45.922180+00:00"}
{"article_id": "5eb812c0", "title": "Michael Saylor\u2019s Strategy buys 3,273 bitcoin as it inches closer to its 1 million target", "link": "https://www.coindesk.com/markets/2026/04/27/michael-saylor-s-strategy-buys-3-273-bitcoin-as-it-inches-closer-to-its-1-million-target", "summary": "", "published": "Mon, 27 Apr 2026 13:43:09 +0000", "timestamp": "2026-04-27T14:01:45.928667+00:00"}
{"article_id": "c445287a", "title": "Bitmine buys $236 million in ether as Tom Lee touts ETH as 'wartime store of value'", "link": "https://www.coindesk.com/business/2026/04/27/bitmine-buys-usd236-million-in-ether-as-tom-lee-touts-eth-as-wartime-store-of-value", "summary": "", "published": "Mon, 27 Apr 2026 13:17:02 +0000", "timestamp": "2026-04-27T14:01:45.928923+00:00"}
Processed a total of 3 messages

### jalanin consumer_to_hdfs.py (lewat terminal 3 atau terminal 2 bekas verifikasi gpp, tapi verifikasi di terminal baru)
# input 
python consumer_to_hdfs.py atau python3 consumer_to_hdfs.py

# output
🚀 consumer_to_hdfs.py started
   Flush interval: setiap 3 menit
🎧 Listening on topic: crypto-api
🎧 Listening on topic: crypto-rss
  📰 RSS: Bitcoin Hits New High...

# nanti muncul ini kalau berhasil:
Successfully copied 9.94kB (transferred 11.8kB) to namenode:/tmp/rss_2026-04-27_14-09.json
2026-04-27 14:09:30,652 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
  ☁️  Uploaded → /data/crypto/rss/rss_2026-04-27_14-09.json
📰 RSS flush: 25 events → rss_2026-04-27_14-09.json

### verifikasi
# Tunggu 3 menit sampai flush terjadi lalu verifikasi di HDFS:
# Cek file masuk ke HDFS
docker exec namenode hdfs dfs -ls /data/crypto/rss/

# Lihat isi filenya
docker exec namenode hdfs dfs -cat /data/crypto/rss/rss_2026-04-27_14-09.json

# Dan cek local copy untuk dashboard:
cat dashboard/data/live_rss.json

**Anggota 4:**

Langkah 1: Buat file spark/analysis.py
```bash
cd ~/cryptowatch
```
```bash
nano spark/analysis.py
```

Kemudian copy-paste kode berikut:
```bash
#!/usr/bin/env python3
"""
CryptoWatch - Spark Analysis
Anggota 4: Processing Layer (3 Analisis Wajib)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, avg, max, min, stddev, abs, count
import json
import os

# ==================== KONFIGURASI ====================
HDFS_BASE = "hdfs://namenode:8020"
API_PATH = f"{HDFS_BASE}/data/crypto/api/"
RSS_PATH = f"{HDFS_BASE}/data/crypto/rss/"
OUTPUT_HDFS = f"{HDFS_BASE}/data/crypto/hasil/spark_results"
DASHBOARD_JSON = "dashboard/data/spark_results.json"

# ==================== SPARK SESSION ====================
spark = SparkSession.builder \
    .appName("CryptoWatch-Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session berhasil dibuat")

# ==================== LOAD DATA ====================
print("📥 Membaca data dari HDFS...")

# Baca semua file JSON di folder api dan rss (multiLine karena array of objects)
df_api = spark.read.option("multiLine", True).json(API_PATH)
df_rss = spark.read.option("multiLine", True).json(RSS_PATH)

print(f"   API records : {df_api.count()}")
print(f"   RSS records : {df_rss.count()}")

# Buat temporary views
df_api.createOrReplaceTempView("crypto_api")
df_rss.createOrReplaceTempView("crypto_rss")

# ==================== ANALISIS 1: Statistik Harga per Koin ====================
print("📊 Menjalankan Analisis 1: Statistik Harga per Koin...")

stat_harga = spark.sql("""
    SELECT 
        symbol,
        COUNT(*) as jumlah_record,
        ROUND(AVG(price_usd), 2) as rata_rata_usd,
        ROUND(MAX(price_usd), 2) as tertinggi_usd,
        ROUND(MIN(price_usd), 2) as terendah_usd,
        ROUND(STDDEV(price_usd), 2) as std_deviasi_usd,
        ROUND(AVG(change_24h), 2) as rata_rata_change_24h
    FROM crypto_api
    GROUP BY symbol
    ORDER BY symbol
""")

stat_harga.show()

# ==================== ANALISIS 2: Volatilitas per Jam ====================
print("📊 Menjalankan Analisis 2: Volatilitas per Jam...")

volatilitas = spark.sql("""
    SELECT 
        HOUR(TO_TIMESTAMP(timestamp)) AS jam,
        ROUND(AVG(ABS(change_24h)), 4) as avg_volatilitas,
        COUNT(*) as jumlah_data
    FROM crypto_api
    WHERE change_24h IS NOT NULL
    GROUP BY HOUR(TO_TIMESTAMP(timestamp))
    ORDER BY avg_volatilitas DESC
""")

volatilitas.show()

# ==================== ANALISIS 3: Volume Berita per Jam ====================
print("📊 Menjalankan Analisis 3: Volume Berita per Jam...")

berita_per_jam = spark.sql("""
    SELECT 
        HOUR(TO_TIMESTAMP(timestamp)) AS jam,
        COUNT(*) as jumlah_artikel
    FROM crypto_rss
    WHERE timestamp IS NOT NULL
    GROUP BY HOUR(TO_TIMESTAMP(timestamp))
    ORDER BY jumlah_artikel DESC
""")

berita_per_jam.show()

# ==================== SIMPAN HASIL KE JSON ====================
print("💾 Menyimpan hasil analisis...")

# Gabungkan semua hasil menjadi satu dictionary
results = {
    "statistik_harga": [row.asDict() for row in stat_harga.collect()],
    "volatilitas_per_jam": [row.asDict() for row in volatilitas.collect()],
    "berita_per_jam": [row.asDict() for row in berita_per_jam.collect()],
    "metadata": {
        "total_api_records": df_api.count(),
        "total_rss_records": df_rss.count(),
        "last_updated": "2026-05-04"  # bisa diganti otomatis nanti
    }
}

# Simpan ke dashboard (untuk Flask)
os.makedirs("dashboard/data", exist_ok=True)
with open(DASHBOARD_JSON, "w") as f:
    json.dump(results, f, indent=2)

print(f"✅ spark_results.json berhasil disimpan ke {DASHBOARD_JSON}")

# Simpan juga ke HDFS (opsional)
stat_harga.write.mode("overwrite").json(f"{OUTPUT_HDFS}/stat_harga")
volatilitas.write.mode("overwrite").json(f"{OUTPUT_HDFS}/volatilitas")
berita_per_jam.write.mode("overwrite").json(f"{OUTPUT_HDFS}/berita")

print("🎉 Semua analisis selesai!")
spark.stop()
```

Langkah 2: Jalankan Analisis Spark
```bash
cd ~/cryptowatch
```
```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark-apps/analysis.py
```

Error handling sebelum langkah 3
```bash
docker exec spark find / -name "spark_results.json" 2>/dev/null
```
```bash
docker cp spark:/opt/spark/work-dir/dashboard/data/spark_results.json ~/cryptowatch/dashboard/data/
```

Langkah 3: Verifikasi Hasil
```bash
#Cek file JSON di dashboard
cat ~/cryptowatch/dashboard/data/spark_results.json | head -n 50
```
```bash
#Cek di HDFS
docker exec namenode hdfs dfs -ls /data/crypto/hasil/
```

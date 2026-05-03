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

# Log Pipeline Project

Bu proje, parquet dosyalarından log verilerini okuyup MySQL veritabanına işleyen, veri kalitesi kontrolü yapan ve test edilmiş bir ETL pipeline'ı içerir.

## 📁 Proje Yapısı

```
raw_logs/
├── log_pipeline.py         # Ana pipeline kodu
├── unit_test.ipynb        # Birim testler
├── qualty_test.ipynb      # Veri kalitesi testleri
├── oop_result.ipynb       # Pipeline kullanım örnekleri
└── README.md              # Bu dosya
``` 

## 🚀 Özellikler

- **Parquet Dosya Okuma**: Büyük dosyalar için batch işleme desteği
- **Veri Doğrulama**: Pydantic ile tip güvenli validasyon
- **MySQL Upsert**: Duplicate key handling ile güvenli veri yazma
- **Veri Kalitesi Kontrolü**: Null değer, duplicate, freshness kontrolleri
- **Birim Testler**: %100 test coverage
- **Chunk İşleme**: Büyük dosyalar için bellek optimizasyonu

## 📋 Gereksinimler

```python
pandas>=1.5.0
pydantic>=2.0.0
sqlalchemy>=2.0.0
pymysql>=1.0.0
numpy>=1.24.0
pathlib
```

## 🗄️ Veritabanı Tabloları

Pipeline 5 farklı tablo oluşturur:

- **users**: Kullanıcı bilgileri
- **sessions**: Oturum verileri  
- **events**: Olay logları
- **hotels**: Otel bilgileri
- **payments**: Ödeme durumları

## 🔧 Kurulum

1. Gerekli kütüphaneleri yükleyin:
```bash
pip install pandas pydantic sqlalchemy pymysql numpy
```

2. MySQL veritabanı yapılandırması için `config.json` dosyası oluşturun:
```json
{
    "kullanici": "your_username",
    "sifre": "your_password", 
    "host": "localhost",
    "port": 3306,
    "veritabani": "your_database"
}
```

## 📖 Kullanım

### Basit Kullanım

```python
from pathlib import Path
import json
from sqlalchemy import create_engine
from log_pipeline import LogPipeline

# Veritabanı bağlantısı
cfg = json.loads(Path("config.json").read_text())
engine = create_engine(f"mysql+pymysql://{cfg['kullanici']}:{cfg['sifre']}@{cfg['host']}:{cfg['port']}/{cfg['veritabani']}")

# Pipeline çalıştır
pipeline = LogPipeline("data.parquet.gzip", engine)
pipeline.run_once()
```

### Batch İşleme (Büyük Dosyalar İçin)

```python
pipeline = LogPipeline("large_data.parquet.gzip", engine)

for batch in pipeline.reader.read_in_batches(batch_size=10000):
    batch.dropna(subset=["user_id", "session_id", "hotel_id", "request_id", "funnel_id"], inplace=True)
    users, sessions, events, hotels, payments = pipeline.validator.validate(batch)
    
    # Timestamp ekle
    now = pipeline.now
    for tbl in (users, sessions, events, hotels, payments):
        if not tbl.empty:
            tbl["updated_Date"] = now
    
    # Veritabanına yaz
    pipeline.writer.upsert_df(users, "users")
    pipeline.writer.upsert_df(sessions, "sessions") 
    pipeline.writer.upsert_df(events, "events")
    pipeline.writer.upsert_df(hotels, "hotels")
    pipeline.writer.upsert_df(payments, "payments")
```

### Veri Kalitesi Kontrolü İle

```python
from qualty_test import LogPipelineWithQuality

pipeline = LogPipelineWithQuality("data.parquet.gzip", engine)
quality_passed = pipeline.run_once()

if not quality_passed:
    print("Veri kalitesi sorunları tespit edildi!")
```

## 🧪 Test Çalıştırma

### Birim Testler

```python
# unit_test.ipynb dosyasını çalıştırın
import unittest
unittest.main(argv=[''], exit=False, verbosity=2)
```

Test edilen bileşenler:
- ✅ ParquetReader (okuma ve batch işleme)
- ✅ ValidatorProcessor (veri doğrulama)
- ✅ UpsertWriter (veritabanı yazma)
- ✅ LogPipeline (end-to-end pipeline)

### Veri Kalitesi Testleri

```python
from qualty_test import DataQualityChecker

quality_checker = DataQualityChecker(engine)
quality_passed = quality_checker.run_all_checks()
```

Kontrol edilen kalite kriterleri:
- 🔍 Null değer kontrolü
- 🔍 Duplicate kayıt kontrolü  
- 🔍 Veri güncellik kontrolü
- 🔍 Schema validasyonu
- 🔍 Veri aralık kontrolü

## 📊 Veri Modelleri

### UserModel
```python
- user_id: float (zorunlu)
- subscriber_id: float (opsiyonel)
- country: str (opsiyonel)
- has_email_contact_permission: bool (opsiyonel)
- has_phone_contact_permission: bool (opsiyonel)
```

### EventModel
```python
- request_id: str (zorunlu)
- session_id: str (zorunlu)
- funnel_id: str (zorunlu)
- hotel_id: int (zorunlu)
- timestamp: datetime (zorunlu)
- page_name: str (opsiyonel)
- search_query: str (opsiyonel)
- destination_id: float (opsiyonel)
- num_guests: float (opsiyonel)
```

### HotelModel
```python
- hotel_id: int (zorunlu)
- hotel_price: float (opsiyonel)
- currency: str (opsiyonel)
```

### PaymentModel
```python
- request_id: str (zorunlu)
- payment_status: str (opsiyonel) # "pending", "success", "failed"
- confirmation_number: str (opsiyonel)
```

### SessionModel
```python
- session_id: str (zorunlu)
- user_id: float (opsiyonel)
- user_agent: str (opsiyonel)
- device_type: str (opsiyonel)
- ip_address: str (opsiyonel)
- utm_source: str (opsiyonel)
```

## ⚙️ Yapılandırma

### Boolean Değer Dönüşümü
```python
true_set = {"yes", "true", "1", "y", "evet"}
false_set = {"no", "false", "0", "n", "hayir", "hayır"}
```

### Desteklenen Tarih Formatları
```python
formats = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S", 
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S"
]
```

### Payment Status Mapping
```python
status = {
    "done": "success",
    "ok": "success", 
    "paid": "success",
    "fail": "failed",
    "error": "failed"
}
```

## 🔒 Primary Key Yapılandırması

```python
TABLE_PK_MAP = {
    "users": ["user_id"],
    "sessions": ["user_id", "session_id"],
    "events": ["request_id", "session_id", "hotel_id", "funnel_id"],
    "hotels": ["hotel_id"],
    "payments": ["request_id"]
}
```

## 🐛 Hata Yönetimi

- **Validation Errors**: Geçersiz veriler loglanır, pipeline devam eder
- **Database Errors**: Transaction rollback ile güvenli hata yönetimi
- **Schema Errors**: Missing column kontrolleri
- **Type Conversion Errors**: Graceful fallback mekanizmaları

## 📈 Performans

- **Chunk Size**: 1000 kayıt (ayarlanabilir)
- **Batch Processing**: Bellek optimizasyonu için büyük dosyalarda kullanın
- **Upsert Logic**: Efficient MySQL ON DUPLICATE KEY UPDATE
- **Connection Pooling**: SQLAlchemy engine ile otomatik yönetim
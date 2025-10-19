import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Generator, List, Dict
import pandas as pd
from pydantic import BaseModel, field_validator
from sqlalchemy import create_engine, text

# ...existing code...
# Sabitler / yardımcı setler
true_set = {"yes", "true", "1", "y", "evet"}
false_set = {"no", "false", "0", "n", "hayir", "hayır"}

formats = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
]

status = {
    "done": "success",
    "ok": "success",
    "paid": "success",
    "fail": "failed",
    "error": "failed",
}

TABLE_PK_MAP = {
    "users": ["user_id"],
    "sessions": ["user_id", "session_id"],
    "events": ["request_id", "session_id", "hotel_id", "funnel_id"],
    "hotels": ["hotel_id"],
    "payments": ["request_id"],
}

# Pydantic modelleri (orijinal davranış korunuyor)
class UserModel(BaseModel):
    user_id: Optional[float]
    subscriber_id: Optional[float]
    country: Optional[str]
    has_email_contact_permission: Optional[bool]
    has_phone_contact_permission: Optional[bool]

    @field_validator("has_email_contact_permission", "has_phone_contact_permission", mode="before")
    def to_bool(cls, v):
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            try:
                return bool(int(v))
            except Exception:
                raise ValueError(f"Boolean parse failed for numeric value: {v}")
        if isinstance(v, str):
            s = v.strip().lower()
            if s in true_set:
                return True
            if s in false_set:
                return False
        raise ValueError(f"Cannot parse boolean value: {v}")

class EventModel(BaseModel):
    request_id: str
    session_id: str
    funnel_id: str
    hotel_id: int
    timestamp: datetime
    page_name: Optional[str]
    search_query: Optional[str]
    destination_id: Optional[float]
    num_guests: Optional[float]

    @field_validator("timestamp", mode="before")
    def parse_timestamp(cls, v):
        if isinstance(v, datetime):
            return v
        if v is None:
            raise ValueError("timestamp boş olamaz")
        if isinstance(v, str):
            s = v.strip()
            for fmt in formats:
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            raise ValueError("timestamp geçersiz")
        raise ValueError(f"Unsupported timestamp type: {type(v)}")

class HotelModel(BaseModel):
    hotel_id: int
    hotel_price: Optional[float]
    currency: Optional[str]

    @field_validator("hotel_price", mode="before")
    def clean_price(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            v = v.replace(",", ".").replace("$", "").strip()
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

class PaymentModel(BaseModel):
    request_id: str
    payment_status: Optional[str]
    confirmation_number: Optional[str]

    @field_validator("payment_status", mode="before")
    def normalize_status(cls, v):
        if v is None:
            return None
        if not isinstance(v, str):
            v = str(v)
        v = v.strip().lower()
        return status.get(v, v)

    @field_validator("payment_status", mode="after")
    def check_valid_values(cls, v):
        allowed = {"pending", "success", "failed"}
        if v is None:
            return None
        if v not in allowed:
            raise ValueError(f"Invalid payment status: {v}")
        return v

class SessionModel(BaseModel):
    session_id: str
    user_id: Optional[float]
    user_agent: Optional[str]
    device_type: Optional[str]
    ip_address: Optional[str]
    utm_source: Optional[str]

    @field_validator("session_id", mode="before")
    def session_id_not_empty(cls, v):
        if v is None:
            raise ValueError("session_id boş olamaz")
        if isinstance(v, str):
            v = v.strip()
            if not v:
                raise ValueError("session_id boş olamaz")
            return v
        return str(v)

# Sorumluluklar: okuma, doğrulama, yazma
class ParquetReader:
    def __init__(self, path: str):
        self.path = path

    def read_all(self) -> pd.DataFrame:
        # Not: pandas read_parquet için chunk desteği yok; büyük veriler için pyarrow stream veya partition önerilir.
        return pd.read_parquet(self.path)

    def read_in_batches(self, batch_size: int) -> Generator[pd.DataFrame, None, None]:
        df = self.read_all()
        for i in range(0, len(df), batch_size):
            yield df.iloc[i : i + batch_size].copy()

class ValidatorProcessor:
    def __init__(self):
        pass

    def validate(self, df: pd.DataFrame):
        users, sessions, events, hotels, payments = [], [], [], [], []
        for row in df.to_dict(orient="records"):
            try:
                users.append(UserModel(**row).model_dump(exclude_none=True))
                sessions.append(SessionModel(**row).model_dump(exclude_none=True))
                events.append(EventModel(**row).model_dump(exclude_none=True))
                hotels.append(HotelModel(**row).model_dump(exclude_none=True))
                payments.append(PaymentModel(**row).model_dump(exclude_none=True))
            except Exception as e:
                # konsola yazdır; istenirse logging ile değiştirilebilir
                print(f"Validation error: {e}")
        return (
            pd.DataFrame(users).drop_duplicates(subset=["user_id"]) if users else pd.DataFrame(),
            pd.DataFrame(sessions).drop_duplicates(subset=["session_id"]) if sessions else pd.DataFrame(),
            pd.DataFrame(events) if events else pd.DataFrame(),
            pd.DataFrame(hotels).drop_duplicates(subset=["hotel_id"]) if hotels else pd.DataFrame(),
            pd.DataFrame(payments) if payments else pd.DataFrame(),
        )

class UpsertWriter:
    def __init__(self, engine):
        self.engine = engine

    def upsert_df(self, df: pd.DataFrame, table_name: str, chunk_size: int = 1000):
        if df is None or df.empty:
            return
        pk_cols = TABLE_PK_MAP.get(table_name, [])
        cols = list(df.columns)
        cols_quoted = ", ".join(f"`{c}`" for c in cols)
        vals_placeholders = ", ".join(":" + c for c in cols)

        update_parts = []
        for c in cols:
            if c in pk_cols:
                continue
            if c == "updated_Date":
                update_parts.append(f"`{c}` = GREATEST(`{c}`, VALUES(`{c}`))")
            else:
                update_parts.append(
                    f"`{c}` = IF(VALUES(`updated_Date`) > `updated_Date` AND NOT (VALUES(`{c}`) <=> `{c}`), VALUES(`{c}`), `{c}`)"
                )

        update_clause = ", ".join(update_parts) if update_parts else "/* no-update */"
        sql = f"INSERT INTO `{table_name}` ({cols_quoted}) VALUES ({vals_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

        records = df.to_dict(orient="records")
        with self.engine.begin() as conn:
            for i in range(0, len(records), chunk_size):
                chunk = records[i : i + chunk_size]
                conn.execute(text(sql), chunk)

# Basit pipeline wrapper
class LogPipeline:
    def __init__(self, parquet_path: str, engine):
        self.reader = ParquetReader(parquet_path)
        self.validator = ValidatorProcessor()
        self.writer = UpsertWriter(engine)
        self.now = datetime.now()

    def run_once(self):
        df = self.reader.read_all()
        # önce gerekli zorunlu sütunları temizle (orijinal davranış)
        df.dropna(subset=["user_id", "session_id", "hotel_id", "request_id", "funnel_id"], inplace=True)
        users, sessions, events, hotels, payments = self.validator.validate(df)

        # updated_Date ekle
        for tbl in (users, sessions, events, hotels, payments):
            if not getattr(tbl, "empty", True):
                tbl["updated_Date"] = self.now

        # yaz
        self.writer.upsert_df(users, "users")
        self.writer.upsert_df(sessions, "sessions")
        self.writer.upsert_df(events, "events")
        self.writer.upsert_df(hotels, "hotels")
        self.writer.upsert_df(payments, "payments")
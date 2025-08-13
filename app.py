from __future__ import annotations

from fastapi import FastAPI, Depends, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional, Protocol
from base64 import b64decode, b64encode
from datetime import datetime, timezone
from pathlib import Path as SysPath
import hashlib, os, re, requests

API_TOKENS = {"hello1234"}  # this is the token needed in every transaction with the API

# Local filesystem backend dir
LOCAL_STORAGE_DIR = SysPath("./storage")


DATABASE_URL = "sqlite:///./app.db"


S3_BUCKET   = "expiriment"                 
S3_REGION   = "eu-north-1"                 
S3_ENDPOINT = f"https://s3.eu-north-1.amazonaws.com"




#Authrization 
def require_bearer(authorization: Optional[str] = Header(None)) -> None:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing/invalid Authorization header")
    token = authorization.split(" ", 1)[1]
    if token not in API_TOKENS:
        raise HTTPException(status_code=403, detail="Invalid token")

#Classes
class BlobIn(BaseModel):
    id: str = Field(..., description="Unique identifier for the blob (string/path ok)")
    data: str = Field(..., description="Base64-encoded bytes")

class BlobOut(BaseModel):
    id: str
    data: str
    size: int
    created_at: str

#db setup
from sqlalchemy import (
    create_engine, Column, String, Integer, DateTime, LargeBinary, UniqueConstraint
)
from sqlalchemy.orm import sessionmaker, declarative_base, Session

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class BlobMeta(Base):
    __tablename__ = "blob_meta"
    id = Column(String, primary_key=True)
    backend = Column(String, nullable=False)             # "local" OR "db" OR "s3"
    locator = Column(String, nullable=False)             # where in backend
    size = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    __table_args__ = (UniqueConstraint("backend", "id", name="uq_backend_id"),)

class BlobData(Base):
    __tablename__ = "blob_data"  # used only by DB backend to store bytes
    id = Column(String, primary_key=True)
    data = Column(LargeBinary, nullable=False)

Base.metadata.create_all(engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Helpers ----------
SAFE_SEG = re.compile(r"^[A-Za-z0-9._-]+$")
def safe_relpath(key: str) -> SysPath:
    parts = []
    for seg in key.strip("/").split("/"):
        seg = seg.strip()
        if not seg or seg == "." or seg == "..":
            continue
        parts.append(seg if SAFE_SEG.match(seg) else hashlib.sha256(seg.encode()).hexdigest()[:16])
    return SysPath(*parts) if parts else SysPath("blob")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Storage Protocol 
class Storage(Protocol):
    def put(self, key: str, data: bytes) -> str: ...
    def get(self, locator: str) -> bytes: ...

# Local FS 
class LocalFS(Storage):
    def __init__(self, base: SysPath):
        self.base = base
        self.base.mkdir(parents=True, exist_ok=True)
    def put(self, key: str, data: bytes) -> str:
        rel = safe_relpath(key)
        path = self.base / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".part")
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)  # atomic
        return str(rel).replace("\\", "/")
    def get(self, locator: str) -> bytes:
        rel = safe_relpath(locator)
        path = self.base / rel
        if not path.exists():
            raise FileNotFoundError(locator)
        return path.read_bytes()

LOCAL = LocalFS(LOCAL_STORAGE_DIR)

# Database
class DBTable(Storage):
    def __init__(self, db_factory):
        self.db_factory = db_factory
    def put(self, key: str, data: bytes) -> str:
        with self.db_factory() as db:
            row = db.get(BlobData, key)
            if row:
                db.delete(row); db.flush()
            db.add(BlobData(id=key, data=data))
            db.commit()
        return key
    def get(self, locator: str) -> bytes:
        with self.db_factory() as db:
            row = db.get(BlobData, locator)
            if not row:
                raise FileNotFoundError(locator)
            return bytes(row.data)

DBBACK = DBTable(SessionLocal)

# S3
class S3(Storage):
    def __init__(self, endpoint: str, region: str, bucket: str):
        self.endpoint = endpoint.rstrip("/")
        self.region = region
        self.bucket = bucket
        self.session = requests.Session()

    def _url(self, object_key: str) -> str:
        host = f"{self.bucket}.s3.{self.region}.amazonaws.com"
        return f"https://{host}/{object_key}"

    def put(self, key: str, data: bytes) -> str:
        object_key = "/".join(safe_relpath(key).parts)
        url = self._url(object_key)
        r = self.session.put(url, data=data)  
        if r.status_code not in (200, 201):
            raise RuntimeError(f"S3 PUT failed: {r.status_code} {r.text}")
        return object_key

    def get(self, locator: str) -> bytes:
        object_key = "/".join(safe_relpath(locator).parts)
        url = self._url(object_key)
        r = self.session.get(url)  
        if r.status_code != 200:
            raise FileNotFoundError(f"S3 GET failed: {r.status_code} {r.text}")
        return r.content

S3BACK = S3(S3_ENDPOINT, S3_REGION, S3_BUCKET)

# FastAPI 
app = FastAPI(title="Simple Blob Store (local, db, s3 unsigned)")

# Local
@app.post("/v1/local/blobs", response_model=BlobOut)
def store_local(body: BlobIn, _=Depends(require_bearer), db: Session = Depends(get_db)):
    try:
        raw = b64decode(body.data, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Base64 'data'")
    exists = db.query(BlobMeta).filter_by(backend="local", id=body.id).one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="id already exists for this backend")
    try:
        locator = LOCAL.put(body.id, raw)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Backend error: {e}")
    meta = BlobMeta(id=body.id, backend="local", locator=locator, size=len(raw), created_at=datetime.now(timezone.utc))
    db.add(meta); db.commit()
    return BlobOut(id=body.id, data=body.data, size=len(raw), created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

@app.get("/v1/local/blobs/{id:path}", response_model=BlobOut)
def get_local(id: str, _=Depends(require_bearer), db: Session = Depends(get_db)):
    meta = db.query(BlobMeta).filter_by(backend="local", id=id).one_or_none()
    if not meta:
        raise HTTPException(status_code=404, detail="Unknown id")
    try:
        raw = LOCAL.get(meta.locator)
    except FileNotFoundError:
        raise HTTPException(status_code=502, detail="Object missing in backend")
    return BlobOut(id=id, data=b64encode(raw).decode(), size=meta.size, created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

# DB
@app.post("/v1/db/blobs", response_model=BlobOut)
def store_db(body: BlobIn, _=Depends(require_bearer), db: Session = Depends(get_db)):
    try:
        raw = b64decode(body.data, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Base64 'data'")
    exists = db.query(BlobMeta).filter_by(backend="db", id=body.id).one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="id already exists for this backend")
    try:
        locator = DBBACK.put(body.id, raw)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Backend error: {e}")
    meta = BlobMeta(id=body.id, backend="db", locator=locator, size=len(raw), created_at=datetime.now(timezone.utc))
    db.add(meta); db.commit()
    return BlobOut(id=body.id, data=body.data, size=len(raw), created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

@app.get("/v1/db/blobs/{id:path}", response_model=BlobOut)
def get_db_blob(id: str, _=Depends(require_bearer), db: Session = Depends(get_db)):
    meta = db.query(BlobMeta).filter_by(backend="db", id=id).one_or_none()
    if not meta:
        raise HTTPException(status_code=404, detail="Unknown id")
    try:
        raw = DBBACK.get(meta.locator)
    except FileNotFoundError:
        raise HTTPException(status_code=502, detail="Object missing in backend")
    return BlobOut(id=id, data=b64encode(raw).decode(), size=meta.size, created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

# S3
@app.post("/v1/s3/blobs", response_model=BlobOut)
def store_s3(body: BlobIn, _=Depends(require_bearer), db: Session = Depends(get_db)):
    try:
        raw = b64decode(body.data, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Base64 'data'")
    exists = db.query(BlobMeta).filter_by(backend="s3", id=body.id).one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="id already exists for this backend")
    try:
        locator = S3BACK.put(body.id, raw)  
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"S3 error: {e}")
    meta = BlobMeta(id=body.id, backend="s3", locator=locator, size=len(raw), created_at=datetime.now(timezone.utc))
    db.add(meta); db.commit()
    return BlobOut(id=body.id, data=body.data, size=len(raw), created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

@app.get("/v1/s3/blobs/{id:path}", response_model=BlobOut)
def get_s3_blob(id: str, _=Depends(require_bearer), db: Session = Depends(get_db)):
    meta = db.query(BlobMeta).filter_by(backend="s3", id=id).one_or_none()
    if not meta:
        raise HTTPException(status_code=404, detail="Unknown id")
    try:
        raw = S3BACK.get(meta.locator) 
    except FileNotFoundError:
        raise HTTPException(status_code=502, detail="Object missing in backend")
    return BlobOut(id=id, data=b64encode(raw).decode(), size=meta.size, created_at=meta.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"))

@app.get("/health")
def health():
    return {"ok": True, "backends": ["local", "db", "s3_unsigned"], "bucket": S3_BUCKET}
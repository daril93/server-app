from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
import httpx
import io
import csv
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid
from datetime import datetime, date, timezone, timedelta

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

MONGO_URL = "mongodb+srv://daril93_db_user:cwjIKbptMnvPCIR7y@cluster0.eqsbuvj.mongodb.net/"
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Push notifications
PUSH_BASE_URL = "https://integrations.emergentagent.com"
PUSH_KEY = os.environ.get("EMERGENT_PUSH_KEY", "placeholder")
_push_client = httpx.AsyncClient(
    base_url=PUSH_BASE_URL,
    headers={"X-Push-Key": PUSH_KEY},
    timeout=10.0,
)

# The single "user" of this local shop app (no auth) - fixed id
SHOP_USER_ID = "shop-owner"

app = FastAPI()
api_router = APIRouter(prefix="/api")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ---------- Models ----------
class Product(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    description: str
    quantity: int
    expiration_date: str  # ISO date string YYYY-MM-DD
    lot_number: str
    supplier: str
    warning_days: Optional[int] = None  # override global setting
    barcode: Optional[str] = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    notified_warning: bool = False
    notified_expired: bool = False


class ProductCreate(BaseModel):
    description: str
    quantity: int
    expiration_date: str
    lot_number: str
    supplier: str
    warning_days: Optional[int] = None
    barcode: Optional[str] = None


class ProductUpdate(BaseModel):
    description: Optional[str] = None
    quantity: Optional[int] = None
    expiration_date: Optional[str] = None
    lot_number: Optional[str] = None
    supplier: Optional[str] = None
    warning_days: Optional[int] = None
    barcode: Optional[str] = None


class Settings(BaseModel):
    default_warning_days: int = 7
    push_enabled: bool = True


class RegisterPushBody(BaseModel):
    user_id: str
    platform: str
    device_token: str


# ---------- Helpers ----------
async def send_push(recipients: List[str], data: dict, idempotency_key: Optional[str] = None) -> None:
    if not recipients:
        return
    if "title" not in data or "message" not in data:
        raise ValueError("data must include title and message")
    payload = {"recipients": recipients, "data": data}
    if idempotency_key:
        payload["$idempotency_key"] = idempotency_key
    resp = await _push_client.post("/api/v1/push/trigger", json=payload)
    if resp.status_code == 401:
        raise HTTPException(500, "EMERGENT_PUSH_KEY missing or invalid")
    if resp.status_code >= 500:
        raise HTTPException(502, "Push provider unavailable")
    resp.raise_for_status()


def compute_status(product: dict, default_warning_days: int) -> str:
    warning_days = product.get("warning_days") or default_warning_days
    try:
        exp = datetime.strptime(product["expiration_date"], "%Y-%m-%d").date()
    except Exception:
        return "ok"
    today = date.today()
    if exp < today:
        return "expired"
    if (exp - today).days <= warning_days:
        return "warning"
    return "ok"


async def get_settings_doc() -> dict:
    doc = await db.settings.find_one({"_id": "singleton"}, {"_id": 0})
    if not doc:
        default = Settings().model_dump()
        await db.settings.update_one({"_id": "singleton"}, {"$set": default}, upsert=True)
        return default
    return doc


# ---------- Routes ----------
@api_router.get("/")
async def root():
    return {"message": "ScadenzaPro API"}


@api_router.post("/products", response_model=Product)
async def create_product(body: ProductCreate):
    product = Product(**body.model_dump())
    doc = product.model_dump()
    await db.products.insert_one(doc.copy())
    return product


@api_router.get("/products")
async def list_products():
    settings = await get_settings_doc()
    default_warning = settings.get("default_warning_days", 7)
    products = await db.products.find({}, {"_id": 0}).to_list(2000)
    # Sort by expiration date ascending
    for p in products:
        p["status"] = compute_status(p, default_warning)
    products.sort(key=lambda p: p.get("expiration_date", "9999-12-31"))
    return products


@api_router.get("/products/stats")
async def product_stats():
    settings = await get_settings_doc()
    default_warning = settings.get("default_warning_days", 7)
    products = await db.products.find({}, {"_id": 0}).to_list(5000)
    total = len(products)
    total_qty = sum(int(p.get("quantity") or 0) for p in products)
    warning = 0
    expired = 0
    ok = 0
    for p in products:
        s = compute_status(p, default_warning)
        if s == "warning":
            warning += 1
        elif s == "expired":
            expired += 1
        else:
            ok += 1
    # top 3 expiring soon (not expired)
    upcoming = [p for p in products if compute_status(p, default_warning) in ("warning", "ok")]
    upcoming.sort(key=lambda p: p.get("expiration_date", "9999-12-31"))
    for p in upcoming[:5]:
        p["status"] = compute_status(p, default_warning)
    return {
        "total": total,
        "total_quantity": total_qty,
        "warning": warning,
        "expired": expired,
        "ok": ok,
        "upcoming": upcoming[:5],
    }


@api_router.get("/products/{product_id}", response_model=Product)
async def get_product(product_id: str):
    doc = await db.products.find_one({"id": product_id}, {"_id": 0})
    if not doc:
        raise HTTPException(404, "Product not found")
    return Product(**doc)


@api_router.put("/products/{product_id}", response_model=Product)
async def update_product(product_id: str, body: ProductUpdate):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if "expiration_date" in updates:
        # reset notification flags if expiration changed
        updates["notified_warning"] = False
        updates["notified_expired"] = False
    result = await db.products.update_one({"id": product_id}, {"$set": updates})
    if result.matched_count == 0:
        raise HTTPException(404, "Product not found")
    doc = await db.products.find_one({"id": product_id}, {"_id": 0})
    return Product(**doc)


@api_router.delete("/products/{product_id}")
async def delete_product(product_id: str):
    result = await db.products.delete_one({"id": product_id})
    if result.deleted_count == 0:
        raise HTTPException(404, "Product not found")
    return {"status": "deleted"}


@api_router.get("/settings", response_model=Settings)
async def get_settings():
    doc = await get_settings_doc()
    return Settings(**doc)


@api_router.put("/settings", response_model=Settings)
async def update_settings(body: Settings):
    await db.settings.update_one({"_id": "singleton"}, {"$set": body.model_dump()}, upsert=True)
    return body


@api_router.post("/register-push", status_code=201)
async def register_push(body: RegisterPushBody):
    try:
        resp = await _push_client.post("/api/v1/push/users/register", json=body.model_dump())
        if resp.status_code == 401:
            raise HTTPException(500, "EMERGENT_PUSH_KEY missing or invalid")
        if resp.status_code >= 500:
            raise HTTPException(502, "Push provider unavailable")
        resp.raise_for_status()
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"register_push failed (non-blocking): {e}")
    # Also track locally for our own reference
    await db.devices.update_one(
        {"device_token": body.device_token},
        {"$set": body.model_dump()},
        upsert=True,
    )
    return {"status": "registered"}


@api_router.post("/check-expirations")
async def check_expirations():
    """Iterates all products and sends push for newly-entered warning or expired states."""
    settings = await get_settings_doc()
    if not settings.get("push_enabled", True):
        return {"sent_warning": 0, "sent_expired": 0, "note": "push disabled"}
    default_warning = settings.get("default_warning_days", 7)
    products = await db.products.find({}, {"_id": 0}).to_list(5000)
    sent_warning = 0
    sent_expired = 0
    today = date.today()
    for p in products:
        status = compute_status(p, default_warning)
        if status == "expired" and not p.get("notified_expired"):
            try:
                await send_push(
                    recipients=[SHOP_USER_ID],
                    data={
                        "title": "Prodotto scaduto",
                        "message": f"{p['description']} (Lotto {p['lot_number']}) è scaduto.",
                        "action_url": "/(tabs)/products",
                    },
                    idempotency_key=f"expired-{p['id']}-{today.isoformat()}",
                )
                sent_expired += 1
            except Exception as e:
                logger.warning(f"push expired failed (non-blocking): {e}")
            await db.products.update_one(
                {"id": p["id"]},
                {"$set": {"notified_expired": True, "notified_warning": True}},
            )
        elif status == "warning" and not p.get("notified_warning"):
            try:
                warning_days = p.get("warning_days") or default_warning
                exp = datetime.strptime(p["expiration_date"], "%Y-%m-%d").date()
                days_left = (exp - today).days
                await send_push(
                    recipients=[SHOP_USER_ID],
                    data={
                        "title": "Prodotto in scadenza",
                        "message": f"{p['description']} scade tra {days_left} giorni (Lotto {p['lot_number']}).",
                        "action_url": "/(tabs)/products",
                    },
                    idempotency_key=f"warning-{p['id']}-{today.isoformat()}",
                )
                sent_warning += 1
            except Exception as e:
                logger.warning(f"push warning failed (non-blocking): {e}")
            await db.products.update_one(
                {"id": p["id"]},
                {"$set": {"notified_warning": True}},
            )
    return {"sent_warning": sent_warning, "sent_expired": sent_expired}


@api_router.get("/products/export/csv")
async def export_csv():
    products = await db.products.find({}, {"_id": 0}).to_list(5000)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Descrizione", "Quantità", "Data Scadenza", "Numero Lotto", "Fornitore", "Giorni Preavviso", "Barcode"])
    for p in products:
        writer.writerow([
            p.get("description", ""),
            p.get("quantity", ""),
            p.get("expiration_date", ""),
            p.get("lot_number", ""),
            p.get("supplier", ""),
            p.get("warning_days", ""),
            p.get("barcode", ""),
        ])
    return {"csv": output.getvalue(), "count": len(products)}


app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
    await _push_client.aclose()

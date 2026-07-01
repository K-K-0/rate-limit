import time
import uuid
import base64
import json
from threading import Lock
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Assigned values ----
TOTAL_ORDERS = 42
RATE_LIMIT = 17          # requests
RATE_WINDOW_S = 10       # seconds

# ---- Fixed catalog: orders 1..T ----
CATALOG = {
    i: {"id": i, "item": f"item-{i}", "amount": round(10 + i * 1.5, 2)}
    for i in range(1, TOTAL_ORDERS + 1)
}

# ---- Idempotency store: key -> order dict ----
idempotency_store = {}
idempotency_lock = Lock()

# ---- Rate limiter: client_id -> list of request timestamps ----
rate_buckets = {}
rate_lock = Lock()


def encode_cursor(next_id: int) -> str:
    raw = json.dumps({"next_id": next_id}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        return int(data["next_id"])
    except Exception:
        return 1  # default to start if cursor is invalid/missing


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Apply rate limiting to all endpoints based on X-Client-Id
    client_id = request.headers.get("X-Client-Id")

    if client_id:
        now = time.time()
        with rate_lock:
            bucket = rate_buckets.setdefault(client_id, [])
            # Drop timestamps outside the window
            window_start = now - RATE_WINDOW_S
            bucket[:] = [t for t in bucket if t > window_start]

            if len(bucket) >= RATE_LIMIT:
                # Compute Retry-After based on oldest request in window
                oldest = min(bucket)
                retry_after = max(1, int(RATE_WINDOW_S - (now - oldest)) + 1)
                return JSONResponse(
                    status_code=429,
                    content={"error": "rate limit exceeded"},
                    headers={"Retry-After": str(retry_after)},
                )

            bucket.append(now)

    response = await call_next(request)
    return response


@app.post("/orders")
async def create_order(request: Request, idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key")):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")

    with idempotency_lock:
        if idempotency_key in idempotency_store:
            existing = idempotency_store[idempotency_key]
            return JSONResponse(status_code=200, content=existing)

        try:
            body = await request.json()
        except Exception:
            body = {}

        new_order = {
            "id": str(uuid.uuid4()),
            "item": body.get("item", "unspecified"),
            "amount": body.get("amount", 0),
            "created_at": time.time(),
        }
        idempotency_store[idempotency_key] = new_order

    return JSONResponse(status_code=201, content=new_order)


@app.get("/orders")
async def list_orders(limit: int = 10, cursor: Optional[str] = None):
    limit = max(1, limit)
    start_id = decode_cursor(cursor) if cursor else 1

    if start_id > TOTAL_ORDERS:
        return {"items": [], "next_cursor": None, "next": None, "orders": []}

    end_id = min(start_id + limit - 1, TOTAL_ORDERS)
    items = [CATALOG[i] for i in range(start_id, end_id + 1)]

    next_id = end_id + 1
    next_cursor = encode_cursor(next_id) if next_id <= TOTAL_ORDERS else None

    return {
        "items": items,
        "next_cursor": next_cursor,
        "next": next_cursor,
        "orders": items,
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
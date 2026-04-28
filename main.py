import asyncio
import os
import traceback
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from sessions_store import storage_backend, db_health_check
import telegram_client as tg

SERVICE_API_KEY = os.environ.get("SERVICE_API_KEY", "")
SERVICE_VERSION = "2026-04-28"

REQUIRED_ENV = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "SERVICE_API_KEY",
    "SUPABASE_DB_URL",
]


def _missing_env() -> list[str]:
    return [name for name in REQUIRED_ENV if not os.environ.get(name)]


def _check_auth(x_service_auth: Optional[str]) -> None:
    if not SERVICE_API_KEY:
        raise HTTPException(500, "SERVICE_API_KEY non configuré")
    if not x_service_auth or x_service_auth != SERVICE_API_KEY:
        raise HTTPException(401, "Unauthorized")


async def _keep_alive():
    while True:
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[startup] version={SERVICE_VERSION}")

    missing = _missing_env()
    if missing:
        print(f"[startup] variables manquantes: {missing}")

    db = db_health_check()
    print(f"[startup] db_health: {db}")

    if not missing and db.get("ok"):
        try:
            print("[startup] restauration sessions...")
            await tg.restore_all_sessions()
            asyncio.create_task(_keep_alive())
            print(f"[startup] clients actifs: {len(tg._clients)}")
        except Exception as exc:
            print(f"[startup] erreur restore: {exc}")
    else:
        print("[startup] restauration ignorée")

    yield

    print("[shutdown] fermeture service")


app = FastAPI(title="Telethon Service", lifespan=lifespan)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print(f"[error] {request.method} {request.url.path}: {exc}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
    )


# ---------- Health ----------

@app.get("/health")
async def health():
    missing = _missing_env()
    db = db_health_check()
    backend = storage_backend()
    return {
        "ok": (not missing) and db.get("ok", False),
        "version": SERVICE_VERSION,
        "active_clients": len(tg._clients),
        "session_storage": backend,
        "database_ok": db.get("ok", False),
        "missing_env": missing,
    }


@app.get("/diagnostics")
async def diagnostics(x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return {
        "ok": True,
        "active_clients": list(tg._clients.keys()),
        "persistence": tg.get_persist_diagnostics(),
        "database": db_health_check(),
    }


@app.post("/admin/reload")
async def admin_reload(x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    missing = _missing_env()
    if missing:
        raise HTTPException(500, f"Variables manquantes: {missing}")
    db = db_health_check()
    if not db.get("ok"):
        raise HTTPException(500, f"DB indisponible: {db.get('error')}")
    tg.reset_persist_diagnostics()
    await tg.restore_all_sessions()
    return {
        "ok": True,
        "version": SERVICE_VERSION,
        "active_clients": list(tg._clients.keys()),
    }


# ---------- Login QR ----------

class StartQRBody(BaseModel):
    owner_id: str
    account_id: str


@app.post("/accounts/login/qr/start")
async def qr_start(body: StartQRBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.start_qr_login(owner_id=body.owner_id, account_id=body.account_id)


class CheckQRBody(BaseModel):
    account_id: str


@app.post("/accounts/login/qr/check")
async def qr_check(body: CheckQRBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.check_qr_login(account_id=body.account_id)


class TwoFABody(BaseModel):
    account_id: str
    password: str


@app.post("/accounts/login/2fa")
async def login_2fa(body: TwoFABody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.submit_2fa_password(account_id=body.account_id, password=body.password)


# ---------- Login par numéro de téléphone ----------

class StartPhoneBody(BaseModel):
    owner_id: str
    account_id: str
    phone: str


@app.post("/accounts/login/phone/send-code")
async def phone_send_code(body: StartPhoneBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.send_phone_code(
        owner_id=body.owner_id,
        account_id=body.account_id,
        phone=body.phone,
    )


@app.post("/accounts/login/phone/start")
async def phone_start(body: StartPhoneBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.send_phone_code(
        owner_id=body.owner_id,
        account_id=body.account_id,
        phone=body.phone,
    )


class PhoneCodeBody(BaseModel):
    account_id: str
    code: str
    phone_code_hash: Optional[str] = None


@app.post("/accounts/login/phone/verify-code")
async def phone_verify_code(body: PhoneCodeBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.verify_phone_code(
        account_id=body.account_id,
        code=body.code,
        phone_code_hash=body.phone_code_hash,
    )


@app.post("/accounts/login/phone/code")
async def phone_code(body: PhoneCodeBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.verify_phone_code(
        account_id=body.account_id,
        code=body.code,
        phone_code_hash=body.phone_code_hash,
    )


# ---------- Opérations ----------

class DisconnectBody(BaseModel):
    account_id: str


@app.post("/accounts/disconnect")
async def disconnect(body: DisconnectBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    await tg.disconnect_account(body.account_id)
    return {"ok": True}


class SendBody(BaseModel):
    account_id: str
    telegram_chat_id: int
    body: str


@app.post("/messages/send")
async def send(body: SendBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.send_message(
        account_id=body.account_id,
        telegram_chat_id=body.telegram_chat_id,
        body=body.body,
    )


class SyncBody(BaseModel):
    account_id: str
    max_chats: int = 50
    max_messages_per_chat: int = 200


@app.post("/sync/history")
async def sync_history(body: SyncBody, x_service_auth: Optional[str] = Header(None)):
    _check_auth(x_service_auth)
    return await tg.sync_history(
        account_id=body.account_id,
        max_chats=body.max_chats,
        max_messages_per_chat=body.max_messages_per_chat,
    )

"""
API HTTP du service Telethon.

Sécurité : toutes les routes (sauf /health) exigent le header
    X-Service-Auth: <SERVICE_API_KEY>
qui doit correspondre à la variable d'env SERVICE_API_KEY.

C'est l'app Lovable qui appelle ces routes (jamais le navigateur de
l'utilisateur final directement).
"""
import asyncio
import os
import traceback
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import telegram_client as tg

SERVICE_API_KEY = os.environ.get("SERVICE_API_KEY", "")


def _check_auth(x_service_auth: Optional[str]) -> None:
    if not SERVICE_API_KEY:
        raise HTTPException(500, "SERVICE_API_KEY non configuré côté service")
    if not x_service_auth or x_service_auth != SERVICE_API_KEY:
        raise HTTPException(401, "Unauthorized")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[startup] restauration des sessions Telethon")
    try:
        await tg.restore_all_sessions()
    except Exception as exc:
        print(f"[startup] erreur restore: {exc}")
    yield
    print("[shutdown] déconnexion des clients")


app = FastAPI(title="Opsis Telethon Service", lifespan=lifespan)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Évite les 500 nus : renvoie le détail de l'erreur en JSON pour le frontend."""
    tb = traceback.format_exc()
    print(f"[unhandled] {request.method} {request.url.path}: {exc}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
    )


@app.get("/health")
async def health():
    return {"ok": True, "service": "telethon", "active_clients": len(tg._clients)}


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


# Alias rétro-compatible
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


# Alias rétro-compatible
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
    # Lance la sync en tâche de fond, retourne immédiatement
    asyncio.create_task(
        tg.sync_history(
            account_id=body.account_id,
            max_chats=body.max_chats,
            max_messages_per_chat=body.max_messages_per_chat,
        )
    )
    return {"ok": True, "started": True}

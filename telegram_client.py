"""
Gestion multi-comptes Telethon.

Chaque compte Telegram (= 1 ligne dans `telegram_accounts` côté Lovable) a son
propre TelegramClient stocké en mémoire. Les sessions sont sérialisées en
StringSession et persistées dans Supabase pour survivre aux redémarrages.

Login par QR code :
    1. POST /accounts/login/qr/start  -> renvoie qr_url
    2. L'utilisateur scanne avec Telegram → Paramètres → Appareils → Lier un appareil
    3. POST /accounts/login/qr/check  -> waiting / 2fa_required / success
    4. Si 2FA : POST /accounts/login/2fa { password }
"""
import asyncio
import os
from typing import Dict, Optional

from telethon import TelegramClient, events
from telethon.errors import (
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import User

from sessions_store import delete_session, list_all_sessions, save_session
from webhook import post_to_lovable

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")

_clients: Dict[str, TelegramClient] = {}
_owners: Dict[str, str] = {}
_pending_qr: Dict[str, dict] = {}  # { account_id: { "qr": QRLogin, "client": TelegramClient } }
_pending_2fa: Dict[str, TelegramClient] = {}
_pending_phone: Dict[str, dict] = {}


def _new_client(session_string: Optional[str] = None) -> TelegramClient:
    if not API_ID or not API_HASH:
        raise RuntimeError("TELEGRAM_API_ID et TELEGRAM_API_HASH doivent être configurés")
    session = StringSession(session_string) if session_string else StringSession()
    return TelegramClient(session, API_ID, API_HASH, device_model="Opsis CRM", system_version="1.0")


def _attach_handlers(client: TelegramClient, owner_id: str, account_id: str) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def on_incoming(event):
        await _forward_message(event, owner_id, account_id, direction="in")

    @client.on(events.NewMessage(outgoing=True))
    async def on_outgoing(event):
        await _forward_message(event, owner_id, account_id, direction="out")


async def _forward_message(event, owner_id: str, account_id: str, direction: str) -> None:
    try:
        msg = event.message
        chat = await event.get_chat()
        if not isinstance(chat, User):
            return

        sender = await event.get_sender() if direction == "in" else chat
        full_name = " ".join(filter(None, [getattr(sender, "first_name", None), getattr(sender, "last_name", None)])).strip() or (getattr(sender, "username", None) or "Inconnu")
        username = getattr(sender, "username", None)
        handle = f"@{username}" if username else f"id:{sender.id}"
        initials = "".join([p[0] for p in full_name.split()[:2]]).upper() or full_name[:2].upper()

        payload = {
            "owner_id": owner_id,
            "account_id": account_id,
            "telegram_chat_id": chat.id,
            "telegram_access_hash": str(getattr(chat, "access_hash", "") or ""),
            "telegram_message_id": msg.id,
            "direction": direction,
            "body": msg.message or "",
            "sent_at": msg.date.isoformat() if msg.date else None,
            "contact": {
                "name": full_name,
                "handle": handle,
                "initials": initials,
                "telegram_user_id": chat.id,
            },
        }
        await post_to_lovable("incoming", payload)
    except Exception as exc:
        print(f"[telethon] erreur forward {account_id}: {exc}")


# ---------- Lifecycle ----------

async def restore_all_sessions() -> None:
    sessions = list_all_sessions()
    print(f"[telethon] restauration de {len(sessions)} session(s)")
    for s in sessions:
        try:
            await _connect_with_session(
                owner_id=s["owner_id"],
                account_id=s["account_id"],
                session_string=s["session_string"],
            )
        except Exception as exc:
            print(f"[telethon] échec restore {s['account_id']}: {exc}")


async def _connect_with_session(*, owner_id: str, account_id: str, session_string: str) -> TelegramClient:
    client = _new_client(session_string)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError(f"Session expirée pour {account_id}")
    _attach_handlers(client, owner_id, account_id)
    _clients[account_id] = client
    _owners[account_id] = owner_id
    print(f"[telethon] compte {account_id} connecté")
    return client


# ---------- Login QR ----------

async def start_qr_login(*, owner_id: str, account_id: str) -> dict:
    if account_id in _clients:
        return {"already_connected": True}

    # Nettoie un éventuel pending précédent
    old = _pending_qr.pop(account_id, None)
    if old and isinstance(old, dict) and old.get("client"):
        try:
            await old["client"].disconnect()
        except Exception:
            pass

    client = _new_client()
    await client.connect()
    qr = await client.qr_login()
    _pending_qr[account_id] = {"qr": qr, "client": client}
    _owners[account_id] = owner_id
    return {
        "qr_url": qr.url,
        "expires": qr.expires.isoformat() if qr.expires else None,
    }


async def check_qr_login(*, account_id: str) -> dict:
    pending = _pending_qr.get(account_id)
    if not pending:
        return {"status": "not_started"}

    # Compat : si jamais un ancien format traîne (objet QRLogin direct), on récupère proprement
    if isinstance(pending, dict) and "qr" in pending:
        qr = pending["qr"]
        client = pending["client"]
    else:
        # Ancien format : pending est directement l'objet QRLogin
        qr = pending
        client = getattr(qr, "client", None)
        if client is None:
            _pending_qr.pop(account_id, None)
            return {"status": "not_started"}

    try:
        user: User = await qr.wait(timeout=2)
    except asyncio.TimeoutError:
        return {"status": "waiting"}
    except SessionPasswordNeededError:
        _pending_2fa[account_id] = client
        del _pending_qr[account_id]
        return {"status": "2fa_required"}
    except Exception as exc:
        del _pending_qr[account_id]
        return {"status": "error", "error": str(exc)}

    del _pending_qr[account_id]
    return await _finalize_login(account_id=account_id, client=client, user=user)


async def submit_2fa_password(*, account_id: str, password: str) -> dict:
    client = _pending_2fa.get(account_id)
    if not client:
        return {"status": "no_pending_2fa"}

    try:
        user: User = await client.sign_in(password=password)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

    del _pending_2fa[account_id]
    return await _finalize_login(account_id=account_id, client=client, user=user)


async def _finalize_login(*, account_id: str, client: TelegramClient, user: User) -> dict:
    owner_id = _owners[account_id]
    session_string = client.session.save()
    save_session(
        owner_id=owner_id,
        account_id=account_id,
        session_string=session_string,
        telegram_user_id=user.id,
        phone_number=user.phone,
    )
    _attach_handlers(client, owner_id, account_id)
    _clients[account_id] = client
    return {
        "status": "success",
        "telegram_user_id": user.id,
        "phone": user.phone,
        "first_name": user.first_name,
        "username": user.username,
    }


# ---------- Login par numéro de téléphone ----------

async def send_phone_code(*, owner_id: str, account_id: str, phone: str) -> dict:
    if account_id in _clients:
        return {"status": "already_connected"}

    client = _new_client()
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await client.disconnect()
        return {"status": "error", "error": "Numéro de téléphone invalide."}
    except Exception as exc:
        await client.disconnect()
        return {"status": "error", "error": str(exc)}

    _pending_phone[account_id] = {
        "client": client,
        "phone": phone,
        "phone_code_hash": sent.phone_code_hash,
    }
    _owners[account_id] = owner_id
    return {
        "status": "code_sent",
        "phone": phone,
        "phone_code_hash": sent.phone_code_hash,
    }


async def verify_phone_code(
    *, account_id: str, code: str, phone_code_hash: Optional[str] = None
) -> dict:
    pending = _pending_phone.get(account_id)
    if not pending:
        return {"status": "no_pending_phone"}

    client: TelegramClient = pending["client"]
    phone: str = pending["phone"]
    effective_hash: str = phone_code_hash or pending["phone_code_hash"]

    try:
        if not client.is_connected():
            await client.connect()
    except Exception as exc:
        del _pending_phone[account_id]
        return {"status": "error", "error": f"Connexion Telegram perdue : {exc}. Réessayez."}

    try:
        user: User = await client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=effective_hash,
        )
    except SessionPasswordNeededError:
        _pending_2fa[account_id] = client
        del _pending_phone[account_id]
        return {"status": "2fa_required"}
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        return {"status": "error", "error": "Code incorrect ou expiré."}
    except Exception as exc:
        print(f"[verify_phone_code] erreur sign_in {account_id}: {type(exc).__name__}: {exc}")
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    del _pending_phone[account_id]
    return await _finalize_login(account_id=account_id, client=client, user=user)


# Aliases rétro-compat
start_phone_login = send_phone_code
submit_phone_code = verify_phone_code


# ---------- Opérations ----------

def get_client(account_id: str) -> Optional[TelegramClient]:
    return _clients.get(account_id)


async def disconnect_account(account_id: str) -> None:
    client = _clients.pop(account_id, None)
    _owners.pop(account_id, None)
    _pending_qr.pop(account_id, None)
    _pending_2fa.pop(account_id, None)
    _pending_phone.pop(account_id, None)
    delete_session(account_id)
    if client:
        try:
            await client.log_out()
        except Exception:
            pass
        await client.disconnect()


async def send_message(*, account_id: str, telegram_chat_id: int, body: str) -> dict:
    client = _clients.get(account_id)
    if not client:
        return {"ok": False, "error": "account_not_connected"}
    try:
        sent = await client.send_message(telegram_chat_id, body)
        return {
            "ok": True,
            "telegram_message_id": sent.id,
            "sent_at": sent.date.isoformat() if sent.date else None,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def sync_history(*, account_id: str, max_chats: int = 50, max_messages_per_chat: int = 200) -> dict:
    client = _clients.get(account_id)
    if not client:
        return {"ok": False, "error": "account_not_connected"}

    owner_id = _owners[account_id]
    chats_synced = 0
    messages_synced = 0

    async for dialog in client.iter_dialogs(limit=max_chats):
        entity = dialog.entity
        if not isinstance(entity, User):
            continue

        full_name = " ".join(filter(None, [entity.first_name, entity.last_name])).strip() or (entity.username or "Inconnu")
        username = entity.username
        handle = f"@{username}" if username else f"id:{entity.id}"
        initials = "".join([p[0] for p in full_name.split()[:2]]).upper() or full_name[:2].upper()

        await post_to_lovable("sync-chat", {
            "owner_id": owner_id,
            "account_id": account_id,
            "telegram_chat_id": entity.id,
            "telegram_access_hash": str(getattr(entity, "access_hash", "") or ""),
            "contact": {
                "name": full_name,
                "handle": handle,
                "initials": initials,
                "telegram_user_id": entity.id,
            },
            "last_message_at": dialog.date.isoformat() if dialog.date else None,
            "last_message_text": (dialog.message.message if dialog.message else "") or "",
            "unread_count": dialog.unread_count or 0,
        })
        chats_synced += 1

        async for msg in client.iter_messages(entity, limit=max_messages_per_chat):
            if not msg.message:
                continue
            await post_to_lovable("incoming", {
                "owner_id": owner_id,
                "account_id": account_id,
                "telegram_chat_id": entity.id,
                "telegram_access_hash": str(getattr(entity, "access_hash", "") or ""),
                "telegram_message_id": msg.id,
                "direction": "out" if msg.out else "in",
                "body": msg.message,
                "sent_at": msg.date.isoformat() if msg.date else None,
                "is_history": True,
                "contact": {
                    "name": full_name,
                    "handle": handle,
                    "initials": initials,
                    "telegram_user_id": entity.id,
                },
            })
            messages_synced += 1

    return {"ok": True, "chats_synced": chats_synced, "messages_synced": messages_synced}

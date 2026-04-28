"""
Gestion multi-comptes Telethon

✔ Sessions persistées Supabase
✔ QR login + phone login + 2FA
✔ Sync historique
✔ Events temps réel (run_until_disconnected)
"""

import asyncio
import os
from typing import Dict, Optional

from telethon import TelegramClient, events
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import User

from sessions_store import delete_session, list_all_sessions, load_session_record, save_session
from sync_store import direct_storage_enabled, upsert_incoming, upsert_sync_chat
from webhook import post_to_lovable

# ---------------- CONFIG ----------------

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")

_clients: Dict[str, TelegramClient] = {}
_owners: Dict[str, str] = {}

_pending_qr: Dict[str, dict] = {}
_pending_2fa: Dict[str, TelegramClient] = {}
_pending_phone: Dict[str, dict] = {}

# ---------------- DIAGNOSTICS ----------------

_persist_diagnostics: Dict[str, int] = {
    "ok": 0,
    "error": 0,
}


def get_persist_diagnostics() -> dict:
    return dict(_persist_diagnostics)


def reset_persist_diagnostics() -> None:
    _persist_diagnostics["ok"] = 0
    _persist_diagnostics["error"] = 0


# ---------------- KEEP ALIVE LOOP ----------------

async def _run_client_forever(client: TelegramClient, account_id: str):
    """
    Maintient la boucle asyncio active pour recevoir les events Telethon en temps réel.
    Sans ça, aucun event NewMessage n'est déclenché.
    """
    print(f"[telethon] 🔁 loop active {account_id}")
    try:
        await client.run_until_disconnected()
    except Exception as e:
        print(f"[telethon] ❌ loop crash {account_id}: {e}")


# ---------------- CLIENT FACTORY ----------------

def _new_client(session_string: Optional[str] = None) -> TelegramClient:
    if not API_ID or not API_HASH:
        raise RuntimeError("TELEGRAM_API_ID / TELEGRAM_API_HASH manquants")
    session = StringSession(session_string) if session_string else StringSession()
    return TelegramClient(session, API_ID, API_HASH)


# ---------------- PERSIST PAYLOAD ----------------

async def _persist_payload(path: str, payload: dict) -> None:
    """Envoie un payload vers le stockage direct ou le webhook Lovable."""
    try:
        if direct_storage_enabled():
            if path == "sync-chat":
                upsert_sync_chat(payload)
            else:
                upsert_incoming(payload)
        else:
            await post_to_lovable(path, payload)
        _persist_diagnostics["ok"] += 1
    except Exception as exc:
        _persist_diagnostics["error"] += 1
        print(f"[telethon] _persist_payload error ({path}): {exc}")


# ---------------- HANDLERS ----------------

def _attach_handlers(client: TelegramClient, owner_id: str, account_id: str):

    @client.on(events.NewMessage(incoming=True))
    async def _on_incoming(event):
        await _forward(event, owner_id, account_id, "in")

    @client.on(events.NewMessage(outgoing=True))
    async def _on_outgoing(event):
        await _forward(event, owner_id, account_id, "out")


# ---------------- FORWARD MESSAGE ----------------

async def _forward(event, owner_id: str, account_id: str, direction: str):
    try:
        msg = event.message
        chat = await event.get_chat()

        if not isinstance(chat, User):
            return

        full_name = " ".join(filter(None, [chat.first_name, chat.last_name])).strip() or (chat.username or "Inconnu")
        username = chat.username
        handle = f"@{username}" if username else f"id:{chat.id}"
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

        await _persist_payload("incoming", payload)

    except Exception as exc:
        print(f"[telethon] forward error {account_id}: {exc}")


# ---------------- RESTORE SESSIONS ----------------

async def restore_all_sessions() -> None:
    """Au démarrage du service, reconnecte tous les comptes existants."""
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


# ---------------- CONNECT WITH SESSION ----------------

async def _connect_with_session(*, owner_id: str, account_id: str, session_string: str) -> TelegramClient:
    client = _new_client(session_string)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError(f"Session expirée pour {account_id}")
    _attach_handlers(client, owner_id, account_id)
    _clients[account_id] = client
    _owners[account_id] = owner_id
    asyncio.create_task(_run_client_forever(client, account_id))
    print(f"[telethon] compte {account_id} connecté")
    return client


# ---------------- ENSURE CONNECTED ----------------

async def ensure_connected(account_id: str) -> Optional[TelegramClient]:
    """Retourne un client connecté et autorisé, en rechargeant depuis la DB si nécessaire."""
    client = _clients.get(account_id)
    if client:
        try:
            if not client.is_connected():
                await client.connect()
            if await client.is_user_authorized():
                return client
        except Exception as exc:
            print(f"[telethon] client mémoire invalide {account_id}: {exc}")
        _clients.pop(account_id, None)

    session = load_session_record(account_id)
    if not session or not session.get("session_string"):
        return None
    return await _connect_with_session(
        owner_id=session["owner_id"],
        account_id=session["account_id"],
        session_string=session["session_string"],
    )


# ---------------- FINALIZE LOGIN ----------------

async def _finalize_login(*, account_id: str, client: TelegramClient, user: User) -> dict:
    """Persistance + attache des handlers temps réel après un login réussi."""
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
    asyncio.create_task(_run_client_forever(client, account_id))
    print(f"[telethon] login complete {account_id}")
    return {
        "status": "success",
        "telegram_user_id": user.id,
        "phone": user.phone,
        "first_name": user.first_name,
        "username": user.username,
    }


# ---------------- QR LOGIN ----------------

async def start_qr_login(owner_id: str, account_id: str) -> dict:
    """Démarre une connexion QR. Renvoie l'URL tg://login?token=..."""
    if account_id in _clients:
        return {"already_connected": True}

    client = _new_client()
    await client.connect()
    qr = await client.qr_login()

    _pending_qr[account_id] = {"client": client, "qr": qr}
    _owners[account_id] = owner_id

    return {
        "qr_url": qr.url,
        "expires": qr.expires.isoformat() if qr.expires else None,
    }


async def check_qr_login(account_id: str) -> dict:
    """Vérifie si l'utilisateur a scanné le QR."""
    pending = _pending_qr.get(account_id)
    if not pending:
        return {"status": "not_started"}

    qr = pending["qr"]
    client = pending["client"]

    try:
        user: User = await qr.wait(timeout=2)
    except asyncio.TimeoutError:
        return {"status": "waiting"}
    except SessionPasswordNeededError:
        _pending_2fa[account_id] = client
        _pending_qr.pop(account_id, None)
        return {"status": "2fa_required"}
    except Exception as exc:
        _pending_qr.pop(account_id, None)
        return {"status": "error", "error": str(exc)}

    _pending_qr.pop(account_id, None)
    return await _finalize_login(account_id=account_id, client=client, user=user)


# ---------------- 2FA ----------------

async def submit_2fa_password(account_id: str, password: str) -> dict:
    """Finalise le login (QR ou téléphone) quand un mot de passe 2FA est requis."""
    client = _pending_2fa.get(account_id)
    if not client:
        return {"status": "no_pending_2fa"}

    try:
        user: User = await client.sign_in(password=password)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}

    _pending_2fa.pop(account_id, None)
    return await _finalize_login(account_id=account_id, client=client, user=user)


# ---------------- PHONE LOGIN ----------------

async def send_phone_code(owner_id: str, account_id: str, phone: str) -> dict:
    """
    Étape 1 du login par numéro : Telegram envoie un code à l'utilisateur
    (in-app si connecté ailleurs, sinon SMS).
    """
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


async def verify_phone_code(account_id: str, code: str, phone_code_hash: Optional[str] = None) -> dict:
    """Étape 2 : valide le code reçu. Peut nécessiter le 2FA ensuite."""
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
        _pending_phone.pop(account_id, None)
        return {"status": "error", "error": f"Connexion Telegram perdue : {exc}. Réessayez."}

    try:
        user: User = await client.sign_in(
            phone=phone,
            code=code,
            phone_code_hash=effective_hash,
        )
    except SessionPasswordNeededError:
        _pending_2fa[account_id] = client
        _pending_phone.pop(account_id, None)
        return {"status": "2fa_required"}
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        return {"status": "error", "error": "Code incorrect ou expiré."}
    except Exception as exc:
        print(f"[verify_phone_code] erreur sign_in {account_id}: {type(exc).__name__}: {exc}")
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}

    _pending_phone.pop(account_id, None)
    return await _finalize_login(account_id=account_id, client=client, user=user)


# Aliases pour compatibilité
start_phone_login = send_phone_code
submit_phone_code = verify_phone_code


# ---------------- CLIENT OPERATIONS ----------------

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
        try:
            await client.disconnect()
        except Exception:
            pass


async def send_message(account_id: str, telegram_chat_id: int, body: str) -> dict:
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


# ---------------- SYNC HISTORY ----------------

async def sync_history(account_id: str, max_chats: int = 50, max_messages_per_chat: int = 200) -> dict:
    """Importe l'historique récent : pour chaque DM, les N derniers messages."""
    client = await ensure_connected(account_id)
    if not client:
        return {"ok": False, "error": "account_not_connected"}

    # Vérification supplémentaire post-reconnexion
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as exc:
            return {"ok": False, "error": f"reconnect failed: {exc}"}

    if not await client.is_user_authorized():
        return {"ok": False, "error": "not_authorized"}

    owner_id = _owners.get(account_id, "")
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

        sync_payload = {
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
        }
        await _persist_payload("sync-chat", sync_payload)
        chats_synced += 1

        async for msg in client.iter_messages(entity, limit=max_messages_per_chat):
            if not msg.message:
                continue
            incoming_payload = {
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
            }
            await _persist_payload("incoming", incoming_payload)
            messages_synced += 1

    return {"ok": True, "chats_synced": chats_synced, "messages_synced": messages_synced}

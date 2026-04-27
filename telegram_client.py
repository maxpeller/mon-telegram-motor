"""
Gestion multi-comptes Telethon.

Chaque compte Telegram (= 1 ligne dans `telegram_accounts` côté Lovable) a son
propre TelegramClient stocké en mémoire. Les sessions sont sérialisées en
StringSession et persistées dans Supabase pour survivre aux redémarrages.

Login par QR code (recommandé pour multi-comptes utilisateur) :
    1. POST /accounts/{account_id}/login/qr/start  -> renvoie qr_url + token
    2. L'utilisateur scanne avec son téléphone (Telegram → Paramètres → Appareils → Lier un appareil)
    3. POST /accounts/{account_id}/login/qr/check  -> renvoie status (waiting / 2fa_required / success)
    4. Si 2FA activé : POST /accounts/{account_id}/login/2fa { password }

Une fois connecté, un event handler temps réel écrit directement en base quand
SUPABASE_DB_URL est configuré, puis tente aussi le webhook en secours.
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
from telethon.tl.custom import QRLogin
from telethon.tl.types import User

from sessions_store import delete_session, list_all_sessions, load_session, load_session_record, save_session
from sync_store import direct_storage_enabled, upsert_incoming, upsert_sync_chat
from webhook import post_to_lovable

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")

# Clients Telethon actifs en mémoire : { account_id: TelegramClient }
_clients: Dict[str, TelegramClient] = {}
# Owner de chaque account (pour les webhooks) : { account_id: owner_id }
_owners: Dict[str, str] = {}
# Logins QR en cours : { account_id: QRLogin }
_pending_qr: Dict[str, dict] = {}
# Logins QR en attente de 2FA : { account_id: TelegramClient }
_pending_2fa: Dict[str, TelegramClient] = {}
# Logins par téléphone en attente du code SMS
_pending_phone: Dict[str, dict] = {}


def _new_client(session_string: Optional[str] = None) -> TelegramClient:
    if not API_ID or not API_HASH:
        raise RuntimeError("TELEGRAM_API_ID et TELEGRAM_API_HASH doivent être configurés")
    session = StringSession(session_string) if session_string else StringSession()
    return TelegramClient(session, API_ID, API_HASH, device_model="Opsis CRM", system_version="1.0")


def _attach_handlers(client: TelegramClient, owner_id: str, account_id: str) -> None:
    """Branche les listeners temps réel : nouveaux messages entrants/sortants."""

    @client.on(events.NewMessage(incoming=True))
    async def on_incoming(event):
        await _forward_message(event, owner_id, account_id, direction="in")

    @client.on(events.NewMessage(outgoing=True))
    async def on_outgoing(event):
        await _forward_message(event, owner_id, account_id, direction="out")


# Diagnostic : on garde trace des dernières opérations de persistance
_last_persist_error: Optional[dict] = None
_persist_counters: Dict[str, int] = {"sync_chat_ok": 0, "sync_chat_err": 0, "incoming_ok": 0, "incoming_err": 0}


def get_persist_diagnostics() -> dict:
    return {
        "direct_storage_enabled": direct_storage_enabled(),
        "counters": dict(_persist_counters),
        "last_error": _last_persist_error,
    }


def reset_persist_diagnostics() -> None:
    global _last_persist_error
    _last_persist_error = None
    for k in list(_persist_counters.keys()):
        _persist_counters[k] = 0


def _try_direct_write(kind: str, payload: dict) -> None:
    global _last_persist_error
    if not direct_storage_enabled():
        _last_persist_error = {"kind": kind, "error": "direct_storage_disabled (SUPABASE_DB_URL absent)"}
        return
    try:
        if kind == "incoming":
            upsert_incoming(payload)
            _persist_counters["incoming_ok"] += 1
        else:
            upsert_sync_chat(payload)
            _persist_counters["sync_chat_ok"] += 1
    except Exception as exc:
        import traceback as _tb
        tb = _tb.format_exc()
        _persist_counters[f"{'incoming' if kind == 'incoming' else 'sync_chat'}_err"] += 1
        _last_persist_error = {
            "kind": kind,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": tb.splitlines()[-6:],
            "payload_keys": list(payload.keys()),
            "owner_id": payload.get("owner_id"),
            "account_id": payload.get("account_id"),
            "telegram_chat_id": payload.get("telegram_chat_id"),
        }
        print(f"[telethon] écriture directe {kind} ÉCHEC: {exc}\n{tb}")


async def _persist_payload(kind: str, payload: dict) -> None:
    """Écrit en base directement; le webhook reste un secours non bloquant."""
    if direct_storage_enabled():
        _try_direct_write(kind, payload)
        return
    await post_to_lovable(kind, payload)


async def _forward_message(event, owner_id: str, account_id: str, direction: str) -> None:
    """Transforme un événement Telethon en payload pour Lovable."""
    try:
        msg = event.message
        chat = await event.get_chat()

        # On ignore groupes et channels (CRM = DM uniquement)
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
        await _persist_payload("incoming", payload)
    except Exception as exc:
        print(f"[telethon] erreur forward {account_id}: {exc}")


# ---------- Lifecycle ----------

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


async def _safe_initial_sync(account_id: str) -> None:
    try:
        result = await sync_history(account_id=account_id, max_chats=50, max_messages_per_chat=200)
        print(f"[telethon] sync initiale {account_id}: {result}")
    except Exception as exc:
        print(f"[telethon] erreur sync initiale {account_id}: {exc}")


def _start_initial_sync(account_id: str) -> None:
    asyncio.create_task(_safe_initial_sync(account_id))


# ---------- Login QR ----------

async def start_qr_login(*, owner_id: str, account_id: str) -> dict:
    """Démarre une connexion QR. Renvoie l'URL `tg://login?token=...`."""
    if account_id in _clients:
        return {"already_connected": True}

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
    """Vérifie si l'utilisateur a scanné le QR."""
    pending = _pending_qr.get(account_id)
    if not pending:
        return {"status": "not_started"}

    if isinstance(pending, dict) and "qr" in pending:
        qr = pending["qr"]
        client = pending["client"]
    else:
        qr = pending
        client = _clients.get(account_id)
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
    del _pending_qr[account_id]
    _start_initial_sync(account_id)

    return {
        "status": "success",
        "telegram_user_id": user.id,
        "phone": user.phone,
        "first_name": user.first_name,
        "username": user.username,
    }


async def submit_2fa_password(*, account_id: str, password: str) -> dict:
    """Finalise le login (QR ou téléphone) quand un mot de passe 2FA est requis."""
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
    _start_initial_sync(account_id)
    return {
        "status": "success",
        "telegram_user_id": user.id,
        "phone": user.phone,
        "first_name": user.first_name,
        "username": user.username,
    }


# ---------- Login par numéro de téléphone ----------

async def send_phone_code(*, owner_id: str, account_id: str, phone: str) -> dict:
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


async def verify_phone_code(
    *, account_id: str, code: str, phone_code_hash: Optional[str] = None
) -> dict:
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


# Aliases pour compatibilité
start_phone_login = send_phone_code
submit_phone_code = verify_phone_code


# ---------- Opérations ----------

def get_client(account_id: str) -> Optional[TelegramClient]:
    return _clients.get(account_id)


async def ensure_connected(account_id: str) -> Optional[TelegramClient]:
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


async def disconnect_account(account_id: str) -> None:
    client = _clients.pop(account_id, None)
    _owners.pop(account_id, None)
    _pending_qr.pop(account_id, None)
    _pending_2fa.pop(account_id, None)
    _pending_phone.pop(account_id, None)
    delete_session(account_id)
    if client:
        await client.log_out()
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
    """Importe l'historique récent : pour chaque DM, les N derniers messages."""
    client = await ensure_connected(account_id)
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

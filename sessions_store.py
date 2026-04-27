"""
Stockage des sessions Telethon dans Supabase.

On utilise StringSession (sérialisation texte) plutôt que des fichiers .session
pour que le service puisse redémarrer sans perdre les connexions Telegram.

Table attendue côté Supabase (créée par migration Lovable) :
    telethon_sessions (
        id uuid pk,
        owner_id uuid,
        account_id uuid,
        session_string text,
        telegram_user_id bigint,
        phone_number text,
        created_at timestamptz,
        updated_at timestamptz
    )
"""
import os
from typing import Optional

from supabase import Client, create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

_client: Optional[Client] = None


def get_supabase() -> Client:
    global _client
    if _client is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise RuntimeError(
                "SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY doivent être configurés"
            )
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client


def load_session(account_id: str) -> Optional[str]:
    """Récupère la session_string Telethon pour un compte."""
    res = (
        get_supabase()
        .table("telethon_sessions")
        .select("session_string")
        .eq("account_id", account_id)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    if rows and rows[0].get("session_string"):
        return rows[0]["session_string"]
    return None


def save_session(
    *,
    owner_id: str,
    account_id: str,
    session_string: str,
    telegram_user_id: Optional[int] = None,
    phone_number: Optional[str] = None,
) -> None:
    payload = {
        "owner_id": owner_id,
        "account_id": account_id,
        "session_string": session_string,
    }
    if telegram_user_id is not None:
        payload["telegram_user_id"] = telegram_user_id
    if phone_number is not None:
        payload["phone_number"] = phone_number

    # upsert sur (owner_id, account_id)
    get_supabase().table("telethon_sessions").upsert(
        payload, on_conflict="owner_id,account_id"
    ).execute()


def list_all_sessions() -> list[dict]:
    """Toutes les sessions à reconnecter au démarrage du service."""
    res = (
        get_supabase()
        .table("telethon_sessions")
        .select("owner_id,account_id,session_string,telegram_user_id,phone_number")
        .execute()
    )
    return res.data or []


def delete_session(account_id: str) -> None:
    get_supabase().table("telethon_sessions").delete().eq(
        "account_id", account_id
    ).execute()

"""
Stockage des sessions Telethon dans Supabase.

On utilise StringSession (sérialisation texte) plutôt que des fichiers .session
pour que le service puisse redémarrer sans perdre les connexions Telegram.
"""
import os
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "")

_client: Optional[Any] = None


def storage_backend() -> str:
    """Backend réellement utilisé pour les sessions Telethon."""
    if _has_direct_db():
        return "direct_db"
    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        return "rest"
    return "not_configured"


def _db_url_with_ssl() -> str:
    if not SUPABASE_DB_URL:
        return ""
    parts = urlsplit(SUPABASE_DB_URL)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _has_direct_db() -> bool:
    return bool(SUPABASE_DB_URL)


def _connect_db():
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "Le package psycopg est requis pour SUPABASE_DB_URL. "
            "Redéployez le service après `pip install -r requirements.txt`."
        ) from exc
    return psycopg.connect(_db_url_with_ssl(), row_factory=dict_row)


def _raise_cache_hint(exc: Exception) -> None:
    message = str(exc)
    if "PGRST205" in message or "telethon_sessions" in message:
        raise RuntimeError(
            "La table telethon_sessions existe, mais l'API REST du backend ne la voit pas. "
            "Ajoutez SUPABASE_DB_URL aux variables Railway du service Telethon puis redéployez."
        ) from exc
    raise exc


def _is_schema_cache_error(exc: Exception) -> bool:
    message = str(exc)
    return "PGRST205" in message and "telethon_sessions" in message


def _warn_schema_cache(operation: str, exc: Exception) -> None:
    print(
        f"[sessions_store] {operation}: telethon_sessions inaccessible via REST "
        f"(cache schema PGRST205). Le service continue sans bloquer. Détail: {exc}"
    )


def get_supabase() -> Any:
    global _client
    if _client is None:
        try:
            from supabase import create_client
        except ImportError as exc:
            raise RuntimeError(
                "Le package supabase est requis si SUPABASE_DB_URL n'est pas configuré."
            ) from exc
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise RuntimeError("SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY doivent être configurés")
        if not SUPABASE_SERVICE_ROLE_KEY.startswith("eyJ"):
            raise RuntimeError(
                "SUPABASE_SERVICE_ROLE_KEY doit être la clé service_role JWT, pas la clé anon/publishable."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client


def load_session(account_id: str) -> Optional[str]:
    if _has_direct_db():
        with _connect_db() as conn, conn.cursor() as cur:
            cur.execute(
                "select session_string from public.telethon_sessions where account_id = %s limit 1",
                (account_id,),
            )
            row = cur.fetchone()
            return row["session_string"] if row and row.get("session_string") else None
    try:
        res = (
            get_supabase()
            .table("telethon_sessions")
            .select("session_string")
            .eq("account_id", account_id)
            .limit(1)
            .execute()
        )
        rows = res.data or []
    except Exception as exc:
        if _is_schema_cache_error(exc):
            _warn_schema_cache("load_session", exc)
            return None
        _raise_cache_hint(exc)
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

    if _has_direct_db():
        with _connect_db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update public.telethon_sessions
                set
                    session_string = %s,
                    telegram_user_id = coalesce(%s, telegram_user_id),
                    phone_number = coalesce(%s, phone_number),
                    updated_at = now()
                where account_id = %s
                """,
                (session_string, telegram_user_id, phone_number, account_id),
            )
            if cur.rowcount == 0:
                cur.execute(
                    """
                    insert into public.telethon_sessions
                        (owner_id, account_id, session_string, telegram_user_id, phone_number, updated_at)
                    values (%s, %s, %s, %s, %s, now())
                    """,
                    (owner_id, account_id, session_string, telegram_user_id, phone_number),
                )
        return

    try:
        get_supabase().table("telethon_sessions").upsert(
            payload, on_conflict="owner_id,account_id"
        ).execute()
    except Exception as exc:
        if _is_schema_cache_error(exc):
            _warn_schema_cache("save_session", exc)
            return
        _raise_cache_hint(exc)


def list_all_sessions() -> list[dict]:
    if _has_direct_db():
        with _connect_db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select owner_id::text, account_id::text, session_string, telegram_user_id, phone_number
                from public.telethon_sessions
                order by updated_at desc
                """
            )
            return list(cur.fetchall() or [])
    try:
        res = (
            get_supabase()
            .table("telethon_sessions")
            .select("owner_id,account_id,session_string,telegram_user_id,phone_number")
            .execute()
        )
        return res.data or []
    except Exception as exc:
        if _is_schema_cache_error(exc):
            _warn_schema_cache("list_all_sessions", exc)
            return []
        _raise_cache_hint(exc)


def delete_session(account_id: str) -> None:
    if _has_direct_db():
        with _connect_db() as conn, conn.cursor() as cur:
            cur.execute("delete from public.telethon_sessions where account_id = %s", (account_id,))
        return
    try:
        get_supabase().table("telethon_sessions").delete().eq("account_id", account_id).execute()
    except Exception as exc:
        if _is_schema_cache_error(exc):
            _warn_schema_cache("delete_session", exc)
            return
        _raise_cache_hint(exc)

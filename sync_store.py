"""Persistance directe des conversations/messages Telegram dans la base."""

from typing import Any, Dict, Optional

from sessions_store import _connect_db, _has_direct_db


def direct_storage_enabled() -> bool:
    return _has_direct_db()


def _contact(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get("contact") or {}


def upsert_sync_chat(payload: Dict[str, Any]) -> Optional[str]:
    contact = _contact(payload)
    with _connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.conversations (
                    owner_id, account_id, telegram_chat_id, telegram_access_hash,
                    contact_handle, contact_name, contact_initials,
                    last_message_text, last_message_at, unread_count, status
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'new')
                on conflict (owner_id, account_id, telegram_chat_id) where telegram_chat_id is not null
                do update set
                    telegram_access_hash = excluded.telegram_access_hash,
                    contact_handle = excluded.contact_handle,
                    contact_name = excluded.contact_name,
                    contact_initials = excluded.contact_initials,
                    last_message_text = excluded.last_message_text,
                    last_message_at = excluded.last_message_at,
                    unread_count = excluded.unread_count,
                    updated_at = now()
                returning id::text
                """,
                (
                    payload["owner_id"],
                    payload["account_id"],
                    payload["telegram_chat_id"],
                    payload.get("telegram_access_hash") or None,
                    contact.get("handle") or "",
                    contact.get("name") or "Inconnu",
                    contact.get("initials") or "",
                    (payload.get("last_message_text") or "")[:200],
                    payload.get("last_message_at"),
                    payload.get("unread_count") or 0,
                ),
            )
            row = cur.fetchone()
            result_id = row["id"] if row else None
        conn.commit()
        return result_id


def upsert_incoming(payload: Dict[str, Any]) -> Optional[str]:
    contact = _contact(payload)
    sent_at = payload.get("sent_at")
    is_history = bool(payload.get("is_history"))
    with _connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.conversations (
                    owner_id, account_id, telegram_chat_id, telegram_access_hash,
                    contact_handle, contact_name, contact_initials,
                    last_message_text, last_message_at, unread_count, status
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, coalesce(%s::timestamptz, now()), %s, 'new')
                on conflict (owner_id, account_id, telegram_chat_id) where telegram_chat_id is not null
                do update set
                    telegram_access_hash = excluded.telegram_access_hash,
                    contact_handle = excluded.contact_handle,
                    contact_name = excluded.contact_name,
                    contact_initials = excluded.contact_initials,
                    last_message_text = case
                        when public.conversations.last_message_at is null
                          or excluded.last_message_at >= public.conversations.last_message_at
                        then excluded.last_message_text
                        else public.conversations.last_message_text
                    end,
                    last_message_at = greatest(
                        coalesce(public.conversations.last_message_at, excluded.last_message_at),
                        excluded.last_message_at
                    ),
                    unread_count = public.conversations.unread_count + case when %s then 1 else 0 end,
                    updated_at = now()
                returning id::text
                """,
                (
                    payload["owner_id"],
                    payload["account_id"],
                    payload["telegram_chat_id"],
                    payload.get("telegram_access_hash") or None,
                    contact.get("handle") or "",
                    contact.get("name") or "Inconnu",
                    contact.get("initials") or "",
                    (payload.get("body") or "")[:200],
                    sent_at,
                    0 if is_history or payload.get("direction") != "in" else 1,
                    (not is_history and payload.get("direction") == "in"),
                ),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            conversation_id = row["id"]
            cur.execute(
                """
                insert into public.messages (
                    owner_id, conversation_id, direction, body, sent_at, telegram_message_id
                )
                values (%s, %s, %s, %s, coalesce(%s::timestamptz, now()), %s)
                on conflict (owner_id, conversation_id, telegram_message_id) where telegram_message_id is not null
                do nothing
                """,
                (
                    payload["owner_id"],
                    conversation_id,
                    payload["direction"],
                    payload.get("body") or "",
                    sent_at,
                    payload.get("telegram_message_id"),
                ),
            )
        conn.commit()
        return conversation_id

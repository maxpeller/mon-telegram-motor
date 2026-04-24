"""
Envoi signé HMAC vers les routes /api/public/telethon/* de l'app Lovable.
Chaque payload est signé pour que Lovable puisse vérifier l'authenticité.
"""
import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict

import httpx

LOVABLE_WEBHOOK_URL = os.environ.get("LOVABLE_WEBHOOK_URL", "").rstrip("/")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")


def _sign(body: str) -> str:
    return hmac.new(
        WEBHOOK_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def post_to_lovable(path: str, payload: Dict[str, Any]) -> None:
    """
    POST vers https://<lovable-app>/api/public/telethon/<path>
    Headers:
      - x-telethon-signature: HMAC-SHA256(timestamp + "." + body) en hex
      - x-telethon-timestamp: timestamp unix (anti-replay)
      - content-type: application/json
    """
    if not LOVABLE_WEBHOOK_URL or not WEBHOOK_SECRET:
        print(f"[webhook] LOVABLE_WEBHOOK_URL ou WEBHOOK_SECRET manquant, skip {path}")
        return

    url = f"{LOVABLE_WEBHOOK_URL}/api/public/telethon/{path.lstrip('/')}"
    body = json.dumps(payload, default=str, separators=(",", ":"))
    timestamp = str(int(time.time()))
    signed_payload = f"{timestamp}.{body}"
    signature = _sign(signed_payload)

    headers = {
        "content-type": "application/json",
        "x-telethon-signature": signature,
        "x-telethon-timestamp": timestamp,
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, content=body, headers=headers)
            if resp.status_code >= 400:
                print(f"[webhook] {path} -> {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"[webhook] {path} -> {resp.status_code} OK")
    except Exception as exc:
        print(f"[webhook] erreur POST {url}: {exc}")


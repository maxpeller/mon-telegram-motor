import os
from fastapi import FastAPI, Header, HTTPException
from telethon import TelegramClient, StringSession
from pydantic import BaseModel
import httpx

app = FastAPI()

# Configuration (Railway récupère ces variables automatiquement)
API_ID = int(os.getenv("TELEGRAM_API_ID", 0))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SERVICE_API_KEY = os.getenv("SERVICE_API_KEY")
URL_WEBHOOK_LOVABLE = os.getenv("URL_WEBHOOK_LOVABLE")

# Dictionnaire temporaire pour stocker les clients en cours de connexion
clients = {}

class LoginRequest(BaseModel):
    phone: str

class VerifyRequest(BaseModel):
    phone: str
    code: str
    phone_code_hash: str

@app.get("/")
async def root():
    return {"status": "Moteur Telethon Actif"}

# 1. Demander l'envoi du code SMS
@app.post("/send-code")
async def send_code(req: LoginRequest, x_api_key: str = Header(None)):
    if x_api_key != SERVICE_API_KEY:
        raise HTTPException(status_code=403, detail="Clé API invalide")
    
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    result = await client.send_code_request(req.phone)
    clients[req.phone] = {"client": client, "hash": result.phone_code_hash}
    
    return {"phone_code_hash": result.phone_code_hash}

# 2. Vérifier le code et créer la session
@app.post("/verify-code")
async def verify_code(req: VerifyRequest, x_api_key: str = Header(None)):
    if x_api_key != SERVICE_API_KEY:
        raise HTTPException(status_code=403, detail="Clé API invalide")
    
    data = clients.get(req.phone)
    if not data:
        raise HTTPException(status_code=400, detail="Session expirée")
    
    client = data["client"]
    try:
        await client.sign_in(req.phone, req.code, phone_code_hash=req.phone_code_hash)
        session_string = client.session.save()
        return {"session_string": session_string, "status": "connected"}
    except Exception as e:
        return {"error": str(e)}

# 3. Ecouteur de messages (Webhook vers Lovable)
# Note: Pour un SaaS complet, ce bloc nécessite un "worker" séparé qui tourne 24/7

import os
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def home():
    return {"message": "Le moteur Telethon est prêt !"}

@app.get("/health")
async def health():
    return {"status": "ok"}

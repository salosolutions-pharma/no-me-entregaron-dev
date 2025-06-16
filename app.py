# app.py
import os
import logging
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from channels.telegram_c import create_application, check_expired_sessions

logger = logging.getLogger(__name__)
app = FastAPI()
app.state.telegram_app = None

@app.on_event("startup")
async def startup_event():
    # 1) Construir y arrancar la app de Telegram
    application = create_application()
    await application.initialize()
    await application.start()

    # 2) Registrar jobs periódicos
    interval = int(os.getenv("CHECK_INTERVAL_SECONDS", 60))
    application.job_queue.run_repeating(
        callback=check_expired_sessions,
        interval=interval,
        first=10
    )

    # 3) Registrar el webhook en Telegram
    webhook_base = os.getenv("WEBHOOK_BASE_URL")
    if not webhook_base:
        logger.critical("No WEBHOOK_BASE_URL configurada")
        raise RuntimeError("Falta WEBHOOK_BASE_URL")
    full_url = f"{webhook_base.rstrip('/')}/webhook"
    await application.bot.set_webhook(full_url)
    logger.info("Webhook registrado en %s", full_url)

    # 4) Guardar la instancia para usarla en el endpoint
    app.state.telegram_app = application

@app.on_event("shutdown")
async def shutdown_event():
    application = app.state.telegram_app
    if application:
        await application.shutdown()
        await application.stop()

@app.post("/webhook")
async def telegram_webhook(req: Request):
    application = app.state.telegram_app
    if not application:
        raise HTTPException(503, "Bot no inicializado")
    body = await req.json()
    logger.info("🔔 Webhook received: %s", body)
    update = Update.de_json(body, application.bot)
    await application.process_update(update)
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "healthy"}






# app.py

import os
import logging
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from channels.telegram_c import create_application
import traceback
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
# Si tienes un módulo de WhatsApp, impórtalo también:
# from channels.whatsapp_c import WhatsAppClient

logger = logging.getLogger(__name__)
app = FastAPI()
app.state.telegram_app = None
# app.state.whatsapp_client = None

@app.on_event("startup")
async def startup_event():
    # 1) Inicializar Telegram
    telegram_app = create_application()
    await telegram_app.initialize()
    await telegram_app.start()
    app.state.telegram_app = telegram_app

    # 2) (Opcional) Inicializar WhatsApp client
    # wa_client = WhatsAppClient(...)
    # await wa_client.initialize()
    # app.state.whatsapp_client = wa_client

    # 3) Registrar jobs periódicos
    """interval = int(os.getenv("CHECK_INTERVAL_SECONDS", 60))
    telegram_app.job_queue.run_repeating(
        callback=check_expired_sessions,
        interval=interval,
        first=10
    )"""

    # 4) Registrar webhook Telegram
    webhook_base = os.getenv("WEBHOOK_BASE_URL")
    if not webhook_base:
        logger.critical("No WEBHOOK_BASE_URL configurada")
        raise RuntimeError("Falta WEBHOOK_BASE_URL")
    full_url = f"{webhook_base.rstrip('/')}/webhook"
    await telegram_app.bot.set_webhook(full_url)
    logger.info("Webhook registrado en %s", full_url)

@app.on_event("shutdown")
async def shutdown_event():
    if app.state.telegram_app:
        await app.state.telegram_app.shutdown()
        await app.state.telegram_app.stop()
    # if app.state.whatsapp_client:
    #     await app.state.whatsapp_client.shutdown()

@app.post("/webhook")
async def telegram_webhook(req: Request):
    telegram_app = app.state.telegram_app
    if not telegram_app:
        raise HTTPException(503, "Bot no inicializado")
    body = await req.json()
    logger.info("🔔 Webhook received: %s", body)
    update = Update.de_json(body, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}

@app.post("/send_message")
async def send_message(req: Request):
    """
    Recibe payload {"user_id": "TL_57123…", "message": "texto"}
    y reenvía por Telegram o WhatsApp.
    """
    data = await req.json()
    sid = data.get("user_id")
    session_id = data.get("session_id")
    text = data.get("message")
    buttons = data.get("buttons")
    if not sid or not text:
        raise HTTPException(400, "user_id y message son requeridos")
    prefix, rest = sid.split("_", 1)
    if prefix == "TL":
        # Telegram: el resto contiene el chat_id (e.g. país+número)
        chat_id = int(rest.split("_")[0])
        logger.info(f"Intentando enviar mensaje Telegram a chat_id: +{chat_id} y sesion {session_id}")
        try:
            if buttons:
                # construye InlineKeyboardMarkup
                kb = [
                    [InlineKeyboardButton(btn["label"], callback_data=f"{btn['action']}_{session_id}")]
                    for btn in buttons
                ]
                reply_markup = InlineKeyboardMarkup(kb)
                await app.state.telegram_app.bot.send_message(
                    chat_id=chat_id, text=text, reply_markup=reply_markup
                )
            else:
                await app.state.telegram_app.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"❌ Error enviando Telegram a chat_id {chat_id} (user_id: {sid}): {e}\n{tb}")
            raise HTTPException(500, f"Falló envío Telegram: {str(e)}")
    elif prefix == "WA":
        # WhatsApp: usa tu cliente o API de WhatsApp
        wa_client = app.state.whatsapp_client
        if not wa_client:
            raise HTTPException(503, "Canal WhatsApp no está habilitado")
        phone = rest.split("_")[0]
        try:
            await wa_client.send_message(phone_number=phone, text=text)
        except Exception as e:
            logger.error(f"Error enviando WhatsApp a {phone}: {e}")
            raise HTTPException(500, "Falló envío WhatsApp")
    else:
        raise HTTPException(400, f"Canal desconocido en user_id: {prefix}")

    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "healthy"}

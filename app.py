# app.py

import os
import logging
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from channels.telegram_c import create_application
from channels.whatsapp import create_whatsapp_service
import traceback
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)
app = FastAPI()
app.state.telegram_app = None
app.state.whatsapp_service = None

@app.on_event("startup")
async def startup_event():
    # 1) Inicializar Telegram
    telegram_app = create_application()
    await telegram_app.initialize()
    await telegram_app.start()
    app.state.telegram_app = telegram_app

    # 2) Inicializar WhatsApp service
    try:
        whatsapp_service = create_whatsapp_service()
        app.state.whatsapp_service = whatsapp_service
        logger.info("WhatsApp service inicializado correctamente")
    except Exception as e:
        logger.warning(f"WhatsApp service no disponible: {e}")
        app.state.whatsapp_service = None

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
    if app.state.whatsapp_service:
        logger.info("WhatsApp service finalizado")

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
                # ✅ ARREGLO: usar "text" y "callback_data"
                kb = [
                    [InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"])]
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
        # WhatsApp: usa el servicio de WhatsApp Business API
        whatsapp_service = app.state.whatsapp_service
        if not whatsapp_service:
            raise HTTPException(503, "Canal WhatsApp no está habilitado")
        phone = rest.split("_")[0]
        logger.info(f"Intentando enviar mensaje WhatsApp a teléfono: {phone} y sesión {session_id}")
        try:
            success = await whatsapp_service.send_message(phone, text, buttons)
            if not success:
                raise HTTPException(500, "Falló envío WhatsApp")
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"❌ Error enviando WhatsApp a {phone} (user_id: {sid}): {e}\n{tb}")
            raise HTTPException(500, f"Falló envío WhatsApp: {str(e)}")
    else:
        raise HTTPException(400, f"Canal desconocido en user_id: {prefix}")

    return {"ok": True}

# Nuevos endpoints para WhatsApp

@app.get("/whatsapp/webhook")
async def whatsapp_webhook_verify(req: Request):
    """Verifica el webhook de WhatsApp durante la configuración."""
    mode = req.query_params.get("hub.mode")
    token = req.query_params.get("hub.verify_token")
    challenge = req.query_params.get("hub.challenge")
    
    whatsapp_service = app.state.whatsapp_service
    if not whatsapp_service:
        raise HTTPException(503, "WhatsApp service no disponible")
    
    result = whatsapp_service.verify_webhook(mode, token, challenge)
    if result:
        return int(result)  # WhatsApp espera el challenge como número
    else:
        raise HTTPException(403, "Verificación de webhook fallida")

@app.post("/whatsapp/webhook")
async def whatsapp_webhook_handler(req: Request):
    """Maneja webhooks entrantes de WhatsApp."""
    whatsapp_service = app.state.whatsapp_service
    if not whatsapp_service:
        raise HTTPException(503, "WhatsApp service no disponible")
    
    body = await req.json()
    logger.info("🔔 WhatsApp webhook received: %s", body)
    
    result = await whatsapp_service.handle_webhook(body)
    
    if result["status"] == "success":
        return {"ok": True}
    elif result["status"] == "ignored":
        return {"ok": True, "ignored": True, "reason": result.get("reason")}
    else:
        logger.error(f"Error procesando webhook WhatsApp: {result.get('error')}")
        raise HTTPException(500, f"Error procesando webhook: {result.get('error', 'Desconocido')}")

@app.get("/health")
async def health():
    """Health check que incluye estado de ambos canales."""
    telegram_healthy = app.state.telegram_app is not None
    whatsapp_healthy = False
    whatsapp_details = {}
    
    if app.state.whatsapp_service:
        whatsapp_health = app.state.whatsapp_service.health_check()
        whatsapp_healthy = whatsapp_health.get("healthy", False)
        whatsapp_details = whatsapp_health.get("components", {})
    
    return {
        "status": "healthy" if telegram_healthy else "degraded",
        "channels": {
            "telegram": {
                "status": "healthy" if telegram_healthy else "unhealthy",
                "available": telegram_healthy
            },
            "whatsapp": {
                "status": "healthy" if whatsapp_healthy else "unhealthy",
                "available": whatsapp_healthy,
                "components": whatsapp_details
            }
        },
        "overall_healthy": telegram_healthy  # Al menos Telegram debe estar funcionando
    }

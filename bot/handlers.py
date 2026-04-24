# bot/handlers.py
"""
Bot de Telegram para Onoruame — PJCDMX Entregas
Lee directo de PostgreSQL via repositories.

Funciones:
  - Repartidor solicita su ruta asignada
  - Ve paradas y personas en detalle
  - Abre Google Maps con la ruta
  - Manda fotos de acuses
  - Comparte ubicación en tiempo real (hasta 40 min)
  - Contacto con supervisor
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict

import telebot
from telebot import types

from core.repositories import (
    RutaRepo, RepartidorRepo, AvanceRepo, PersonaRepo, ParadaRepo
)
from core.database import db
from core.config import settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────────────────────

TOKEN = os.environ.get("BOT_TOKEN") or settings.__dict__.get("BOT_TOKEN", "")
if not TOKEN:
    raise ValueError("BOT_TOKEN no configurado en .env")

bot = telebot.TeleBot(TOKEN, parse_mode="Markdown")

# Estado en memoria: telegram_id → ruta_id asignada
_sesiones: Dict[int, int] = {}

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _get_repartidor(telegram_id: int) -> Optional[Dict]:
    """Busca repartidor por telegram_id en la BD."""
    with db.get_cursor() as cur:
        cur.execute(
            "SELECT * FROM repartidores WHERE telegram_id = %s AND activo = true",
            (str(telegram_id),)
        )
        row = cur.fetchone()
        return dict(row) if row else None

def _get_ruta_asignada(telegram_id: int) -> Optional[Dict]:
    """Retorna la ruta asignada al repartidor completa con paradas."""
    rep = _get_repartidor(telegram_id)
    if not rep:
        return None
    rutas = RutaRepo.list_all(estado='asignada')
    for ruta in rutas:
        if str(ruta.get('repartidor_id', '')) == str(rep['id']):
            return RutaRepo.get_full(ruta['id'])
    return None

def _menu_markup():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🚗 Mi ruta",          callback_data="ver_ruta"),
        types.InlineKeyboardButton("📋 Mis paradas",      callback_data="ver_paradas"),
        types.InlineKeyboardButton("📍 Compartir ubicación", callback_data="ubicacion"),
        types.InlineKeyboardButton("📞 Supervisor",       callback_data="supervisor"),
        types.InlineKeyboardButton("📊 Estado entregas",  callback_data="estado"),
    )
    return markup

# ─────────────────────────────────────────────────────────────
# /start — Registro y menú
# ─────────────────────────────────────────────────────────────

@bot.message_handler(commands=['start', 'menu', 'inicio'])
def cmd_start(message):
    telegram_id = message.from_user.id
    nombre      = message.from_user.first_name

    rep = _get_repartidor(telegram_id)

    if rep:
        texto = (
            f"👋 ¡Hola *{rep['nombre']}*!\n\n"
            f"⚡ Sistema Onoruame — PJCDMX\n\n"
            f"¿Qué deseas hacer?"
        )
        bot.send_message(message.chat.id, texto, reply_markup=_menu_markup())
    else:
        texto = (
            f"👋 Hola *{nombre}*\n\n"
            f"No estás registrado en el sistema.\n"
            f"Contacta a tu supervisor para que te registre con tu ID de Telegram:\n\n"
            f"`{telegram_id}`"
        )
        bot.send_message(message.chat.id, texto)

# ─────────────────────────────────────────────────────────────
# Ver ruta asignada
# ─────────────────────────────────────────────────────────────

@bot.message_handler(commands=['ruta', 'mi_ruta'])
def cmd_ruta(message):
    _mostrar_ruta(message.chat.id, message.from_user.id)

def _mostrar_ruta(chat_id: int, telegram_id: int):
    ruta = _get_ruta_asignada(telegram_id)

    if not ruta:
        bot.send_message(
            chat_id,
            "⚠️ *No tienes una ruta asignada.*\n\n"
            "Contacta a tu supervisor.",
            reply_markup=_menu_markup()
        )
        return

    texto = (
        f"🗺️ *RUTA {ruta['id']} — {ruta['zona']}*\n\n"
        f"📊 Estado: `{ruta['estado']}`\n"
        f"🏢 Paradas: *{ruta['total_paradas']}*\n"
        f"👥 Personas: *{ruta['total_personas']}*\n"
        f"📏 Distancia: *{ruta.get('distancia_km', 0):.1f} km*\n"
        f"⏱️ Tiempo estimado: *{ruta.get('tiempo_min', 0)} min*\n"
    )

    markup = types.InlineKeyboardMarkup(row_width=1)

    if ruta.get('google_maps_url'):
        markup.add(
            types.InlineKeyboardButton("📍 Abrir en Google Maps", url=ruta['google_maps_url'])
        )

    markup.add(
        types.InlineKeyboardButton("📋 Ver todas las paradas", callback_data=f"paradas_{ruta['id']}"),
        types.InlineKeyboardButton("⬅️ Menú",                  callback_data="menu"),
    )

    bot.send_message(chat_id, texto, reply_markup=markup)

# ─────────────────────────────────────────────────────────────
# Ver paradas detalladas
# ─────────────────────────────────────────────────────────────

def _mostrar_paradas(chat_id: int, ruta_id: int):
    ruta = RutaRepo.get_full(ruta_id)
    if not ruta:
        bot.send_message(chat_id, "❌ Ruta no encontrada")
        return

    paradas = ruta.get('paradas', [])
    if not paradas:
        bot.send_message(chat_id, "⚠️ Esta ruta no tiene paradas registradas")
        return

    for i, parada in enumerate(paradas, 1):
        import json as _json
        personas = parada.get('personas', [])
        if isinstance(personas, str):
            try:
                personas = _json.loads(personas)
            except Exception:
                personas = []

        estado_icono = "✅" if parada.get('estado') == 'visitada' else "⏳"

        texto = (
            f"{estado_icono} *Parada {i}*\n"
            f"📍 {parada.get('direccion_original', '')[:100]}\n"
            f"🏛️ {parada.get('alcaldia', '')}\n\n"
        )

        if personas:
            texto += "*Personas a entregar:*\n"
            for p in personas:
                est = "✅" if p.get('estado') == 'entregado' else "⏳"
                texto += f"  {est} {p.get('nombre', '')}\n"

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton(
                "✅ Marcar visitada",
                callback_data=f"visitada_{parada['id']}"
            ),
            types.InlineKeyboardButton(
                "📷 Subir acuse",
                callback_data=f"acuse_{parada['id']}_{ruta_id}"
            ),
        )

        if i < len(paradas):
            markup.add(
                types.InlineKeyboardButton("➡️ Siguiente parada", callback_data=f"parada_{paradas[i]['id']}_{ruta_id}")
            )

        bot.send_message(chat_id, texto, reply_markup=markup)

# ─────────────────────────────────────────────────────────────
# Fotos de acuse
# ─────────────────────────────────────────────────────────────

# Estado temporal para saber qué parada espera foto
_esperando_foto: Dict[int, Dict] = {}   # telegram_id → {parada_id, ruta_id}

@bot.message_handler(content_types=['photo'])
def handle_foto(message):
    telegram_id = message.from_user.id
    rep         = _get_repartidor(telegram_id)

    if not rep:
        bot.reply_to(message, "❌ No estás registrado en el sistema.")
        return

    # Obtener foto de mayor resolución
    foto    = message.photo[-1]
    file_id = foto.file_id

    # Contexto: ¿estaba esperando foto de una parada específica?
    ctx = _esperando_foto.get(telegram_id)

    if ctx:
        parada_id = ctx['parada_id']
        ruta_id   = ctx['ruta_id']
        del _esperando_foto[telegram_id]
    else:
        # Sin contexto — registrar como avance genérico
        ruta = _get_ruta_asignada(telegram_id)
        if not ruta:
            bot.reply_to(message, "⚠️ No tienes ruta asignada. No se puede registrar el acuse.")
            return
        ruta_id   = ruta['id']
        parada_id = None

    # Guardar avance en BD
    try:
        AvanceRepo.create(
            ruta_id       = ruta_id,
            repartidor_id = str(rep['id']),
            persona_id    = None,
            parada_id     = parada_id,
            foto_path     = file_id,    # guardamos file_id de Telegram
            notas         = message.caption or "",
            tipo          = "entrega",
        )

        if parada_id:
            ParadaRepo.cambiar_estado(parada_id, 'visitada')

        bot.reply_to(
            message,
            "✅ *Acuse registrado correctamente*\n\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
            f"Ruta: {ruta_id}",
            reply_markup=_menu_markup()
        )
    except Exception as e:
        logger.error(f"Error guardando acuse: {e}")
        bot.reply_to(message, f"❌ Error al registrar: {e}")

# ─────────────────────────────────────────────────────────────
# Ubicación en tiempo real
# ─────────────────────────────────────────────────────────────

@bot.message_handler(content_types=['location'])
def handle_ubicacion(message):
    lat = message.location.latitude
    lon = message.location.longitude
    telegram_id = message.from_user.id

    # Guardar en BD
    try:
        with db.get_cursor() as cur:
            cur.execute("""
                INSERT INTO ubicaciones_repartidor (telegram_id, lat, lng, timestamp)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (telegram_id) DO UPDATE
                    SET lat = EXCLUDED.lat,
                        lng = EXCLUDED.lng,
                        timestamp = NOW()
            """, (str(telegram_id), lat, lon))
    except Exception:
        pass   # tabla opcional, no bloquear

    maps_url = f"https://www.google.com/maps?q={lat},{lon}"
    markup   = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🗺️ Ver en Maps", url=maps_url),
        types.InlineKeyboardButton("⬅️ Menú",        callback_data="menu"),
    )

    bot.reply_to(
        message,
        f"📍 *Ubicación registrada*\n\n"
        f"Lat: `{lat:.6f}`\n"
        f"Lng: `{lon:.6f}`\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}",
        reply_markup=markup
    )

# ─────────────────────────────────────────────────────────────
# Callbacks de botones inline
# ─────────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id     = call.message.chat.id
    telegram_id = call.from_user.id
    data        = call.data

    bot.answer_callback_query(call.id)

    if data == "menu":
        cmd_start(call.message)

    elif data == "ver_ruta":
        _mostrar_ruta(chat_id, telegram_id)

    elif data == "ver_paradas":
        ruta = _get_ruta_asignada(telegram_id)
        if ruta:
            _mostrar_paradas(chat_id, ruta['id'])
        else:
            bot.send_message(chat_id, "⚠️ No tienes ruta asignada.")

    elif data.startswith("paradas_"):
        ruta_id = int(data.split("_")[1])
        _mostrar_paradas(chat_id, ruta_id)

    elif data.startswith("parada_"):
        _, parada_id, ruta_id = data.split("_")
        # Mostrar parada específica
        ruta = RutaRepo.get_full(int(ruta_id))
        if ruta:
            paradas = ruta.get('paradas', [])
            parada  = next((p for p in paradas if p['id'] == int(parada_id)), None)
            if parada:
                import json as _j
                personas = parada.get('personas', [])
                if isinstance(personas, str):
                    personas = _j.loads(personas)
                texto = (
                    f"⏳ *Parada*\n"
                    f"📍 {parada.get('direccion_original', '')[:100]}\n\n"
                    f"*Personas:*\n"
                )
                for p in personas:
                    est = "✅" if p.get('estado') == 'entregado' else "⏳"
                    texto += f"  {est} {p.get('nombre', '')}\n"
                markup = types.InlineKeyboardMarkup()
                markup.add(
                    types.InlineKeyboardButton("📷 Subir acuse", callback_data=f"acuse_{parada_id}_{ruta_id}"),
                    types.InlineKeyboardButton("✅ Marcar visitada", callback_data=f"visitada_{parada_id}"),
                )
                bot.send_message(chat_id, texto, reply_markup=markup)

    elif data.startswith("visitada_"):
        parada_id = int(data.split("_")[1])
        ParadaRepo.cambiar_estado(parada_id, 'visitada')
        bot.send_message(chat_id, "✅ *Parada marcada como visitada*", reply_markup=_menu_markup())

    elif data.startswith("acuse_"):
        _, parada_id, ruta_id = data.split("_")
        _esperando_foto[telegram_id] = {
            'parada_id': int(parada_id),
            'ruta_id':   int(ruta_id),
        }
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("❌ Cancelar", callback_data="menu"))
        bot.send_message(
            chat_id,
            "📷 *Envía la foto del acuse ahora*\n\n"
            "Puedes agregar una descripción como caption.",
            reply_markup=markup
        )

    elif data == "ubicacion":
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        markup.add(
            types.KeyboardButton("📍 Compartir ubicación", request_location=True)
        )
        bot.send_message(
            chat_id,
            "📍 Presiona el botón para compartir tu ubicación.\n\n"
            "💡 Para ubicación en *tiempo real* (hasta 40 min), "
            "mantén presionado el botón de adjuntar y selecciona 'Ubicación en vivo'.",
            reply_markup=markup
        )

    elif data == "supervisor":
        sup_nombre = os.environ.get("SUPERVISOR_NOMBRE", "Supervisor PJCDMX")
        sup_tel    = os.environ.get("SUPERVISOR_TELEFONO", "")

        texto = (
            f"📞 *CONTACTO SUPERVISOR*\n\n"
            f"👤 {sup_nombre}\n"
        )
        if sup_tel:
            texto += f"📱 `{sup_tel}`\n"
        texto += f"\n🕐 Horario: Lunes a Viernes 8:00–18:00"

        markup = types.InlineKeyboardMarkup()
        if sup_tel:
            markup.add(
                types.InlineKeyboardButton("📞 Llamar",    url=f"tel:{sup_tel}"),
                types.InlineKeyboardButton("💬 WhatsApp",  url=f"https://wa.me/{sup_tel.replace('+','')}"),
            )
        markup.add(types.InlineKeyboardButton("⬅️ Menú", callback_data="menu"))

        bot.send_message(chat_id, texto, reply_markup=markup)

    elif data == "estado":
        ruta = _get_ruta_asignada(telegram_id)
        if not ruta:
            bot.send_message(chat_id, "⚠️ No tienes ruta asignada.", reply_markup=_menu_markup())
            return

        paradas   = ruta.get('paradas', [])
        visitadas = sum(1 for p in paradas if p.get('estado') == 'visitada')
        pendientes = len(paradas) - visitadas

        texto = (
            f"📊 *ESTADO DE TUS ENTREGAS*\n\n"
            f"Ruta: *{ruta['id']} — {ruta['zona']}*\n\n"
            f"✅ Visitadas:  *{visitadas}*\n"
            f"⏳ Pendientes: *{pendientes}*\n"
            f"🏢 Total:      *{len(paradas)}*\n\n"
        )

        if pendientes == 0:
            texto += "🎉 *¡Todas las paradas completadas!*"
        else:
            texto += f"Quedan *{pendientes}* paradas por visitar."

        bot.send_message(chat_id, texto, reply_markup=_menu_markup())

    else:
        bot.send_message(chat_id, "⚠️ Opción no reconocida.", reply_markup=_menu_markup())

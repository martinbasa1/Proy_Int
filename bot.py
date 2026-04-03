import os
import asyncio
import pg8000.native as pg
import google.generativeai as genai
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes

# ─── CONFIGURACIÓN ────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DATABASE_URL   = os.environ.get("DATABASE_URL")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# ─── ESQUEMA DE LA BASE (para que Gemini sepa qué hay) ────────────
SCHEMA = """
Tabla: proyectos
Columnas:
- id (TEXT): código del proyecto, ej: A-002, C-173
- cuenta_analitica (TEXT): nombre largo del proyecto
- tipo_proy (TEXT): tipo de proyecto
- moneda (TEXT): USD, AR$, EUR
- referente (TEXT): nombre del referente técnico
- email_referente (TEXT)
- organismo_aportante (TEXT): FAO, BID, FONTAGRO, PROCISUR, etc.
- pais_origen (TEXT): país del organismo aportante
- fecha_firma (DATE): fecha de firma del acuerdo
- año_firma (INTEGER): año de firma
- fecha_inicio (DATE)
- fecha_fin (DATE)
- plazo_meses (INTEGER)
- fondos_usd (NUMERIC): monto en dólares
- sede_adm (TEXT): sede administrativa
- estado (TEXT): Activo, Cerrado, etc.
- titulo (TEXT): título corto del proyecto
- objeto (TEXT): descripción del objetivo
- referente_adm (TEXT): referente administrativo
- provincia (TEXT): provincia argentina
- ubicacion_inta (TEXT): centro regional INTA
- sede_inta (TEXT)
- categoria (TEXT)
- paises_involucrados (TEXT): países participantes
- sector (TEXT)
- area_tematica (TEXT)
- comentarios (TEXT)
"""

# ─── FUNCIÓN: pregunta → SQL via Gemini ───────────────────────────
def generar_sql(pregunta: str) -> str:
    prompt = f"""Eres un experto en SQL PostgreSQL. 
Tenés acceso a esta base de datos:

{SCHEMA}

El usuario hace esta pregunta en español: "{pregunta}"

Generá SOLO la consulta SQL para responderla. 
- Usá ILIKE para búsquedas de texto (insensible a mayúsculas)
- No uses markdown ni explicaciones, solo el SQL puro
- Si la pregunta no tiene que ver con la base, respondé: NO_SQL
- Limitá los resultados a 20 filas máximo con LIMIT 20
"""
    response = model.generate_content(prompt)
    return response.text.strip()

# ─── FUNCIÓN: ejecutar SQL en Neon ────────────────────────────────
def ejecutar_sql(sql: str) -> list:
    import urllib.parse
    r = urllib.parse.urlparse(DATABASE_URL)
    conn = pg.Connection(
        user=r.username,
        password=r.password,
        host=r.hostname,
        database=r.path[1:],
        ssl_context=True
    )
    result = conn.run(sql)
    columnas = [col["name"] for col in conn.columns]
    conn.close()
    return columnas, result

# ─── FUNCIÓN: resultado → respuesta natural via Gemini ────────────
def formatear_respuesta(pregunta: str, columnas: list, filas: list) -> str:
    if not filas:
        return "No encontré proyectos que coincidan con tu consulta."
    
    # Armar texto con los resultados
    datos = "\n".join([str(dict(zip(columnas, fila))) for fila in filas[:10]])
    
    prompt = f"""El usuario preguntó: "{pregunta}"

Los resultados de la base de datos son:
{datos}

Respondé en español de forma clara y concisa, como si fueras un asistente.
Si hay muchos resultados, hacé un resumen. Máximo 10 items en listas.
No menciones SQL ni bases de datos."""

    response = model.generate_content(prompt)
    return response.text.strip()

# ─── HANDLERS DE TELEGRAM ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "¡Hola! Soy el asistente de proyectos de ArgenINTA 🌱\n\n"
        "Podés preguntarme cosas como:\n"
        "• ¿Cuántos proyectos hay con FAO?\n"
        "• Mostrame los proyectos activos en Patagonia\n"
        "• ¿Cuáles son los proyectos de mayor presupuesto?\n"
        "• ¿Qué proyectos hay en la provincia de Salta?\n\n"
        "¡Preguntame lo que quieras!"
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pregunta = update.message.text
    await update.message.reply_text("🔍 Consultando la base de datos...")
    
    try:
        # 1. Generar SQL
        sql = generar_sql(pregunta)
        
        if sql == "NO_SQL":
            await update.message.reply_text(
                "Esa pregunta no parece estar relacionada con los proyectos. "
                "Preguntame sobre proyectos de ArgenINTA 🌱"
            )
            return
        
        # 2. Ejecutar SQL
        columnas, filas = ejecutar_sql(sql)
        
        # 3. Formatear respuesta
        respuesta = formatear_respuesta(pregunta, columnas, filas)
        await update.message.reply_text(respuesta)
        
    except Exception as e:
        await update.message.reply_text(
            f"Ocurrió un error al procesar tu consulta. Intentá reformular la pregunta."
        )
        print(f"Error: {e}")

# ─── MAIN ─────────────────────────────────────────────────────────
def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("Bot iniciado...")
    app.run_polling()

if __name__ == "__main__":
    main()

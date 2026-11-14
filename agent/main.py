# agent/main.py — Servidor FastAPI + Webhook de WhatsApp
# Generado por AgentKit

"""
Servidor principal del agente.
Funciona con cualquier proveedor (Zernio, Meta) gracias a la capa de providers.
"""

import asyncio
import logging
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse

from agent.brain import generar_respuesta, obtener_mensaje_error
from agent.memory import (
    esta_pausada,
    guardar_mensaje,
    inicializar_db,
    liberar_evento,
    limpiar_eventos_viejos,
    limpiar_todas_las_pausas,
    marcar_evento_procesado,
    obtener_historial,
    pausar_conversacion,
    reanudar_conversacion,
)
from agent.providers import obtener_proveedor
from agent.providers.base import MensajeEntrante

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agentkit")
# En desarrollo queremos el detalle de NUESTRO agente, no el de las librerias.
# Poner el nivel raiz en DEBUG llena la terminal de ruido de aiosqlite y httpx
# y hace imposible leer lo que hizo el agente.
logger.setLevel(logging.DEBUG if ENVIRONMENT == "development" else logging.INFO)

PORT = int(os.getenv("PORT", "8000"))

# ── Notificaciones de lead caliente ──────────────────────────────────────────
# Cuando Camila detecta que un prospecto quiere avanzar (Paso 7 del flujo de ventas),
# manda un WhatsApp a Israel con el resumen del lead.
#
# Requiere un setup de una vez: Israel manda cualquier mensaje desde su celular personal
# al numero de WhatsApp del negocio. El conversation_id queda en los logs de Railway.
# Ese ID se agrega como ISRAEL_CONVERSATION_ID en las variables de entorno de Railway.
ISRAEL_CONVERSATION_ID = os.getenv("ISRAEL_CONVERSATION_ID", "")
ISRAEL_ACCOUNT_ID = os.getenv("ISRAEL_ACCOUNT_ID", "")

# Detecta si Camila acaba de llegar al Paso 7 (lead caliente: pasar a Israel).
# Claude genera frases variadas, asi que combinamos frases exactas conocidas con
# una regla flexible: "israel" + cualquier palabra que indique traspaso o contacto.
_FRASES_EXACTAS_ESCALACION = [
    "pasar tu contacto a israel",
    "paso tu contacto a israel",
    "paso tus datos a israel",
    "datos a israel",
    "contacto a israel",
    "coordinarlo con israel",
    "coordinar con israel",
    "coordinar los detalles y agendar",
    "israel se va a comunicar",
    "israel se comunicara",
    "israel te va a",
    "israel va a contactar",
    "israel para que se comunique",
    "israel para coordinar",
]
_PALABRAS_TRASPASO = ["paso", "pasar", "contactar", "coordin", "comunic", "datos", "reunion", "reunión", "llamar", "hablar"]


def _es_lead_caliente(respuesta: str) -> bool:
    """True si Camila indica que va a pasar el prospecto a Israel (Paso 7 del flujo)."""
    lower = respuesta.lower()
    if any(frase in lower for frase in _FRASES_EXACTAS_ESCALACION):
        return True
    # Regla flexible: "israel" + cualquier palabra de traspaso en la misma respuesta
    if "israel" in lower and any(p in lower for p in _PALABRAS_TRASPASO):
        return True
    return False

# Evitar notificar el mismo numero dos veces en el mismo deploy.
# Si el agente se reinicia (nuevo deploy), puede notificar de nuevo — es aceptable.
_leads_notificados: set[str] = set()

# Un candado por numero de telefono. En WhatsApp es normal que alguien mande "hola" y
# medio segundo despues la pregunta de verdad: sin esto los dos mensajes se procesarian
# en paralelo, los dos leerian el mismo historial y las escrituras quedarian intercaladas.
_candados: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

# Cuando el usuario reactiva manualmente desde el panel, guardamos el momento exacto.
# Al detectar un mensaje humano en el historial de Zernio, comparamos su timestamp con
# este valor: si la reactivacion es mas reciente que el mensaje de Israel, lo ignoramos
# y no volvemos a pausar. Se resetea solo cuando llega un mensaje humano MAS NUEVO.
# Clave "*" = reactivacion global (boton "Reactivar TODAS").
# Se pierde en cada redespliegue — es intencional: si Railway reinicia, el agente arranca
# limpio y detecta el estado real desde Zernio.
_reactivaciones: dict[str, datetime] = {}

# Si la configuracion esta mal, guardamos el error y lo mostramos en el health check,
# en vez de reventar en el import y dejar a Railway reiniciando el contenedor a ciegas.
proveedor = None
error_configuracion: str | None = None
try:
    proveedor = obtener_proveedor()
except Exception as e:  # noqa: BLE001 — cualquier problema de configuracion
    error_configuracion = str(e)

# Resultado del chequeo de credenciales que se hace al arrancar. Se expone en el health
# check: que el servidor conteste no significa que el agente pueda responder por WhatsApp.
estado_proveedor: dict = {"ok": None, "detalle": "sin verificar"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Prepara la base de datos y chequea el proveedor al arrancar."""
    await inicializar_db()
    await limpiar_eventos_viejos()
    logger.info("Base de datos lista")
    logger.info(f"Servidor AgentKit escuchando en el puerto {PORT}")

    global estado_proveedor
    if proveedor is not None:
        logger.info(f"Proveedor de WhatsApp: {proveedor.__class__.__name__}")
        ok, detalle = await proveedor.verificar_conexion()
        estado_proveedor = {"ok": ok, "detalle": detalle}
        logger.info(f"Conexion con el proveedor: {'OK' if ok else 'ERROR'} — {detalle}")
    else:
        logger.error(f"Proveedor de WhatsApp NO configurado: {error_configuracion}")

    yield


app = FastAPI(title="Camila — Agente de Ventas de Carrillo Audiovisual", version="2.0.0", lifespan=lifespan)


@app.get("/")
async def health_check():
    """Endpoint de salud para Railway y monitoreo."""
    if error_configuracion:
        return {"status": "error", "service": "agentkit", "detalle": error_configuracion}

    # Se responde 200 aunque las credenciales esten mal, para que Railway no marque el
    # deploy como caido y puedas leer el diagnostico. El detalle esta en el cuerpo.
    return {
        "status": "ok" if estado_proveedor["ok"] else "degradado",
        "service": "agentkit",
        "agente": "Camila Vilches — Carrillo Audiovisual",
        "proveedor": proveedor.__class__.__name__ if proveedor else None,
        "conexion": estado_proveedor,
    }


@app.get("/panel", response_class=HTMLResponse)
async def panel_control():
    """Panel web para pausar/reactivar Camila desde el celular."""
    return HTMLResponse("""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Camila — Control</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f0f0f;color:#fff;min-height:100vh;padding:28px 18px}
h1{font-size:22px;font-weight:700;margin-bottom:4px}
.sub{color:#666;font-size:14px;margin-bottom:28px}
label{display:block;font-size:13px;color:#999;margin-bottom:8px}
input{width:100%;padding:15px 16px;border-radius:14px;border:1px solid #2a2a2a;background:#181818;color:#fff;font-size:17px;margin-bottom:18px;outline:none;-webkit-appearance:none}
input:focus{border-color:#444}
.fila{display:grid;grid-template-columns:1fr 1fr;gap:11px;margin-bottom:11px}
button{padding:16px;border-radius:14px;border:none;font-size:15px;font-weight:700;cursor:pointer;width:100%;transition:transform .1s}
button:active{transform:scale(.96)}
.pausa{background:#c0392b;color:#fff}
.pausa.larga{background:#9b2335}
.reactivar{background:#1a7a45;color:#fff;grid-column:1/-1}
#res{margin-top:22px;padding:15px 17px;border-radius:14px;font-size:14px;line-height:1.5;display:none}
.ok{background:#0d2117;color:#55d98a;border:1px solid #1a5c37}
.err{background:#200d0d;color:#ff7070;border:1px solid #5c1a1a}
.hint{color:#555;font-size:12px;margin-top:-12px;margin-bottom:18px}
</style>
</head>
<body>
<h1>Camila — Control</h1>
<p class="sub">Pausa o reactiva la IA para un cliente</p>

<label>Teléfono del cliente</label>
<input type="tel" id="tel" placeholder="56912345678" inputmode="numeric">
<p class="hint">Sin +, con código de país. Ej: 56912345678</p>

<div class="fila">
  <button class="pausa" onclick="pausar(2)">Pausar 2h</button>
  <button class="pausa larga" onclick="pausar(8)">Pausar 8h</button>
</div>
<div class="fila">
  <button class="reactivar" onclick="reactivar()">▶ Reactivar IA</button>
</div>
<div class="fila" style="margin-top:8px">
  <button class="reactivar" style="background:#0a4a7a" onclick="reactivarTodo()">▶▶ Reactivar TODAS</button>
</div>

<div id="res"></div>

<script>
function tel(){return document.getElementById('tel').value.trim()}
function mostrar(msg,ok){
  const el=document.getElementById('res');
  el.innerHTML=msg;el.className=ok?'ok':'err';el.style.display='block';
}
async function pausar(h){
  if(!tel()){mostrar('Ingresa el número del cliente','err');return}
  try{
    const r=await fetch('/admin/pausar/'+tel()+'?horas='+h,{method:'POST'});
    const d=await r.json();
    r.ok?mostrar('✓ IA pausada '+h+'h para<br><b>'+tel()+'</b>',true):mostrar('Error: '+JSON.stringify(d),false);
  }catch(e){mostrar('Error de red — ¿está el servidor activo?',false)}
}
async function reactivar(){
  if(!tel()){mostrar('Ingresa el número del cliente',false);return}
  try{
    const r=await fetch('/admin/reanudar/'+tel(),{method:'POST'});
    const d=await r.json();
    r.ok?mostrar('✓ IA reactivada para<br><b>'+tel()+'</b>',true):mostrar('Error: '+JSON.stringify(d),false);
  }catch(e){mostrar('Error de red — ¿está el servidor activo?',false)}
}
async function reactivarTodo(){
  try{
    const r=await fetch('/admin/reanudar-todo',{method:'POST'});
    const d=await r.json();
    r.ok?mostrar('✓ IA reactivada para todos los clientes<br>('+d.pausas_eliminadas+' pausas eliminadas)',true):mostrar('Error: '+JSON.stringify(d),false);
  }catch(e){mostrar('Error de red — ¿está el servidor activo?',false)}
}
</script>
</body>
</html>""")


@app.post("/admin/pausar/{telefono}")
async def admin_pausar(telefono: str, horas: int = 2):
    """Pausa la IA para un numero. Util cuando un humano toma el control manualmente."""
    await pausar_conversacion(f"phone:{telefono}", horas=horas)
    return {"status": "ok", "telefono": telefono, "pausado_por_horas": horas}


@app.post("/admin/reanudar/{telefono}")
async def admin_reanudar(telefono: str):
    """Reactiva la IA para un numero inmediatamente."""
    await reanudar_conversacion(f"phone:{telefono}")
    # Guardamos CUANDO se reactivo: si detectar_control_humano encuentra el mensaje de
    # Israel en el historial, comparamos su timestamp con este — si la reactivacion es
    # mas reciente, ignoramos la deteccion y no volvemos a pausar.
    _reactivaciones[telefono] = datetime.now(timezone.utc)
    logger.info(f"Reactivacion manual registrada para {telefono}")
    return {"status": "ok", "telefono": telefono, "estado": "reactivada"}


@app.post("/admin/reanudar-todo")
async def admin_reanudar_todo():
    """Elimina TODAS las pausas activas. Reactiva la IA para todos los clientes."""
    eliminadas = await limpiar_todas_las_pausas()
    # El comodin "*" aplica a cualquier telefono que no tenga reactivacion propia.
    _reactivaciones["*"] = datetime.now(timezone.utc)
    logger.info("Reactivacion global registrada (todos los telefonos)")
    return {"status": "ok", "pausas_eliminadas": eliminadas}


@app.get("/webhook")
async def webhook_verificacion(request: Request):
    """Verificacion GET del webhook. La pide Meta; para Zernio no hace nada."""
    if proveedor is None:
        raise HTTPException(status_code=503, detail=error_configuracion or "Proveedor no configurado")

    respuesta = await proveedor.validar_webhook(request)
    if respuesta is not None:
        return PlainTextResponse(respuesta)

    # Meta pide un 403 cuando manda hub.mode=subscribe y el verify_token no coincide.
    # Devolverle 200 le hace creer que la URL quedo verificada cuando no es cierto.
    if request.query_params.get("hub.mode") == "subscribe":
        raise HTTPException(status_code=403, detail="Verify token incorrecto")

    return {"status": "ok"}


@app.post("/webhook")
async def webhook_handler(request: Request, tareas: BackgroundTasks):
    """
    Recibe los mensajes de WhatsApp.

    Contesta 200 de inmediato y procesa el mensaje en segundo plano.

    Esto NO es un detalle de estilo. Los proveedores esperan un 2xx en unos 5 segundos y,
    si no lo reciben, reintentan el mismo evento hasta 7 veces. Como llamar a Claude tarda
    mas que eso, procesar antes de contestar hace que el cliente reciba la misma respuesta
    repetida. Por eso: responder primero, trabajar despues.
    """
    if proveedor is None:
        raise HTTPException(status_code=503, detail=error_configuracion or "Proveedor no configurado")

    if not await proveedor.verificar_firma(request):
        raise HTTPException(status_code=401, detail="Firma del webhook invalida")

    try:
        mensajes = await proveedor.parsear_webhook(request)
    except Exception as e:  # noqa: BLE001
        # Un payload raro no debe hacer que el proveedor reintente para siempre
        logger.error(f"No se pudo leer el webhook: {e}")
        return {"status": "ignorado"}

    encolados = 0
    for msg in mensajes:
        if msg.es_propio:
            # Israel respondio manualmente desde el inbox de Zernio: pausar la IA 2 horas.
            # Usamos conversation_id como clave porque en mensajes salientes el "sender"
            # es el negocio, no el cliente — el telefono aqui seria el numero del negocio.
            conversation_id = msg.contexto.get("conversation_id", "")
            if conversation_id:
                await pausar_conversacion(f"conv:{conversation_id}", horas=2)
            elif msg.telefono:
                await pausar_conversacion(f"phone:{msg.telefono}", horas=2)
            continue

        if not msg.texto.strip():
            continue

        # Si un humano tomo el control, la IA no interrumpe.
        # Revisamos por telefono (pausa manual) Y por conversation_id (pausa automatica).
        conversation_id = msg.contexto.get("conversation_id", "")
        if await esta_pausada(msg.telefono, conversation_id):
            logger.info(f"IA pausada para {msg.telefono} (conv: {conversation_id}): mensaje ignorado")
            continue

        # La entrega es "al menos una vez": el mismo evento puede llegar dos veces
        evento_id = msg.contexto.get("evento_id") or msg.mensaje_id
        if evento_id and not await marcar_evento_procesado(evento_id):
            logger.info(f"Evento repetido, se ignora: {evento_id}")
            continue

        logger.info(f"Mensaje de {msg.telefono}: {msg.texto}")
        tareas.add_task(procesar_mensaje, msg)
        encolados += 1

    return {"status": "ok", "encolados": encolados}


async def notificar_lead_caliente(telefono: str, historial: list[dict], mensaje_actual: str):
    """
    Manda un WhatsApp a Israel cuando Camila detecta un lead listo para cerrar.

    Construye un resumen con los ultimos mensajes de la conversacion para que Israel
    tenga contexto antes de llamar al prospecto.
    """
    if not ISRAEL_CONVERSATION_ID:
        logger.warning(
            "ISRAEL_CONVERSATION_ID no esta configurado — no se puede notificar el lead. "
            "Ver instrucciones de setup en el README o en el comentario de esta variable."
        )
        return

    if telefono in _leads_notificados:
        logger.info(f"Lead {telefono} ya fue notificado en este deploy, se omite")
        return

    # Ultimos 4 intercambios (8 mensajes) para el resumen
    ultimos = historial[-8:] if len(historial) >= 8 else historial
    lineas = []
    for m in ultimos:
        rol = "Cliente" if m["role"] == "user" else "Camila"
        texto = m["content"][:120].replace("\n", " ")
        lineas.append(f"{rol}: {texto}")
    # Agregar el mensaje actual que disparo el Paso 7
    lineas.append(f"Cliente: {mensaje_actual[:120]}")
    resumen = "\n".join(lineas)

    texto_notificacion = (
        f"🔥 *Lead caliente*\n\n"
        f"📱 +{telefono}\n"
        f"💬 Quiere avanzar — acaba de pedir propuesta o reunión\n\n"
        f"*Últimos mensajes:*\n{resumen}\n\n"
        f"Respóndele pronto 👆"
    )

    account_id = ISRAEL_ACCOUNT_ID or os.getenv("ZERNIO_ACCOUNT_ID", "")
    contexto_israel = {
        "conversation_id": ISRAEL_CONVERSATION_ID,
        "account_id": account_id,
        # Sin evento_id: no queremos deduplicar estas notificaciones entre deploys
    }

    if proveedor is None:
        return

    enviado = await proveedor.enviar_mensaje("", texto_notificacion, contexto_israel)
    if enviado:
        _leads_notificados.add(telefono)
        logger.info(f"Notificacion de lead caliente enviada a Israel: {telefono}")
    else:
        logger.error(f"No se pudo enviar notificacion de lead para {telefono}")


async def procesar_mensaje(msg: MensajeEntrante):
    """
    Genera la respuesta y la manda de vuelta. Corre fuera del ciclo del webhook.

    Se toma un candado por telefono: dos mensajes seguidos del mismo cliente se
    atienden en orden, no en paralelo, para que el historial no se mezcle.
    """
    evento_id = msg.contexto.get("evento_id") or msg.mensaje_id
    conversation_id = msg.contexto.get("conversation_id", "")

    async with _candados[msg.telefono]:
        try:
            # Deteccion automatica: si Israel respondio desde su app de WhatsApp Business,
            # la IA se pausa sola. Se consulta el historial de Zernio y se revisa el
            # sentVia del ultimo mensaje saliente. Si es null = app nativa = humano al mando.
            # Esto corre en background (ya respondimos 200), asi que el tiempo extra no
            # afecta el SLA del webhook.
            account_id = msg.contexto.get("account_id", "")
            if conversation_id and hasattr(proveedor, "detectar_control_humano"):
                momento_humano = await proveedor.detectar_control_humano(conversation_id, account_id)
                if momento_humano is not None:
                    # Hay un mensaje humano en el historial. Verificamos si hubo una
                    # reactivacion manual MAS RECIENTE que ese mensaje.
                    reactivada_en = _reactivaciones.get(msg.telefono) or _reactivaciones.get("*")
                    if reactivada_en and reactivada_en > momento_humano:
                        # El usuario ya reactivo despues de ese mensaje → ignorar deteccion.
                        logger.info(
                            f"Reactivacion manual ({reactivada_en.strftime('%H:%M:%S')} UTC) "
                            f"mas reciente que mensaje humano ({momento_humano.strftime('%H:%M:%S')} UTC): "
                            f"ignorando deteccion para {msg.telefono}"
                        )
                    else:
                        # El mensaje humano es mas nuevo que la ultima reactivacion → pausar.
                        # Limpiamos la reactivacion vieja para que no afecte otras detecciones.
                        _reactivaciones.pop(msg.telefono, None)
                        if reactivada_en:
                            _reactivaciones.pop("*", None)
                        # Usamos phone: (no conv:) para que /admin/reanudar y el /panel funcionen
                        await pausar_conversacion(f"phone:{msg.telefono}", horas=2)
                        logger.info(
                            f"IA pausada automaticamente para {msg.telefono} "
                            f"(conv: {conversation_id}): Israel respondio desde la app de WA"
                        )
                        return

            # El historial se lee ANTES de guardar el mensaje actual: brain.py agrega
            # el mensaje nuevo al final, y asi no queda duplicado.
            historial = await obtener_historial(msg.telefono)
            respuesta, es_respuesta_real = await generar_respuesta(msg.texto, historial)

            enviado = await proveedor.enviar_mensaje(msg.telefono, respuesta, msg.contexto)

            if not enviado:
                # El evento se marco como procesado ANTES de llegar hasta aca, para que dos
                # entregas simultaneas no se dupliquen. Si el envio fallo, hay que soltarlo:
                # si no, el reintento del proveedor se descartaria por duplicado y el cliente
                # se quedaria sin respuesta para siempre.
                logger.error(f"No se pudo enviar la respuesta a {msg.telefono}; se libera el evento")
                await liberar_evento(evento_id)
                return

            # Solo se guarda en el historial lo que de verdad es conversacion. Los avisos
            # tecnicos ("estoy teniendo problemas") no son un turno del agente: guardarlos
            # los deja contaminando el contexto de todos los mensajes que vengan despues.
            if es_respuesta_real:
                await guardar_mensaje(msg.telefono, "user", msg.texto)
                await guardar_mensaje(msg.telefono, "assistant", respuesta)

                # Detectar si Camila acaba de llegar al Paso 7 del flujo (lead caliente).
                if _es_lead_caliente(respuesta):
                    await notificar_lead_caliente(msg.telefono, historial, msg.texto)

            logger.info(f"Respuesta enviada a {msg.telefono}: {respuesta}")

        except Exception as e:  # noqa: BLE001
            logger.exception(f"Error procesando el mensaje de {msg.telefono}: {e}")
            await liberar_evento(evento_id)
            try:
                await proveedor.enviar_mensaje(msg.telefono, obtener_mensaje_error(), msg.contexto)
            except Exception:  # noqa: BLE001
                logger.error("Tampoco se pudo avisarle al cliente del error")

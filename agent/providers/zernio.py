# agent/providers/zernio.py — Adaptador para Zernio
# Generado por AgentKit

"""
Zernio corre sobre la WhatsApp Cloud API de Meta: resuelve la conexion de la WhatsApp
Business Account, el inbox unificado y los webhooks firmados.

Documentacion: https://docs.zernio.com/platforms/whatsapp
"""

import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone

import httpx
from fastapi import Request

from agent.providers.base import MensajeEntrante, ProveedorWhatsApp

logger = logging.getLogger("agentkit")

BASE_URL_POR_DEFECTO = "https://zernio.com/api/v1"


class ProveedorZernio(ProveedorWhatsApp):
    """Proveedor de WhatsApp usando Zernio."""

    def __init__(self):
        self.api_key = os.getenv("ZERNIO_API_KEY", "")
        self.webhook_secret = os.getenv("ZERNIO_WEBHOOK_SECRET", "")
        # Opcional: solo se usa para el chequeo de conexion al arrancar.
        # Para responder, el account_id sale del propio webhook.
        self.account_id = os.getenv("ZERNIO_ACCOUNT_ID", "")
        # Ojo con el "or": os.getenv(clave, default) solo usa el default si la clave NO
        # existe. Como el .env trae "ZERNIO_BASE_URL=" vacia, os.getenv devuelve "" y el
        # default nunca se aplicaria. El "or" cubre los dos casos.
        self.base_url = (os.getenv("ZERNIO_BASE_URL") or BASE_URL_POR_DEFECTO).rstrip("/")

        if not self.api_key:
            logger.warning("ZERNIO_API_KEY no esta configurada: el agente no va a poder responder")
        if not self.webhook_secret:
            logger.warning(
                "ZERNIO_WEBHOOK_SECRET no esta configurado: los webhooks NO se verifican. "
                "Sirve para probar, pero no lo dejes asi en produccion."
            )

    # ── Recibir ──────────────────────────────────────────────────────────

    async def verificar_firma(self, request: Request) -> bool:
        """Compara el header X-Zernio-Signature contra el HMAC-SHA256 del cuerpo crudo."""
        if not self.webhook_secret:
            return True  # modo pruebas, ya se advirtio al arrancar

        firma_recibida = request.headers.get("X-Zernio-Signature") or request.headers.get(
            "X-Late-Signature"
        )
        if not firma_recibida:
            logger.warning("Llego un webhook sin firma X-Zernio-Signature: rechazado")
            return False

        cuerpo = await request.body()
        firma_esperada = hmac.new(
            self.webhook_secret.encode("utf-8"), cuerpo, hashlib.sha256
        ).hexdigest()

        # compare_digest sobre str exige ASCII puro. Los headers HTTP pueden traer bytes
        # que Starlette decodifica como latin-1, y ahi tiraria TypeError: eso escaparia del
        # handler como un 500, y el proveedor reintentaria el evento siete veces.
        try:
            iguales = hmac.compare_digest(firma_esperada, firma_recibida)
        except TypeError:
            logger.warning("La firma del webhook trae caracteres invalidos: rechazado")
            return False

        if not iguales:
            logger.warning("Firma de webhook invalida: rechazado")
            return False
        return True

    async def parsear_webhook(self, request: Request) -> list[MensajeEntrante]:
        """Normaliza el evento message.received de Zernio."""
        payload = await request.json()

        evento = payload.get("event")
        if evento != "message.received":
            # message.sent, message.delivered, message.read, etc. no se contestan
            logger.debug(f"Evento ignorado: {evento}")
            return []

        mensaje = payload.get("message") or {}
        if mensaje.get("platform") != "whatsapp":
            return []

        remitente = mensaje.get("sender") or {}
        # phoneNumber viene en E.164 con "+". Desde abril de 2026 puede faltar
        # (los usuarios con username de WhatsApp no exponen su numero), asi que
        # caemos al identificador que Meta si garantiza.
        telefono = (remitente.get("phoneNumber") or "").lstrip("+")
        if not telefono:
            telefono = remitente.get("businessScopedUserId") or remitente.get("id") or ""

        cuenta = payload.get("account") or {}

        return [
            MensajeEntrante(
                telefono=telefono,
                texto=mensaje.get("text") or "",
                mensaje_id=mensaje.get("platformMessageId") or mensaje.get("id") or "",
                # Zernio marca la direccion: solo contestamos lo que entra
                es_propio=mensaje.get("direction") != "incoming",
                contexto={
                    "evento_id": payload.get("id", ""),
                    "conversation_id": mensaje.get("conversationId", ""),
                    "account_id": cuenta.get("id", ""),
                },
            )
        ]

    # ── Enviar ───────────────────────────────────────────────────────────

    async def enviar_mensaje(
        self, telefono: str, mensaje: str, contexto: dict | None = None
    ) -> bool:
        """Responde dentro de la conversacion que abrio el cliente."""
        contexto = contexto or {}
        conversation_id = contexto.get("conversation_id")
        account_id = contexto.get("account_id") or self.account_id

        if not self.api_key:
            logger.error("No se puede enviar: falta ZERNIO_API_KEY")
            return False
        if not conversation_id or not account_id:
            logger.error(
                "No se puede enviar: el mensaje no trae conversation_id o account_id"
            )
            return False

        url = f"{self.base_url}/inbox/conversations/{conversation_id}/messages"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        # Si el webhook se reintenta, esta clave evita que Zernio mande la respuesta dos veces
        evento_id = contexto.get("evento_id")
        if evento_id:
            headers["Idempotency-Key"] = f"agentkit-{evento_id}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as cliente:
                r = await cliente.post(
                    url,
                    json={"accountId": account_id, "message": mensaje},
                    headers=headers,
                )
        except httpx.HTTPError as e:
            logger.error(f"Error de red hablando con Zernio: {e}")
            return False

        if r.status_code == 200:
            return True

        # Zernio responde {"error": ..., "type": ..., "code": ...}.
        # Loguear los tres ahorra muchisimo tiempo de diagnostico.
        detalle = r.text[:500]
        try:
            cuerpo = r.json()
            detalle = (
                f"{cuerpo.get('error')} "
                f"(type={cuerpo.get('type')}, code={cuerpo.get('code')})"
            )
        except ValueError:
            pass
        logger.error(f"Zernio rechazo el envio [{r.status_code}]: {detalle}")
        return False

    # ── Deteccion de control humano ──────────────────────────────────────

    async def detectar_control_humano(
        self, conversation_id: str, account_id: str = ""
    ) -> "datetime | None":
        """
        Consulta los mensajes recientes y detecta si el ultimo saliente fue humano.

        Retorna el datetime (UTC) del mensaje humano si se detecta control humano,
        o None si Camila fue la ultima en responder / no hay mensajes / falla la consulta.

        Retornar el datetime (en lugar de bool) permite que procesar_mensaje compare
        si hubo una reactivacion manual MAS RECIENTE que ese mensaje — y en ese caso
        ignore la deteccion sin volver a pausar.

        sentVia vocabulario:
          "api"  → Camila respondio via la API de Zernio
          null   → mensaje enviado desde la app nativa de WhatsApp Business (Israel)
          "human"→ operador respondio desde el inbox web de Zernio
        """
        if not self.api_key or not conversation_id:
            return None

        # accountId es REQUERIDO por la API de Zernio — sin el retorna 400
        cuenta = account_id or self.account_id
        if not cuenta:
            logger.warning(
                f"detectar_control_humano: sin account_id para conv {conversation_id}, "
                f"se omite la verificacion"
            )
            return None

        try:
            async with httpx.AsyncClient(timeout=10.0) as cliente:
                r = await cliente.get(
                    f"{self.base_url}/inbox/conversations/{conversation_id}/messages",
                    params={"accountId": cuenta, "sortOrder": "desc", "limit": "10"},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
        except httpx.HTTPError as e:
            logger.warning(f"No se pudo consultar historial de conv {conversation_id}: {e}")
            return None

        if r.status_code != 200:
            logger.warning(
                f"Zernio respondio {r.status_code} al consultar historial "
                f"de conv {conversation_id}: {r.text[:300]}"
            )
            return None

        try:
            body = r.json()
        except ValueError:
            return None

        # El endpoint devuelve los mensajes en body["messages"] (confirmado por spec)
        mensajes = body.get("messages") or []
        logger.debug(
            f"Historial conv {conversation_id}: {len(mensajes)} mensajes, "
            f"sortOrderApplied={body.get('sortOrderApplied')}"
        )

        for msg in mensajes:
            if msg.get("direction") != "outgoing":
                continue

            sent_via = msg.get("sentVia")
            logger.debug(f"Ultimo saliente en conv {conversation_id}: sentVia={sent_via!r}")

            if sent_via is None or sent_via == "human":
                # App nativa de WA Business (null) u operador de Zernio ("human")
                created_at_str = msg.get("createdAt") or ""
                try:
                    momento = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    # Si no podemos parsear la fecha, usamos ahora: pesimista pero seguro.
                    momento = datetime.now(timezone.utc)
                logger.info(
                    f"Control humano detectado (sentVia={sent_via!r}) "
                    f"en conv {conversation_id}: {momento.isoformat()}"
                )
                return momento

            if sent_via == "api":
                # Camila fue la ultima en responder — continuar normalmente
                return None

            # "broadcast", "sequence", "workflow", etc. son automatizaciones de Zernio,
            # no un humano. Seguimos buscando el siguiente mensaje saliente.

        return None

    # ── Diagnostico ──────────────────────────────────────────────────────

    async def verificar_conexion(self) -> tuple[bool, str]:
        """
        Pregunta a Zernio el estado real del numero contra Meta.
        Es la via soportada para saber si el canal esta vivo.
        """
        if not self.api_key:
            return False, "Falta ZERNIO_API_KEY"
        if not self.account_id:
            return True, "ZERNIO_ACCOUNT_ID no configurado: se omite el chequeo del numero"

        try:
            async with httpx.AsyncClient(timeout=15.0) as cliente:
                r = await cliente.get(
                    f"{self.base_url}/whatsapp/number-info",
                    params={"accountId": self.account_id},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
        except httpx.HTTPError as e:
            return False, f"No se pudo contactar a Zernio: {e}"

        if r.status_code != 200:
            return False, f"Zernio respondio {r.status_code}: {r.text[:200]}"

        telefono = r.json().get("phone") or {}
        return True, (
            f"Numero {telefono.get('display_phone_number', '?')} conectado "
            f"(calidad: {telefono.get('quality_rating', '?')})"
        )

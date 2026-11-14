# agent/memory.py — Memoria de conversaciones
# Generado por AgentKit

"""
Guarda el historial de cada conversacion por numero de telefono, y lleva registro de
que eventos de webhook ya se atendieron.

SQLite en local, PostgreSQL en produccion.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import DateTime, Integer, String, Text, delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv()
logger = logging.getLogger("agentkit")

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./agentkit.db")

# Railway entrega la URL de PostgreSQL con el esquema "postgresql://" (o "postgres://").
# SQLAlchemy en modo asincrono necesita que el driver sea explicito.
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# En produccion, SQLite vive dentro del contenedor y el disco del contenedor es efimero:
# cada redespliegue borra el historial de todas las conversaciones. Avisarlo fuerte, porque
# el agente arranca igual y el problema recien se nota cuando un cliente vuelve a escribir.
if DATABASE_URL.startswith("sqlite") and os.getenv("ENVIRONMENT") == "production":
    logger.warning(
        "Estas en produccion con SQLite. El historial se va a borrar en cada redespliegue. "
        "Agrega PostgreSQL y configura DATABASE_URL para que el agente recuerde a sus clientes."
    )

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def ahora() -> datetime:
    """Hora actual en UTC, con zona horaria."""
    return datetime.now(timezone.utc)


def en_utc(momento: datetime) -> datetime:
    """
    Devuelve la fecha con zona horaria UTC.

    PostgreSQL devuelve las fechas con zona horaria, pero SQLite las devuelve sin ella,
    y Python no deja comparar una fecha con zona contra una sin zona (TypeError).
    Todo lo que se guarda sale de ahora(), asi que una fecha sin zona es UTC.
    """
    if momento.tzinfo is None:
        return momento.replace(tzinfo=timezone.utc)
    return momento


class Base(DeclarativeBase):
    pass


class Mensaje(Base):
    """Un mensaje del historial de conversacion."""

    __tablename__ = "mensajes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telefono: Mapped[str] = mapped_column(String(50), index=True)
    role: Mapped[str] = mapped_column(String(20))  # "user" o "assistant"
    content: Mapped[str] = mapped_column(Text)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=ahora)


class EventoProcesado(Base):
    """
    Eventos de webhook que ya se atendieron.

    Los proveedores entregan "al menos una vez": el mismo evento puede llegar dos veces.
    Sin esta tabla, el cliente recibiria la misma respuesta repetida.
    """

    __tablename__ = "eventos_procesados"

    evento_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    creado_en: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=ahora, index=True)


class Seguimiento(Base):
    """
    Rastrea conversaciones activas para el seguimiento automatico.

    Cuando un cliente escribe y Camila responde, se registra su actividad.
    Si no vuelve a escribir en X horas, Camila le manda un recordatorio.
    Maximo 2 seguimientos por conversacion, dentro de la ventana de 24 horas.
    """

    __tablename__ = "seguimientos"

    telefono: Mapped[str] = mapped_column(String(50), primary_key=True)
    conversation_id: Mapped[str] = mapped_column(String(200), default="")
    account_id: Mapped[str] = mapped_column(String(200), default="")
    ultimo_mensaje_cliente: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=ahora)
    seguimientos_enviados: Mapped[int] = mapped_column(Integer, default=0)
    ultimo_seguimiento: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    activo: Mapped[bool] = mapped_column(default=True)


class ConversacionPausada(Base):
    """
    Conversaciones donde la IA esta pausada porque un humano tomo el control.

    Se activa automaticamente cuando el agente detecta un mensaje saliente (Israel
    respondio desde el inbox de Zernio) y se desactiva al vencer el tiempo.

    clave puede ser:
      "phone:<telefono>"  — pausa manual via admin endpoint
      "conv:<conv_id>"    — pausa automatica al detectar mensaje saliente de Zernio

    Se usa una tabla nueva (v2) para no chocar con el esquema anterior.
    """

    __tablename__ = "conversaciones_pausadas_v2"

    clave: Mapped[str] = mapped_column(String(250), primary_key=True)
    pausada_hasta: Mapped[datetime] = mapped_column(DateTime(timezone=True))


async def inicializar_db():
    """Crea las tablas si no existen."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def marcar_evento_procesado(evento_id: str) -> bool:
    """
    Registra un evento. Retorna True si es nuevo, False si ya se habia procesado.

    La unicidad la garantiza la base de datos (clave primaria), no una consulta previa:
    asi dos webhooks que llegan al mismo tiempo no pasan los dos.
    """
    if not evento_id:
        return True  # sin id no podemos deduplicar: se procesa

    async with async_session() as session:
        session.add(EventoProcesado(evento_id=evento_id, creado_en=ahora()))
        try:
            await session.commit()
            return True
        except IntegrityError:
            await session.rollback()
            return False


async def liberar_evento(evento_id: str):
    """
    Borra la marca de un evento para que el reintento del proveedor SI se procese.

    Se usa cuando el mensaje se marco como procesado pero despues fallo el envio de la
    respuesta. Sin esto, el reintento se descartaria por duplicado y el cliente se
    quedaria sin respuesta para siempre.
    """
    if not evento_id:
        return
    async with async_session() as session:
        await session.execute(delete(EventoProcesado).where(EventoProcesado.evento_id == evento_id))
        await session.commit()


async def limpiar_eventos_viejos(dias: int = 7):
    """Borra los eventos de hace mas de N dias para que la tabla no crezca sin fin."""
    limite = ahora() - timedelta(days=dias)
    async with async_session() as session:
        resultado = await session.execute(
            delete(EventoProcesado).where(EventoProcesado.creado_en < limite)
        )
        await session.commit()
    if resultado.rowcount:
        logger.info(f"Se limpiaron {resultado.rowcount} eventos de mas de {dias} dias")


async def guardar_mensaje(telefono: str, role: str, content: str):
    """Guarda un mensaje en el historial de esa conversacion."""
    async with async_session() as session:
        session.add(Mensaje(telefono=telefono, role=role, content=content, timestamp=ahora()))
        await session.commit()


async def obtener_historial(telefono: str, limite: int = 20) -> list[dict]:
    """
    Devuelve los ultimos N mensajes de una conversacion, en orden cronologico.

    Se ordena por id y no por timestamp: dos mensajes guardados en el mismo instante
    tienen el mismo timestamp, y el orden entre ellos quedaria librado al azar.
    """
    async with async_session() as session:
        resultado = await session.execute(
            select(Mensaje)
            .where(Mensaje.telefono == telefono)
            .order_by(Mensaje.id.desc())
            .limit(limite)
        )
        mensajes = list(resultado.scalars().all())

    mensajes.reverse()  # vienen del mas nuevo al mas viejo: los damos vuelta

    # La API de Claude exige que el historial empiece con un mensaje del usuario.
    # Si por un error anterior quedo un "assistant" suelto al principio, lo sacamos.
    while mensajes and mensajes[0].role != "user":
        mensajes.pop(0)

    return [{"role": m.role, "content": m.content} for m in mensajes]


async def limpiar_historial(telefono: str):
    """Borra todo el historial de una conversacion."""
    async with async_session() as session:
        await session.execute(delete(Mensaje).where(Mensaje.telefono == telefono))
        await session.commit()


async def pausar_conversacion(clave: str, horas: int = 2):
    """
    Pausa las respuestas automaticas para una clave por N horas.

    clave debe ser "phone:<telefono>" o "conv:<conversation_id>".
    """
    hasta = ahora() + timedelta(hours=horas)
    async with async_session() as session:
        pausa = await session.get(ConversacionPausada, clave)
        if pausa:
            pausa.pausada_hasta = hasta
        else:
            session.add(ConversacionPausada(clave=clave, pausada_hasta=hasta))
        await session.commit()
    logger.info(f"IA pausada para {clave} por {horas}h (hasta las {hasta.strftime('%H:%M')} UTC)")


async def esta_pausada(telefono: str, conversation_id: str = "") -> bool:
    """
    True si la IA esta pausada para este numero o conversacion.

    Revisa dos claves: "phone:<telefono>" y "conv:<conversation_id>" (si se pasa).
    """
    claves = [f"phone:{telefono}"]
    if conversation_id:
        claves.append(f"conv:{conversation_id}")

    async with async_session() as session:
        for clave in claves:
            pausa = await session.get(ConversacionPausada, clave)
            if not pausa:
                continue
            if ahora() > en_utc(pausa.pausada_hasta):
                await session.delete(pausa)
                await session.commit()
                continue
            return True
    return False


async def limpiar_todas_las_pausas() -> int:
    """Elimina todas las pausas activas. Retorna cuantas se borraron."""
    async with async_session() as session:
        resultado = await session.execute(delete(ConversacionPausada))
        await session.commit()
    n = resultado.rowcount
    if n:
        logger.info(f"Se eliminaron {n} pausas activas")
    return n


async def registrar_actividad_cliente(telefono: str, conversation_id: str, account_id: str):
    """
    Registra o actualiza la actividad de un cliente.

    Se llama cuando el cliente escribe y Camila respondio correctamente.
    Si ya existe un registro, resetea el contador (el cliente respondio — empezar de nuevo).
    """
    async with async_session() as session:
        seg = await session.get(Seguimiento, telefono)
        if seg:
            seg.ultimo_mensaje_cliente = ahora()
            seg.seguimientos_enviados = 0
            seg.ultimo_seguimiento = None
            seg.activo = True
            seg.conversation_id = conversation_id
            seg.account_id = account_id
        else:
            session.add(Seguimiento(
                telefono=telefono,
                conversation_id=conversation_id,
                account_id=account_id,
                ultimo_mensaje_cliente=ahora(),
                seguimientos_enviados=0,
                activo=True,
            ))
        await session.commit()


async def obtener_seguimientos_pendientes(horas_1: int = 3, horas_2: int = 6) -> list[dict]:
    """
    Retorna las conversaciones que necesitan seguimiento ahora.

    - Primer seguimiento: >= horas_1 desde el ultimo mensaje del cliente, sin enviar ninguno
    - Segundo seguimiento: >= horas_2 desde el primer seguimiento enviado
    - En ambos casos: dentro de la ventana de 24 horas (texto libre de WhatsApp)
    """
    ahora_dt = ahora()
    limite_24h = ahora_dt - timedelta(hours=24)

    async with async_session() as session:
        resultado = await session.execute(
            select(Seguimiento).where(Seguimiento.activo == True)  # noqa: E712
        )
        todos = list(resultado.scalars().all())

    pendientes = []
    for seg in todos:
        ultimo_cliente = en_utc(seg.ultimo_mensaje_cliente)

        # Fuera de la ventana de 24 horas: no se puede mandar texto libre
        if ultimo_cliente < limite_24h:
            continue

        tiempo_sin_respuesta = ahora_dt - ultimo_cliente

        if seg.seguimientos_enviados == 0 and tiempo_sin_respuesta >= timedelta(hours=horas_1):
            pendientes.append({
                "telefono": seg.telefono,
                "conversation_id": seg.conversation_id,
                "account_id": seg.account_id,
                "numero": 1,
            })
        elif seg.seguimientos_enviados == 1 and seg.ultimo_seguimiento:
            tiempo_desde_seguimiento = ahora_dt - en_utc(seg.ultimo_seguimiento)
            if tiempo_desde_seguimiento >= timedelta(hours=horas_2):
                pendientes.append({
                    "telefono": seg.telefono,
                    "conversation_id": seg.conversation_id,
                    "account_id": seg.account_id,
                    "numero": 2,
                })

    return pendientes


async def marcar_seguimiento_enviado(telefono: str):
    """Incrementa el contador y registra la hora. Si llego al maximo, desactiva."""
    async with async_session() as session:
        seg = await session.get(Seguimiento, telefono)
        if seg:
            seg.seguimientos_enviados += 1
            seg.ultimo_seguimiento = ahora()
            if seg.seguimientos_enviados >= 2:
                seg.activo = False
            await session.commit()


async def desactivar_seguimiento(telefono: str):
    """Apaga el seguimiento de un numero: el cliente cerro la conversacion o un humano tomo el control."""
    async with async_session() as session:
        seg = await session.get(Seguimiento, telefono)
        if seg and seg.activo:
            seg.activo = False
            await session.commit()


async def reanudar_conversacion(clave: str):
    """
    Reactiva la IA para una clave inmediatamente.

    clave puede ser "phone:<telefono>" o "conv:<conversation_id>".
    """
    async with async_session() as session:
        pausa = await session.get(ConversacionPausada, clave)
        if pausa:
            await session.delete(pausa)
            await session.commit()
    logger.info(f"IA reactivada para {clave}")

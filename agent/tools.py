# agent/tools.py — Herramientas del agente
# Generado por AgentKit — Carrillo Audiovisual

"""
Herramientas especificas del negocio.

OJO: estas funciones NO se ejecutan solas todavia. La informacion del negocio le llega
a Camila por el system prompt (config/prompts.yaml), asi que para CONTESTAR preguntas
no hace falta nada de aca. Este archivo es el lugar para las ACCIONES — y conectarlas
al ciclo de tool use de Claude es un paso aparte.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from ruamel.yaml import YAML

logger = logging.getLogger("agentkit")

CARPETA_KNOWLEDGE = Path("knowledge")

_yaml = YAML()


def cargar_info_negocio() -> dict:
    """Carga la informacion del negocio desde config/business.yaml."""
    try:
        with open("config/business.yaml", "r", encoding="utf-8") as f:
            resultado = _yaml.load(f)
            return dict(resultado) if resultado else {}
    except FileNotFoundError:
        logger.error("config/business.yaml no encontrado")
        return {}


def obtener_horario() -> dict:
    """Retorna el horario de atencion del negocio."""
    info = cargar_info_negocio()
    ahora = datetime.now(timezone.utc)
    return {
        "horario": info.get("negocio", {}).get("horario", "No disponible"),
        "hora_actual_utc": ahora.isoformat(),
    }


def buscar_en_knowledge(consulta: str) -> str:
    """
    Busca informacion en los archivos de /knowledge.
    Retorna los fragmentos que coinciden con la consulta.
    """
    if not CARPETA_KNOWLEDGE.is_dir():
        return "No hay archivos de conocimiento disponibles."

    resultados = []
    for ruta in sorted(CARPETA_KNOWLEDGE.iterdir()):
        if ruta.name.startswith(".") or not ruta.is_file():
            continue
        try:
            contenido = ruta.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue  # binarios y archivos ilegibles se saltean
        if consulta.lower() in contenido.lower():
            resultados.append(f"[{ruta.name}]: {contenido[:500]}")

    if resultados:
        return "\n---\n".join(resultados)
    return "No encontre informacion especifica sobre eso en mis archivos."


# ════════════════════════════════════════════════════════════
# Funciones especificas para los casos de uso de Carrillo Audiovisual
# ════════════════════════════════════════════════════════════

def registrar_lead(telefono: str, nombre: str, rubro: str, horario_contacto: str) -> dict:
    """
    Registra un lead calificado listo para ser contactado por Israel.

    Por ahora guarda en log. Para conectar a un CRM (Notion, HubSpot, Google Sheets),
    agregar la integracion aqui.
    """
    logger.info(
        f"LEAD CALIFICADO — tel: {telefono} | nombre: {nombre} | "
        f"rubro: {rubro} | horario: {horario_contacto}"
    )
    return {
        "registrado": True,
        "mensaje": f"Lead {nombre} registrado. Israel lo va a contactar en {horario_contacto}.",
    }


def obtener_planes() -> list[dict]:
    """Retorna los planes disponibles con sus caracteristicas y precios."""
    return [
        {
            "nombre": "Plan Inicial",
            "precio_mensual": 260000,
            "reels": 8,
            "sesiones": 1,
            "historias": 10,
            "incluye": ["guiones", "edicion", "publicacion", "Meta Business", "reporte mensual"],
        },
        {
            "nombre": "Plan Crecimiento",
            "precio_mensual": 380000,
            "reels": 12,
            "sesiones": 2,
            "historias": 20,
            "incluye": ["calendario", "guiones", "edicion", "publicacion"],
        },
        {
            "nombre": "Plan Full",
            "precio_mensual": 520000,
            "reels": 16,
            "sesiones": 2,
            "historias": 30,
            "incluye": ["calendario", "guiones", "edicion", "publicacion"],
            "destacado": True,
        },
    ]


def obtener_complementos() -> list[dict]:
    """Retorna los servicios adicionales disponibles."""
    return [
        {"nombre": "Creacion de pagina web", "precio": 450000, "tipo": "unico"},
        {"nombre": "Manejo mensual web", "precio": 60000, "tipo": "mensual"},
        {"nombre": "Gestion de Meta Ads", "precio": 100000, "tipo": "mensual",
         "nota": "pauta aparte, minimo recomendado $50.000/mes"},
        {"nombre": "Setup automatizaciones (agente IA)", "precio": 120000, "tipo": "unico"},
        {"nombre": "Mantencion automatizaciones", "precio": 40000, "tipo": "mensual"},
        {"nombre": "Tours Virtuales 360", "precio": None, "tipo": "cotizar"},
        {"nombre": "Streaming en vivo", "precio": None, "tipo": "cotizar"},
    ]

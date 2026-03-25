from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
from types import SimpleNamespace
from fastapi.responses import FileResponse
from fastapi import FastAPI, Depends, Request, Form, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, select, delete
from sqlalchemy import or_, text, func
from sqlalchemy.exc import IntegrityError
from urllib.parse import quote

from collections import defaultdict
from io import BytesIO
from openpyxl import Workbook
from pywebpush import webpush, WebPushException

from db import create_db_and_tables, get_session, engine
from models import (
    User,
    Room,
    Reservation,
    ReservationRequest,
    AuditLog,
    SurgicalMapEntry,
    AgendaBlock,
    AgendaBlockSurgeon,
    GustavoAgendaSnapshot,
    LodgingReservation,
    ProcedureCatalog,
    SurgeryProcedureItem,
    PushSubscription,
    PushNotificationLog,
    FeegowProfessionalMap,
    FeegowValidationRun,
    FeegowValidationResult,
    FeegowValidationAcknowledgement,
)
from auth import hash_password, verify_password, require

from pathlib import Path

import calendar
import os
import json
import logging
import re
import unicodedata
import requests
from logging.handlers import RotatingFileHandler

import threading
import time as pytime

import smtplib
from email.message import EmailMessage

TZ = timezone(timedelta(hours=-3))  # Brasil (-03:00)
SLOT_MINUTES = 30
START_HOUR = 7
END_HOUR = 19  # 19:00 (último slot começa 18:30)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="CHANGE_ME_SUPER_SECRET_KEY")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/service-worker.js")
def service_worker():
    return FileResponse(
        "static/service-worker.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )

AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "audit.log")

audit_logger = logging.getLogger("audit")
audit_logger.setLevel(logging.INFO)
audit_logger.propagate = False

if not audit_logger.handlers:
    fh = RotatingFileHandler(
        AUDIT_LOG_PATH,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    audit_logger.addHandler(fh)

NUCLEI_OPTIONS = [
    "Corporal",
    "Mama",
    "Face",
    "Íntima",
    "Hospedagem",
    "Dermato",
    "Nutrologia",
]

# Mapeie aqui os médicos e os núcleos que eles podem executar.
# Os dois casos especiais são Ricardo e Alice.
SURGEON_ALLOWED_NUCLEI = {
    "Dr. Gustavo Aquino": ["Corporal"],
    "Dr. Ricardo Vilela": ["Corporal", "Mama"],
    "Dra. Alice Osório": ["Corporal", "Mama"],
    "Dra. Mellina Tanure": ["Face"],
    "Dra. Vanessa Santos": ["Dermato"],
    "Dra. Sophia Mourão": ["Dermato"],
    "Dra. Thamilys Benfica": ["Íntima"],
    "Dra. Stella Temponi": ["Nutrologia"],
}

SPECIAL_MAJORITY_SURGEONS = {
    "Dr. Ricardo Vilela",
    "Dra. Alice Osório",
}

def normalize_nucleus(value: str | None) -> str:
    return (value or "").strip()

def get_allowed_nuclei(proc: ProcedureCatalog) -> list[str]:
    raw = getattr(proc, "allowed_nuclei_json", None)
    primary = normalize_nucleus(getattr(proc, "nucleus", ""))

    if raw is None or raw == "":
        return [primary] if primary else []

    if isinstance(raw, list):
        data = raw
    elif isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            data = []
    else:
        data = []

    cleaned = []
    for item in data:
        val = normalize_nucleus(str(item))
        if val and val not in cleaned:
            cleaned.append(val)

    if primary and primary not in cleaned:
        cleaned.insert(0, primary)

    if not cleaned and primary:
        cleaned = [primary]

    return cleaned

def build_allowed_nuclei_json(primary_nucleus: str, allowed_nuclei: list[str] | None) -> list[str]:
    cleaned = []

    primary = normalize_nucleus(primary_nucleus)
    if primary:
        cleaned.append(primary)

    for item in (allowed_nuclei or []):
        val = normalize_nucleus(item)
        if val and val not in cleaned:
            cleaned.append(val)

    return cleaned

def get_surgeon_allowed_nuclei(surgeon: User | None) -> list[str]:
    if not surgeon:
        return []

    full_name = (surgeon.full_name or "").strip()
    nuclei = SURGEON_ALLOWED_NUCLEI.get(full_name, [])

    cleaned = []
    for item in nuclei:
        val = normalize_nucleus(item)
        if val and val not in cleaned:
            cleaned.append(val)

    return cleaned

def resolve_procedure_nuclei_for_entry(
    procedures: list[ProcedureCatalog],
    surgeon: User | None,
) -> dict[int, str]:
    resolved: dict[int, str] = {}
    surgeon_nuclei = get_surgeon_allowed_nuclei(surgeon)
    surgeon_name = (surgeon.full_name or "").strip() if surgeon else ""

    def filtered_allowed(proc: ProcedureCatalog) -> list[str]:
        allowed = get_allowed_nuclei(proc)

        if surgeon_nuclei:
            intersection = [n for n in allowed if n in surgeon_nuclei]
            if intersection:
                return intersection

        return allowed

    # Caso simples: cirurgião com núcleo único
    if len(surgeon_nuclei) == 1:
        forced_nucleus = surgeon_nuclei[0]

        for proc in procedures:
            if proc.id is None:
                continue

            allowed = filtered_allowed(proc)
            primary = normalize_nucleus(proc.nucleus)

            if forced_nucleus in allowed:
                resolved[proc.id] = forced_nucleus
            elif primary and primary in allowed:
                resolved[proc.id] = primary
            elif allowed:
                resolved[proc.id] = allowed[0]
            else:
                resolved[proc.id] = primary or forced_nucleus

        return resolved

    # Caso especial: Ricardo / Alice
    exclusive_counts = defaultdict(int)
    primary_counts = defaultdict(int)

    for proc in procedures:
        allowed = filtered_allowed(proc)
        primary = normalize_nucleus(proc.nucleus)

        if primary:
            primary_counts[primary] += 1

        if len(allowed) == 1:
            exclusive_counts[allowed[0]] += 1

    dominant_nucleus = None

    if surgeon_name in SPECIAL_MAJORITY_SURGEONS:
        if exclusive_counts:
            dominant_nucleus = sorted(
                exclusive_counts.items(),
                key=lambda x: (-x[1], x[0])
            )[0][0]
        elif primary_counts:
            dominant_nucleus = sorted(
                primary_counts.items(),
                key=lambda x: (-x[1], x[0])
            )[0][0]
        elif surgeon_nuclei:
            dominant_nucleus = surgeon_nuclei[0]

    for proc in procedures:
        if proc.id is None:
            continue

        allowed = filtered_allowed(proc)
        primary = normalize_nucleus(proc.nucleus)

        if len(allowed) == 1:
            resolved[proc.id] = allowed[0]
        elif dominant_nucleus and dominant_nucleus in allowed:
            resolved[proc.id] = dominant_nucleus
        elif primary and primary in allowed:
            resolved[proc.id] = primary
        elif allowed:
            resolved[proc.id] = allowed[0]
        else:
            resolved[proc.id] = primary or dominant_nucleus or ""

    return resolved

def to_db_dt(dt: datetime) -> datetime:
    """Converte qualquer datetime para horário local (-03) e remove tz/segundos p/ persistir no SQLite."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ).replace(tzinfo=None)
    return dt.replace(second=0, microsecond=0)

def fmt_brasilia(dt: datetime | None) -> str:
    if not dt:
        return "—"
    # Se veio "naive" do SQLite, vamos assumir que era UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M")

def slot_keys(dt: datetime) -> tuple[str, str]:
    """Retorna 2 chaves: sem segundos e com segundos, para evitar mismatch com o front."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    dt = dt.replace(second=0, microsecond=0)
    return (
        dt.isoformat(timespec="minutes"),  # 2025-11-29T07:00-03:00
        dt.isoformat(timespec="seconds"),  # 2025-11-29T07:00:00-03:00
    )

def local_today_str() -> str:
    return datetime.now(TZ).date().isoformat()


def safe_selected_and_day(raw_date: Optional[str]) -> tuple[str, date]:
    """
    Aceita None, "" ou uma string iso (YYYY-MM-DD).
    Retorna (selected_str, day_date) sempre válido, sem estourar ValueError.
    """
    selected = (raw_date or "").strip() or local_today_str()
    try:
        day = datetime.fromisoformat(selected).date()
    except ValueError:
        selected = local_today_str()
        day = datetime.fromisoformat(selected).date()
    return selected, day

def safe_selected_month(raw: Optional[str]) -> tuple[str, date, date, list[date]]:
    """
    Aceita None, "" ou 'YYYY-MM'. Retorna:
    selected ('YYYY-MM'), first_day, next_month_first_day, list_days
    """
    selected = (raw or "").strip() or datetime.now(TZ).strftime("%Y-%m")
    try:
        dt = datetime.strptime(selected, "%Y-%m")
    except ValueError:
        selected = datetime.now(TZ).strftime("%Y-%m")
        dt = datetime.strptime(selected, "%Y-%m")

    first = date(dt.year, dt.month, 1)
    # primeiro dia do mês seguinte
    if dt.month == 12:
        next_first = date(dt.year + 1, 1, 1)
    else:
        next_first = date(dt.year, dt.month + 1, 1)

    last_day = calendar.monthrange(dt.year, dt.month)[1]
    days = [date(dt.year, dt.month, d) for d in range(1, last_day + 1)]
    return selected, first, next_first, days

def build_slots_for_day(day: date):
    start_dt = datetime.combine(day, time(START_HOUR, 0), tzinfo=TZ)
    end_dt = datetime.combine(day, time(END_HOUR, 0), tzinfo=TZ)
    slots = []
    cur = start_dt
    while cur < end_dt:
        slots.append(cur)
        cur += timedelta(minutes=SLOT_MINUTES)
    return slots

def is_feegow_gate_allowed_path(path: str) -> bool:
    allowed_prefixes = (
        "/login",
        "/logout",
        "/auditoria_feegow/alertas",
        "/auditoria_feegow/alertas/ciencia",
        "/static",
        "/service-worker.js",
        "/favicon.ico",
    )

    return any(
        path == prefix or path.startswith(prefix + "/")
        for prefix in allowed_prefixes
    )

def get_current_user(request: Request, session: Session) -> Optional[User]:
    uid = request.session.get("user_id")
    if not uid:
        return None

    if request.session.get("feegow_alert_gate_required"):
        current_path = request.url.path
        if not is_feegow_gate_allowed_path(current_path):
            raise HTTPException(
                status_code=303,
                headers={"Location": "/auditoria_feegow/alertas"},
            )

    return session.get(User, uid)

def audit_event(
    request: Request,
    actor: Optional[User],
    action: str,
    *,
    success: bool = True,
    message: Optional[str] = None,
    room_id: Optional[int] = None,
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    extra: Optional[dict] = None,
):
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    method = request.method
    path = request.url.path

    # 1) grava no arquivo (nunca pode quebrar o sistema)
    try:
        payload = {
            "actor": getattr(actor, "username", None),
            "role": getattr(actor, "role", None),
            "action": action,
            "success": success,
            "message": message,
            "room_id": room_id,
            "target_type": target_type,
            "target_id": target_id,
            "start_time": start_time.isoformat(timespec="minutes") if start_time else None,
            "end_time": end_time.isoformat(timespec="minutes") if end_time else None,
            "ip": ip,
            "path": path,
            "method": method,
            "extra": extra or None,
        }
        audit_logger.info(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass

    # 2) grava no banco (isolado, pra não atrapalhar transações do request)
    try:
        with Session(engine) as s:
            row = AuditLog(
                actor_user_id=getattr(actor, "id", None),
                actor_username=getattr(actor, "username", None),
                actor_role=getattr(actor, "role", None),
                action=action,
                success=success,
                message=message,
                room_id=room_id,
                target_type=target_type,
                target_id=target_id,
                start_time=start_time,
                end_time=end_time,
                ip=ip,
                user_agent=ua,
                path=path,
                method=method,
                extra_json=json.dumps(extra, ensure_ascii=False) if extra else None,
            )
            s.add(row)
            s.commit()
    except Exception as e:
        audit_logger.exception("AUDIT_DB_FAIL | action=%s | err=%s", action, str(e))

def visible_surgical_map_status_clause():
    return or_(
        SurgicalMapEntry.status == None,
        SurgicalMapEntry.status.in_(["approved", "pending"])
    )


def load_surgical_map_procedures_snapshot(session: Session, entry_id: int) -> list[dict[str, Any]]:
    items = session.exec(
        select(SurgeryProcedureItem)
        .where(SurgeryProcedureItem.surgery_entry_id == entry_id)
        .order_by(SurgeryProcedureItem.id)
    ).all()

    return [
        {
            "procedure_id": item.procedure_id,
            "procedure_name": item.procedure_name_snapshot,
            "nucleus": item.nucleus_snapshot,
            "amount": float(item.amount or 0),
        }
        for item in items
    ]


def build_surgical_map_snapshot(session: Session, entry_or_id: SurgicalMapEntry | int | None) -> dict[str, Any]:
    if entry_or_id is None:
        return {}

    entry = entry_or_id if isinstance(entry_or_id, SurgicalMapEntry) else session.get(SurgicalMapEntry, entry_or_id)
    if not entry:
        return {}

    surgeon = session.get(User, entry.surgeon_id) if entry.surgeon_id else None
    seller = session.get(User, entry.created_by_id) if entry.created_by_id else None

    procedures = load_surgical_map_procedures_snapshot(session, entry.id)
    total_amount = round(sum(float(p.get("amount") or 0) for p in procedures), 2)

    return {
        "entry_id": entry.id,
        "status": entry.status or "active",
        "day": entry.day.isoformat() if entry.day else None,
        "time_hhmm": entry.time_hhmm,
        "patient_name": entry.patient_name,
        "surgeon_id": entry.surgeon_id,
        "surgeon_name": surgeon.full_name if surgeon else None,
        "seller_id": entry.created_by_id,
        "seller_name": seller.full_name if seller else None,
        "procedure_type": entry.procedure_type,
        "location": entry.location,
        "uses_hsr": bool(entry.uses_hsr),
        "is_pre_reservation": bool(entry.is_pre_reservation),
        "created_at": fmt_brasilia(entry.created_at) if getattr(entry, "created_at", None) else None,
        "procedures": procedures,
        "procedures_total_amount": total_amount,
    }


def build_surgical_map_changes(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    labels = {
        "day": "Data",
        "time_hhmm": "Horário",
        "patient_name": "Paciente",
        "surgeon_name": "Cirurgião",
        "seller_name": "Vendedor",
        "procedure_type": "Tipo",
        "location": "Hospital",
        "uses_hsr": "Slot HSR",
        "is_pre_reservation": "Modo",
        "status": "Status",
    }

    changes: list[str] = []

    for key, label in labels.items():
        b = before.get(key)
        a = after.get(key)
        if b != a:
            changes.append(f"{label}: {b or '—'} → {a or '—'}")

    if before.get("procedures") != after.get("procedures"):
        changes.append("Procedimentos e/ou valores alterados.")

    if before.get("procedures_total_amount") != after.get("procedures_total_amount"):
        changes.append(
            f"Valor total dos procedimentos: "
            f"{before.get('procedures_total_amount', 0)} → {after.get('procedures_total_amount', 0)}"
        )

    return changes

FEEGOW_API_BASE_URL = "https://api.feegow.com/v1/api"
FEEGOW_API_TOKEN = os.getenv("FEEGOW_API_TOKEN", "").strip()
FEEGOW_VALIDATION_MAX_DAYS = 30


def get_latest_feegow_run(session: Session) -> Optional[FeegowValidationRun]:
    return session.exec(
        select(FeegowValidationRun).order_by(FeegowValidationRun.id.desc())
    ).first()


def get_latest_feegow_alert_count(session: Session) -> int:
    latest_run = get_latest_feegow_run(session)
    if not latest_run:
        return 0

    return session.exec(
        select(func.count())
        .select_from(FeegowValidationResult)
        .where(
            FeegowValidationResult.run_id == latest_run.id,
            FeegowValidationResult.validation_status == "alert",
        )
    ).one()


def get_latest_feegow_status_by_entry(session: Session) -> dict[int, str]:
    latest_run = get_latest_feegow_run(session)
    if not latest_run:
        return {}

    rows = session.exec(
        select(FeegowValidationResult)
        .where(FeegowValidationResult.run_id == latest_run.id)
    ).all()

    status_by_entry: dict[int, str] = {}
    for row in rows:
        if row.surgical_entry_id is not None:
            status_by_entry[row.surgical_entry_id] = row.validation_status

    return status_by_entry

def get_pending_feegow_alerts_for_user(
    session: Session,
    user_id: int,
) -> list[FeegowValidationResult]:
    latest_run = get_latest_feegow_run(session)
    if not latest_run:
        return []

    rows = session.exec(
        select(FeegowValidationResult, SurgicalMapEntry)
        .join(
            SurgicalMapEntry,
            SurgicalMapEntry.id == FeegowValidationResult.surgical_entry_id,
        )
        .where(
            FeegowValidationResult.run_id == latest_run.id,
            FeegowValidationResult.validation_status.in_(["alert", "surgeon_not_mapped", "api_error"]),
            SurgicalMapEntry.created_by_id == user_id,
        )
        .order_by(
            FeegowValidationResult.map_day,
            FeegowValidationResult.map_patient_name,
        )
    ).all()

    pending: list[FeegowValidationResult] = []

    for result_row, _entry in rows:
        already_ack = session.exec(
            select(FeegowValidationAcknowledgement)
            .where(
                FeegowValidationAcknowledgement.validation_result_id == result_row.id,
                FeegowValidationAcknowledgement.ack_user_id == user_id,
            )
        ).first()

        if not already_ack:
            pending.append(result_row)

    return pending

def normalize_person_name(value: str | None) -> str:
    text_value = (value or "").strip().upper()
    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(ch for ch in text_value if not unicodedata.combining(ch))
    text_value = re.sub(r"[^A-Z0-9 ]+", " ", text_value)
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value


def names_equivalent(a: str | None, b: str | None) -> bool:
    na = normalize_person_name(a)
    nb = normalize_person_name(b)

    if not na or not nb:
        return False

    if na == nb:
        return True

    if na in nb or nb in na:
        return True

    stopwords = {"DA", "DE", "DI", "DO", "DU", "DAS", "DOS", "E"}
    ta = [t for t in na.split() if len(t) > 1 and t not in stopwords]
    tb = [t for t in nb.split() if len(t) > 1 and t not in stopwords]

    if not ta or not tb:
        return False

    common = set(ta) & set(tb)
    return len(common) >= min(2, len(ta), len(tb))


def feegow_headers() -> dict[str, str]:
    if not FEEGOW_API_TOKEN:
        raise RuntimeError("A variável de ambiente FEEGOW_API_TOKEN não foi configurada.")
    return {
        "Host": "api.feegow.com",
        "x-access-token": FEEGOW_API_TOKEN,
    }


def feegow_get_json(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{FEEGOW_API_BASE_URL}/{endpoint.lstrip('/')}"
    response = requests.get(url, headers=feegow_headers(), params=params, timeout=45)

    try:
        payload = response.json()
    except ValueError:
        payload = {"success": False, "content": response.text}

    if response.status_code != 200:
        raise RuntimeError(
            f"Feegow retornou {response.status_code} em {endpoint}: {payload}"
        )

    return payload


def feegow_date_str(value: date) -> str:
    return value.strftime("%d-%m-%Y")


def fetch_feegow_patient_name(patient_id: int | None, patient_cache: dict[int, str]) -> str:
    if not patient_id:
        return ""

    if patient_id in patient_cache:
        return patient_cache[patient_id]

    payload = feegow_get_json(
        "patient/search",
        {
            "paciente_id": patient_id,
            "photo": 0,
        },
    )

    content = payload.get("content") or {}
    patient_name = (content.get("nome") or "").strip()
    patient_cache[patient_id] = patient_name
    return patient_name


def fetch_feegow_appointments_for_professional(
    professional_id: int,
    start_date: date,
    end_date: date,
    patient_cache: dict[int, str],
) -> list[dict[str, Any]]:
    payload = feegow_get_json(
        "appoints/search",
        {
            "profissional_id": professional_id,
            "data_start": feegow_date_str(start_date),
            "data_end": feegow_date_str(end_date),
        },
    )

    rows = payload.get("content") or []
    normalized_rows: list[dict[str, Any]] = []

    for item in rows:
        item_copy = dict(item)
        patient_id = item_copy.get("paciente_id")
        patient_name = fetch_feegow_patient_name(patient_id, patient_cache)

        item_copy["patient_name"] = patient_name
        item_copy["patient_name_normalized"] = normalize_person_name(patient_name)
        normalized_rows.append(item_copy)

    return normalized_rows


def find_matching_feegow_appointment(
    entry: SurgicalMapEntry,
    appointments: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    target_date = entry.day.strftime("%d-%m-%Y")

    same_day = [row for row in appointments if (row.get("data") or "") == target_date]
    if not same_day:
        return None, "Nenhum agendamento encontrado no Feegow para esta data."

    for row in same_day:
        if names_equivalent(entry.patient_name, row.get("patient_name")):
            return row, "Paciente encontrado no Feegow para a mesma data."

    return None, "Há agenda do cirurgião nesta data, mas o paciente não foi encontrado no Feegow."

def redirect(path: str):
    return RedirectResponse(path, status_code=303)


def seed_if_empty(session: Session):
    # =========================
    # USERS (cria SE não existir)
    # =========================
    def ensure_user(username: str, full_name: str, role: str, password: str):
        existing = session.exec(select(User).where(User.username == username)).first()
        if not existing:
            session.add(
                User(
                    username=username,
                    full_name=full_name,
                    role=role,
                    password_hash=hash_password(password),
                    is_active=True,
                )
            )

    # Admin padrão
    ensure_user("secretaria", "Secretaria (Admin)", "admin", "admin123")

    # Médicos padrão
    doctors = [
        ("drgustavo", "Dr. Gustavo Aquino"),
        ("drricardo", "Dr. Ricardo Vilela"),
        ("draalice", "Dra. Alice Osório"),
        ("dramelina", "Dra. Mellina Tanure"),
        ("dravanessa", "Dra. Vanessa Santos"),
        ("drasophia", "Dra. Sophia Mourão"),        
        ("drathamilys", "Dra. Thamilys Benfica"),
        ("drastela", "Dra. Stella Temponi"),
        ("draglesiane", "Dra. Glesiane Teixeira"),
    ]
    for username, name in doctors:
        ensure_user(username, name, "doctor", "senha123")

    # NOVO: usuário do Mapa Cirúrgico
    ensure_user("johnny.ge", "Johnny", "surgery", "@Ynnhoj91")
    ensure_user("ana.maria", "Ana Maria", "surgery", "AnaM#2025@91")
    ensure_user("cris.galdino", "Cristiane Galdino", "surgery", "CrisG@2025#47")
    ensure_user("carolina.abdo", "Carolina", "surgery", "Caro!2025#38")
    ensure_user("ariella.vieira", "Ariella", "surgery", "Ariella$2026")
    ensure_user("camilla.martins", "Camilla", "comissao", "Camilla*2026")
    ensure_user("sayonara.goncalves", "Sayonara", "viewer", "Sayonara*2026")
    ensure_user("andre.silva", "André", "viewer", "Andre*2026")
    ensure_user("amanda.rodrigues", "Amanda", "viewer", "Amanda*2026")

    session.commit()

    # =========================
    # ROOMS (cria SE não existir)
    # =========================
    rooms = session.exec(select(Room)).all()
    if not rooms:
        default_rooms = [
            Room(name="Consultório 1", is_active=True),
            Room(name="Consultório 2", is_active=True),
            Room(name="Consultório 3", is_active=True),
        ]
        session.add_all(default_rooms)
        session.commit()

def validate_mapa_rules(
    session: Session,
    day: date,
    surgeon_id: int,
    procedure_type: str,
    uses_hsr: bool = False,
    exclude_entry_id: int | None = None,
) -> str | None:
    """
    Regras do Mapa Cirúrgico

    ✅ Reserva conta como agendamento (SurgicalMapEntry com is_pre_reservation=True também entra na contagem).

    Regras:
    - Dr. Gustavo Aquino:
        * Cirurgia / Procedimento Simples: somente Segunda e Quarta (máx 2 por dia)
        * Refinamento: Segunda e Quarta (máx 2 por dia) + Sexta (máx 1 por dia)
    - Dra. Alice Osório e Dr. Ricardo Vilela:
        * Operam Terça, Quinta e Sexta (máx 1 por dia)
        * Não podem operar no mesmo dia (se um tem qualquer agendamento/reserva, o outro não pode)
    - Slot HSR: proibido em Janeiro e Julho
    """

    gustavo = session.exec(select(User).where(User.full_name == "Dr. Gustavo Aquino")).first()
    alice = session.exec(select(User).where(User.full_name == "Dra. Alice Osório")).first()
    ricardo = session.exec(select(User).where(User.full_name == "Dr. Ricardo Vilela")).first()

    def _apply_exclude(q):
        q = q.where(visible_surgical_map_status_clause())
        if exclude_entry_id is not None:
            q = q.where(SurgicalMapEntry.id != exclude_entry_id)
        return q

    # HSR jan/jul
    if uses_hsr and day.month in (1, 7):
        return "Regra: não é permitido agendar Slot HSR em Janeiro e Julho."

    wd = day.weekday()  # 0=Seg,1=Ter,2=Qua,3=Qui,4=Sex,5=Sáb,6=Dom

    # =========================
    # (A) Dr. Gustavo Aquino
    # =========================
    if gustavo and surgeon_id == gustavo.id:
        if procedure_type == "Refinamento":
            # Seg/Qua até 2, Sex até 1
            if wd in (0, 2):
                cap = 2
            elif wd == 4:
                cap = 1
            else:
                return "Regra: Dr. Gustavo Aquino opera Refinamento apenas na Segunda, Quarta ou Sexta."
        else:
            # Cirurgia / Procedimento Simples: só Seg/Qua até 2
            if wd not in (0, 2):
                return "Regra: Dr. Gustavo Aquino opera Cirurgia/Procedimento Simples apenas na Segunda e Quarta."
            cap = 2

        q = select(SurgicalMapEntry.id).where(
            SurgicalMapEntry.day == day,
            SurgicalMapEntry.surgeon_id == gustavo.id,
        )
        q = _apply_exclude(q)
        already = session.exec(q).all()

        if len(already) >= cap:
            if cap == 2:
                return "Regra: Dr. Gustavo Aquino não pode ter mais de 2 agendamentos no mesmo dia."
            return "Regra: Dr. Gustavo Aquino não pode ter mais de 1 agendamento (Refinamento) na Sexta-feira."

        return None

    # =========================
    # (B) Alice e Ricardo
    # =========================
    if alice and ricardo and surgeon_id in (alice.id, ricardo.id):
        # dias permitidos: Ter/Qui/Sex
        if wd not in (1, 3, 4):
            return "Regra: Dra. Alice Osório e Dr. Ricardo Vilela operam apenas na Terça, Quinta ou Sexta."

        # capacidade do próprio médico: 1 por dia
        q_self = select(SurgicalMapEntry.id).where(
            SurgicalMapEntry.day == day,
            SurgicalMapEntry.surgeon_id == surgeon_id,
        )
        q_self = _apply_exclude(q_self)
        if session.exec(q_self).first():
            return "Regra: Dra. Alice Osório e Dr. Ricardo Vilela não podem ter mais de 1 procedimento no mesmo dia."

        # conflito Alice x Ricardo: se o outro tem qualquer agendamento/reserva no dia, bloqueia
        other_id = ricardo.id if surgeon_id == alice.id else alice.id
        q_other = select(SurgicalMapEntry.id).where(
            SurgicalMapEntry.day == day,
            SurgicalMapEntry.surgeon_id == other_id,
        )
        q_other = _apply_exclude(q_other)
        if session.exec(q_other).first():
            return "Regra: Dra. Alice Osório e Dr. Ricardo Vilela não podem operar no mesmo dia."

        return None

    # Outros cirurgiões (se existirem) sem regras específicas aqui
    return None

# ============================================================
# HOSPEDAGEM (2 suítes + 1 apartamento) - reservas por período
# check_out é NÃO inclusivo (data de saída)
# ============================================================

def validate_lodging_period(check_in: date, check_out: date) -> Optional[str]:
    if not check_in or not check_out:
        return "Informe check-in e check-out."
    if check_out <= check_in:
        return "Período inválido: check-out deve ser após check-in."
    return None


def validate_lodging_conflict(
    session: Session,
    unit: str,
    check_in: date,
    check_out: date,
    exclude_id: Optional[int] = None,
) -> Optional[str]:
    # conflito se: novo_in < existente_out AND novo_out > existente_in
    q = select(LodgingReservation).where(
        LodgingReservation.unit == unit,
        LodgingReservation.check_in < check_out,
        LodgingReservation.check_out > check_in,
    )
    if exclude_id is not None:
        q = q.where(LodgingReservation.id != exclude_id)

    exists = session.exec(q).first()
    if exists:
        audit_logger.info(
            "LODGE_CONFLICT: new_unit=%s new_ci=%s new_co=%s | "
            "found_id=%s found_unit=%s found_ci=%s found_co=%s found_patient=%s pre=%s surgery_entry_id=%s",
            unit, check_in, check_out,
            getattr(exists, "id", None),
            getattr(exists, "unit", None),
            getattr(exists, "check_in", None),
            getattr(exists, "check_out", None),
            getattr(exists, "patient_name", None),
            getattr(exists, "is_pre_reservation", None),
            getattr(exists, "surgery_entry_id", None),
        )
        return "Hospedagem indisponível: já existe reserva nesse período para esta acomodação."
    return None

def get_lodging_conflict_row(
    session: Session,
    unit: str,
    check_in: date,
    check_out: date,
    exclude_id: Optional[int] = None,
):
    q = select(LodgingReservation).where(
        LodgingReservation.unit == unit,
        LodgingReservation.check_in < check_out,
        LodgingReservation.check_out > check_in,
    )
    if exclude_id is not None:
        q = q.where(LodgingReservation.id != exclude_id)

    return session.exec(q).first()

def human_unit(unit: str) -> str:
    return {
        "suite_1": "Suíte 1304",
        "suite_2": "Suíte 1303",
        "apto": "Apartamento",
    }.get(unit, unit)

def format_unit_for_email(unit: str) -> str:
    unit_norm = normalize_unit(unit)
    return {
        "suite_1": "RESERVA SUÍTE 1304",
        "suite_2": "RESERVA SUÍTE 1303",
        "apto": "RESERVA APARTAMENTO",
    }.get(unit_norm, f"RESERVA {unit_norm.upper()}")


def format_date_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def build_lodging_email_body(
    *,
    action_label: str,
    requested_by_name: str,
    request_date: datetime,
    unit: str,
    patient_name: str,
    patient_cpf: Optional[str],
    patient_phone: Optional[str],
    check_in: date,
    check_out: date,
) -> str:
    return f"""Olá, a pedido do usuario {requested_by_name}, em {request_date.strftime('%d/%m/%Y %H:%M')}, gentileza {action_label} a reserva abaixo:

{format_unit_for_email(unit)}

{patient_name}
CPF {patient_cpf or '-'}
TELEFONE {patient_phone or '-'}
DATA CHECK IN: {format_date_br(check_in)}
DATA CHECK OUT: {format_date_br(check_out)}

Atenciosamente
Time Comercial
Concept Clinic
"""


def send_lodging_email_notification(
    *,
    subject: str,
    body: str,
    unit: str,
):
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    smtp_from = os.getenv("SMTP_FROM", "").strip() or smtp_user

    if normalize_unit(unit) == "apto":
        hotel_to = os.getenv("HOTEL_APARTMENT_NOTIFICATION_TO", "").strip()
    else:
        hotel_to = os.getenv("HOTEL_NOTIFICATION_TO", "").strip()

    recipients = [email.strip() for email in hotel_to.split(",") if email.strip()]

    if not smtp_host or not smtp_user or not smtp_password or not smtp_from or not recipients:
        audit_logger.warning(
            "EMAIL_HOTEL_NAO_ENVIADO: variáveis SMTP/HOTEL_NOTIFICATION_TO não configuradas."
        )
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg, to_addrs=recipients)

def fmt_date_br(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def fmt_day_month_br(d: date) -> str:
    return d.strftime("%d/%m")


def hotel_weekday_pt(d: date) -> str:
    nomes = [
        "segunda-feira",
        "terça-feira",
        "quarta-feira",
        "quinta-feira",
        "sexta-feira",
        "sábado",
        "domingo",
    ]
    return nomes[d.weekday()]


def normalize_unit(unit: Optional[str]) -> str:
    raw = (unit or "").strip().lower()

    mapping = {
        "suite_1": "suite_1",
        "suíte 1": "suite_1",
        "suite 1": "suite_1",
        "suite1": "suite_1",

        "suite_2": "suite_2",
        "suíte 2": "suite_2",
        "suite 2": "suite_2",
        "suite2": "suite_2",

        "apto": "apto",
        "apartamento": "apto",
        "apart": "apto",
    }

    return mapping.get(raw, raw)

def normalize_event_key_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())

def hotel_card_from_reservation(r: LodgingReservation) -> dict:
    unit_norm = normalize_unit(getattr(r, "unit", None))
    return {
        "id": r.id,
        "unit": unit_norm,
        "unit_label": human_unit(unit_norm),
        "patient_name": (r.patient_name or "").strip(),
        "patient_cpf": (getattr(r, "patient_cpf", "") or "").strip(),
        "patient_phone": (getattr(r, "patient_phone", "") or "").strip(),
        "check_in": r.check_in,
        "check_out": r.check_out,
        "check_in_br": fmt_date_br(r.check_in),
        "check_out_br": fmt_date_br(r.check_out),
        "period_br": f"{fmt_day_month_br(r.check_in)} → {fmt_day_month_br(r.check_out)}",
        "is_pre": bool(getattr(r, "is_pre_reservation", False)),
        "note": (getattr(r, "note", "") or "").strip(),
    }


def build_hotel_dashboard_data(session: Session, ref_day: date) -> dict:
    units = ["suite_2", "suite_1", "apto"]
    
    # check-in hoje
    checkins_rows = session.exec(
        select(LodgingReservation).where(
            LodgingReservation.check_in == ref_day
        ).order_by(LodgingReservation.unit, LodgingReservation.patient_name)
    ).all()

    # check-out hoje
    checkouts_rows = session.exec(
        select(LodgingReservation).where(
            LodgingReservation.check_out == ref_day
        ).order_by(LodgingReservation.unit, LodgingReservation.patient_name)
    ).all()

    # hóspedes atualmente no hotel
    inhouse_rows = session.exec(
        select(LodgingReservation).where(
            LodgingReservation.check_in <= ref_day,
            LodgingReservation.check_out > ref_day,
        ).order_by(LodgingReservation.unit, LodgingReservation.check_in, LodgingReservation.patient_name)
    ).all()

    # próximas chegadas
    upcoming_checkins_rows = session.exec(
        select(LodgingReservation).where(
            LodgingReservation.check_in > ref_day
        ).order_by(LodgingReservation.check_in, LodgingReservation.unit, LodgingReservation.patient_name)
    ).all()

    # próximas saídas
    upcoming_checkouts_rows = session.exec(
        select(LodgingReservation).where(
            LodgingReservation.check_out > ref_day
        ).order_by(LodgingReservation.check_out, LodgingReservation.unit, LodgingReservation.patient_name)
    ).all()

    checkins_today = [hotel_card_from_reservation(r) for r in checkins_rows]
    checkouts_today = [hotel_card_from_reservation(r) for r in checkouts_rows]
    inhouse = [hotel_card_from_reservation(r) for r in inhouse_rows]
    upcoming_checkins = [hotel_card_from_reservation(r) for r in upcoming_checkins_rows[:10]]
    upcoming_checkouts = [hotel_card_from_reservation(r) for r in upcoming_checkouts_rows[:10]]

    occupied_by_unit = {}
    for item in inhouse:
        occupied_by_unit[item["unit"]] = item

    room_status = []
    for unit in units:
        current = occupied_by_unit.get(unit)
        if current:
            room_status.append(
                {
                    "unit": unit,
                    "unit_label": human_unit(unit),
                    "status": "occupied",
                    "status_label": "Ocupado",
                    "patient_name": current["patient_name"],
                    "period_br": current["period_br"],
                    "is_pre": current["is_pre"],
                }
            )
        else:
            room_status.append(
                {
                    "unit": unit,
                    "unit_label": human_unit(unit),
                    "status": "free",
                    "status_label": "Livre",
                    "patient_name": "",
                    "period_br": "",
                    "is_pre": False,
                }
            )

    month_label = [
        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
    ][ref_day.month - 1]

    return {
        "ref_day": ref_day,
        "today_label": f"{ref_day.day} de {month_label} - {hotel_weekday_pt(ref_day)}",
        "checkins_today": checkins_today,
        "checkouts_today": checkouts_today,
        "inhouse": inhouse,
        "upcoming_checkins": upcoming_checkins,
        "upcoming_checkouts": upcoming_checkouts,
        "room_status": room_status,
        "count_checkins": len(checkins_today),
        "count_checkouts": len(checkouts_today),
        "count_inhouse": len(inhouse),
    }

def _weekday_pt(idx: int) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[idx]


WEBPUSH_VAPID_PUBLIC_KEY = os.getenv("WEBPUSH_VAPID_PUBLIC_KEY", "").strip()
WEBPUSH_VAPID_PRIVATE_KEY = os.getenv("WEBPUSH_VAPID_PRIVATE_KEY", "").strip()
WEBPUSH_VAPID_SUBJECT = os.getenv("WEBPUSH_VAPID_SUBJECT", "mailto:suporte@conceptclinic.com.br").strip()

TASK_ALERT_SURGEONS = {
    "Dr. Gustavo Aquino",
    "Dra. Alice Osório",
    "Dr. Ricardo Vilela",
    "Dra. Mellina Tanure",
    "Dra. Thamilys Benfica",
}

TASK_PUSH_RUN_LABELS = ("09h", "17h")

def webpush_is_configured() -> bool:
    return bool(
        WEBPUSH_VAPID_PUBLIC_KEY
        and WEBPUSH_VAPID_PRIVATE_KEY
        and WEBPUSH_VAPID_SUBJECT
    )


def build_lodging_push_payload(event_type: str, row) -> dict:
    unit_label = human_unit(normalize_unit(getattr(row, "unit", "")))
    patient_name = (getattr(row, "patient_name", "") or "").strip()
    check_in = getattr(row, "check_in", None)
    check_out = getattr(row, "check_out", None)

    check_in_br = fmt_date_br(check_in) if check_in else "-"
    check_out_br = fmt_date_br(check_out) if check_out else "-"

    titles = {
        "create": "Nova reserva de hospedagem",
        "update": "Hospedagem atualizada",
        "delete": "Hospedagem excluída",
        "override": "Reserva sobreposta",
        "checkin_today": "Check-in de hoje",
        "checkout_today": "Check-out de hoje",
        "arrival_3d": "Chegada em 3 dias",
    }

    bodies = {
        "create": f"{patient_name} • {unit_label} • {check_in_br} → {check_out_br}",
        "update": f"{patient_name} • {unit_label} • {check_in_br} → {check_out_br}",
        "delete": f"{patient_name} • {unit_label} • {check_in_br} → {check_out_br}",
        "override": f"{patient_name} • {unit_label} • {check_in_br} → {check_out_br}",
        "checkin_today": f"{patient_name} entra hoje em {unit_label}",
        "checkout_today": f"{patient_name} sai hoje de {unit_label}",
        "arrival_3d": f"{patient_name} chega em 3 dias em {unit_label}",
    }

    return {
        "title": titles.get(event_type, "Hospedagens Concept"),
        "body": bodies.get(event_type, patient_name or "Atualização de hospedagem"),
        "icon": "/static/icons/icon-512.png",
        "badge": "/static/icons/icon-512.png",
        "tag": f"lodging-{event_type}-{getattr(row, 'id', 'na')}",
        "url": "/hotel_mobile",
        "data": {
            "url": "/hotel_mobile",
            "reservation_id": getattr(row, "id", None),
            "event_type": event_type,
        },
    }


def normalize_chart_task_patient_key(value: str | None) -> str:
    return normalize_event_key_text(value)


def get_chart_task_alert_day(surgery_day: date) -> date:
    wd = surgery_day.weekday()  # 0=seg, 1=ter, 2=qua, 3=qui, 4=sex, 5=sáb, 6=dom

    if wd == 0:  # cirurgia segunda -> alerta sexta
        return surgery_day - timedelta(days=3)

    if wd == 6:  # cirurgia domingo -> alerta sexta
        return surgery_day - timedelta(days=2)

    # terça a sábado -> dia anterior
    return surgery_day - timedelta(days=1)


def build_chart_task_key(
    *,
    task_type: str,
    seller_id: int | None,
    surgery_day: date,
    patient_name: str | None,
) -> str:
    patient_key = normalize_chart_task_patient_key(patient_name)
    seller_part = seller_id if seller_id is not None else "sem_vendedor"
    return f"chart_task:{task_type}:{seller_part}:{surgery_day.isoformat()}:{patient_key}"

def build_feegow_execution_tasks(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    seller_user_id: int | None = None,
) -> list[dict[str, Any]]:
    surgeons = session.exec(
        select(User).where(User.role == "doctor", User.is_active == True)
    ).all()
    surgeons_by_id = {u.id: u for u in surgeons if u.id is not None}

    sellers = session.exec(
        select(User).where(User.role == "surgery", User.is_active == True)
    ).all()
    sellers_by_id = {u.id: u for u in sellers if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= start_day,
            SurgicalMapEntry.day <= end_day,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()

    grouped: dict[str, dict[str, Any]] = {}

    for row in entries:
        surgeon = surgeons_by_id.get(row.surgeon_id)
        if not surgeon:
            continue

        if seller_user_id is not None and row.created_by_id != seller_user_id:
            continue

        task_key = build_chart_task_key(
            task_type="feegow_execute",
            seller_id=row.created_by_id,
            surgery_day=row.day,
            patient_name=row.patient_name,
        )

        seller = sellers_by_id.get(row.created_by_id)
        alert_day = row.day  # aqui o alerta é no próprio dia da cirurgia

        if task_key not in grouped:
            grouped[task_key] = {
                "task_key": task_key,
                "task_type": "feegow_execute",
                "task_type_label": "Execução no Feegow",
                "patient_name": (row.patient_name or "").strip().upper(),
                "surgery_day": row.day,
                "alert_day": alert_day,
                "seller_id": row.created_by_id,
                "seller_name": seller.full_name if seller else "Sem vendedor",
                "surgeons": set(),
                "message": f"Executar a cirurgia da paciente {(row.patient_name or '').strip().upper()} no Feegow.",
            }

        grouped[task_key]["surgeons"].add(surgeon.full_name)

    completed_map = load_completed_chart_tasks(session)

    result: list[dict[str, Any]] = []
    for item in grouped.values():
        completion = completed_map.get(item["task_key"])
        item["surgeons"] = sorted(item["surgeons"])
        item["completed"] = bool(completion)
        item["completed_at"] = completion.get("completed_at") if completion else None
        item["completed_by"] = completion.get("completed_by") if completion else None
        result.append(item)

    result.sort(key=lambda x: (x["alert_day"], x["surgery_day"], x["patient_name"]))
    return result

def build_financial_tasks(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    seller_user_id: int | None = None,
) -> list[dict[str, Any]]:

    surgeons = session.exec(
        select(User).where(User.role == "doctor", User.is_active == True)
    ).all()
    surgeons_by_id = {u.id: u for u in surgeons if u.id is not None}

    sellers = session.exec(
        select(User).where(User.role == "surgery", User.is_active == True)
    ).all()
    sellers_by_id = {u.id: u for u in sellers if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= start_day,
            SurgicalMapEntry.day <= end_day,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.created_at)
    ).all()

    grouped: dict[str, dict[str, Any]] = {}

    for row in entries:
        if seller_user_id is not None and row.created_by_id != seller_user_id:
            continue

        alert_day = row.day - timedelta(days=30)

        task_key = build_chart_task_key(
            task_type="financial_check",
            seller_id=row.created_by_id,
            surgery_day=row.day,
            patient_name=row.patient_name,
        )

        seller = sellers_by_id.get(row.created_by_id)

        if task_key not in grouped:
            grouped[task_key] = {
                "task_key": task_key,
                "task_type": "financial_check",
                "task_type_label": "Acerto Financeiro",
                "patient_name": (row.patient_name or "").strip().upper(),
                "surgery_day": row.day,
                "alert_day": alert_day,
                "seller_id": row.created_by_id,
                "seller_name": seller.full_name if seller else "Sem vendedor",
                "surgeons": set(),
                "message": f"Realizar acerto financeiro da paciente {(row.patient_name or '').strip().upper()} (valores em aberto).",
            }

    completed_map = load_completed_chart_tasks(session)

    result = []
    for item in grouped.values():
        completion = completed_map.get(item["task_key"])
        item["completed"] = bool(completion)
        item["completed_at"] = completion.get("completed_at") if completion else None
        item["completed_by"] = completion.get("completed_by") if completion else None
        result.append(item)

    result.sort(key=lambda x: (x["alert_day"], x["surgery_day"]))
    return result

def build_all_chart_tasks(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    seller_user_id: int | None = None,
) -> list[dict[str, Any]]:
    prontuario_tasks = build_chart_tasks(
        session,
        start_day=start_day,
        end_day=end_day,
        seller_user_id=seller_user_id,
    )

    feegow_tasks = build_feegow_execution_tasks(
        session,
        start_day=start_day,
        end_day=end_day,
        seller_user_id=seller_user_id,
    )
    
    financial_tasks = build_financial_tasks(
        session,
        start_day=start_day,
        end_day=end_day,
        seller_user_id=seller_user_id,
    )

    policy_tasks = build_policy_tasks(
        session,
        ref_day=start_day,
        seller_user_id=seller_user_id,
    )

    all_tasks = prontuario_tasks + feegow_tasks + financial_tasks + policy_tasks
    all_tasks.sort(
        key=lambda x: (
            x["alert_day"],
            x["surgery_day"],
            x.get("task_type", ""),
            x["patient_name"],
        )
    )
    return all_tasks

def load_completed_chart_tasks(session: Session) -> dict[str, dict]:
    rows = session.exec(
        select(AuditLog)
        .where(
            AuditLog.target_type == "chart_task",
            AuditLog.action == "chart_task_completed",
        )
        .order_by(AuditLog.id.desc())
    ).all()

    completed: dict[str, dict] = {}

    for row in rows:
        extra = {}
        raw_extra = getattr(row, "extra_json", None)

        if raw_extra:
            try:
                extra = json.loads(raw_extra)
            except Exception:
                extra = {}

        task_key = extra.get("task_key")
        if not task_key or task_key in completed:
            continue

        completed[task_key] = {
            "completed_at": getattr(row, "created_at", None),
            "completed_by": getattr(row, "actor_username", None) or "—",
            "task_type": extra.get("task_type") or "",
        }

    return completed

def build_chart_tasks(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    seller_user_id: int | None = None,
) -> list[dict[str, Any]]:
    surgeons = session.exec(
        select(User).where(User.role == "doctor", User.is_active == True)
    ).all()
    surgeons_by_id = {u.id: u for u in surgeons if u.id is not None}

    sellers = session.exec(
        select(User).where(User.role == "surgery", User.is_active == True)
    ).all()
    sellers_by_id = {u.id: u for u in sellers if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= start_day,
            SurgicalMapEntry.day <= end_day,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()

    grouped: dict[str, dict[str, Any]] = {}

    for row in entries:
        surgeon = surgeons_by_id.get(row.surgeon_id)
        if not surgeon or surgeon.full_name not in TASK_ALERT_SURGEONS:
            continue

        if seller_user_id is not None and row.created_by_id != seller_user_id:
            continue

        task_key = build_chart_task_key(
            task_type="chart_prontuario",
            seller_id=row.created_by_id,
            surgery_day=row.day,
            patient_name=row.patient_name,
        )

        seller = sellers_by_id.get(row.created_by_id)
        alert_day = get_chart_task_alert_day(row.day)

        if task_key not in grouped:
            grouped[task_key] = {
                "task_key": task_key,
                "task_type": "chart_prontuario",
                "task_type_label": "Prontuário",
                "patient_name": (row.patient_name or "").strip().upper(),
                "surgery_day": row.day,
                "alert_day": alert_day,
                "seller_id": row.created_by_id,
                "seller_name": seller.full_name if seller else "Sem vendedor",
                "surgeons": set(),
                "message": f"Enviar prontuário da paciente {(row.patient_name or '').strip().upper()} para o time de cirurgia.",
            }

        grouped[task_key]["surgeons"].add(surgeon.full_name)

    completed_map = load_completed_chart_tasks(session)

    result: list[dict[str, Any]] = []
    for item in grouped.values():
        completion = completed_map.get(item["task_key"])
        item["surgeons"] = sorted(item["surgeons"])
        item["completed"] = bool(completion)
        item["completed_at"] = completion.get("completed_at") if completion else None
        item["completed_by"] = completion.get("completed_by") if completion else None
        result.append(item)

    result.sort(key=lambda x: (x["alert_day"], x["surgery_day"], x["patient_name"]))
    return result


def build_chart_task_push_payload(task: dict[str, Any], run_label: str) -> dict:
    patient_name = (task.get("patient_name") or "").strip()
    surgery_day = task.get("surgery_day")
    surgery_day_br = fmt_date_br(surgery_day) if surgery_day else "-"
    task_type = task.get("task_type") or "chart_prontuario"

    if task_type == "feegow_execute":
        title = f"Lembrete de execução no Feegow • {run_label}"
        body = f"{patient_name} opera em {surgery_day_br}. Executar a cirurgia no Feegow."

    elif task_type == "financial_check":
        title = f"Lembrete de acerto financeiro • {run_label}"
        body = f"{patient_name} opera em {surgery_day_br}. Realizar acerto financeiro (cobrança de valores em aberto)."

    elif task_type == "policy_issue":
        title = f"Lembrete de apólice • {run_label}"
        body = f"{patient_name} opera em {surgery_day_br}. Solicitar emissão da apólice de seguro."

    else:
        title = f"Lembrete de prontuário • {run_label}"
        body = f"{patient_name} opera em {surgery_day_br}. Enviar prontuário."

    return {
        "title": title,
        "body": body,
        "icon": "/static/icons/icon-512.png",
        "badge": "/static/icons/icon-512.png",
        "tag": f"{task['task_key']}:{run_label}",
        "url": "/tasks",
        "data": {
            "url": "/tasks",
            "task_key": task["task_key"],
            "event_type": "chart_task_reminder",
            "run_label": run_label,
            "task_type": task_type,
        },
    }


def send_push_payload_to_user_subscriptions(session: Session, *, user_id: int, payload: dict) -> None:
    if not webpush_is_configured():
        audit_logger.info("WEBPUSH_TASKS: ignorado (VAPID não configurado).")
        return

    subs = session.exec(
        select(PushSubscription).where(
            PushSubscription.is_active == True,
            PushSubscription.user_id == user_id,
        )
    ).all()

    if not subs:
        audit_logger.info(f"WEBPUSH_TASKS: nenhuma inscrição ativa para user_id={user_id}.")
        return

    changed = False

    for sub in subs:
        try:
            webpush(
                subscription_info={
                    "endpoint": sub.endpoint,
                    "keys": {
                        "p256dh": sub.p256dh,
                        "auth": sub.auth,
                    },
                },
                data=json.dumps(payload, ensure_ascii=False),
                vapid_private_key=WEBPUSH_VAPID_PRIVATE_KEY,
                vapid_claims={"sub": WEBPUSH_VAPID_SUBJECT},
                ttl=60,
            )
        except WebPushException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            audit_logger.exception(
                f"WEBPUSH_TASKS_SEND_ERROR: user_id={user_id} endpoint={sub.endpoint[:80]} status={status_code} err={e}"
            )

            if status_code in (404, 410):
                sub.is_active = False
                sub.updated_at = datetime.utcnow()
                session.add(sub)
                changed = True

        except Exception as e:
            audit_logger.exception(f"WEBPUSH_TASKS_SEND_GENERIC_ERROR: {e}")

    if changed:
        session.commit()


def send_chart_task_push_event(
    session: Session,
    *,
    task: dict[str, Any],
    run_label: str,
) -> None:
    if task.get("completed"):
        audit_logger.info(
            f"WEBPUSH_TASKS_SKIP_COMPLETED: task_key={task['task_key']} run_label={run_label}"
        )
        return

    task_type = task.get("task_type") or ""
    alert_day = task.get("alert_day")
    weekday = alert_day.weekday() if alert_day else None

    # Apólice:
    # - segunda: 09h e 17h
    # - terça: apenas 09h
    if task_type == "policy_issue":
        if weekday == 1 and run_label == "17h":
            audit_logger.info(
                f"WEBPUSH_TASKS_SKIP_POLICY_TUE_17H: task_key={task['task_key']}"
            )
            return
        if weekday not in (0, 1):
            audit_logger.info(
                f"WEBPUSH_TASKS_SKIP_POLICY_INVALID_DAY: task_key={task['task_key']}"
            )
            return

    seller_id = task.get("seller_id")
    if seller_id is None:
        audit_logger.info(
            f"WEBPUSH_TASKS_SKIP_NO_SELLER: task_key={task['task_key']} run_label={run_label}"
        )
        return

    event_key = f"{task['task_key']}:{task['alert_day'].isoformat()}:{run_label}"

    ok = register_push_dispatch_once(
        session,
        event_key=event_key,
        event_type="chart_task_reminder",
        reservation_id=None,
        scheduled_for=task["alert_day"],
    )

    if not ok:
        audit_logger.info(f"WEBPUSH_TASKS_SKIP_DUPLICATE: {event_key}")
        return

    payload = build_chart_task_push_payload(task, run_label)
    send_push_payload_to_user_subscriptions(
        session,
        user_id=seller_id,
        payload=payload,
    )

    audit_logger.info(
        json.dumps(
            {
                "type": "chart_task_push_sent",
                "task_key": task["task_key"],
                "patient_name": task["patient_name"],
                "seller_id": seller_id,
                "seller_name": task["seller_name"],
                "surgery_day": task["surgery_day"].isoformat(),
                "alert_day": task["alert_day"].isoformat(),
                "run_label": run_label,
            },
            ensure_ascii=False,
        )
    )


def dispatch_chart_task_pushes(session: Session, ref_day: date, run_label: str) -> None:
    tasks = build_all_chart_tasks(
        session,
        start_day=ref_day,
        end_day=ref_day + timedelta(days=60),
        seller_user_id=None,
    )

    todays_tasks = [t for t in tasks if t["alert_day"] == ref_day]

    for task in todays_tasks:
        send_chart_task_push_event(
            session,
            task=task,
            run_label=run_label,
        )


def _next_run_chart_tasks_push_sp(now_sp: datetime) -> tuple[datetime, str]:
    run_09 = now_sp.replace(hour=9, minute=0, second=0, microsecond=0)
    run_17 = now_sp.replace(hour=17, minute=0, second=0, microsecond=0)

    if now_sp < run_09:
        return run_09, "09h"

    if now_sp < run_17:
        return run_17, "17h"

    tomorrow_09 = (now_sp + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
    return tomorrow_09, "09h"


def start_chart_tasks_push_scheduler() -> None:
    def runner():
        while True:
            nxt, run_label = _next_run_chart_tasks_push_sp(datetime.now(TZ))
            seconds = max(5, int((nxt - datetime.now(TZ)).total_seconds()))
            audit_logger.info(
                f"WEBPUSH_TASKS: próximo disparo {run_label} em {nxt.isoformat()} (sleep {seconds}s)."
            )
            pytime.sleep(seconds)

            run_day = datetime.now(TZ).date()
            with Session(engine) as session:
                try:
                    dispatch_chart_task_pushes(session, run_day, run_label)
                except Exception as e:
                    audit_logger.exception(f"WEBPUSH_TASKS_ERROR: {e}")

    t = threading.Thread(target=runner, daemon=True)
    t.start()


def register_push_dispatch_once(
    session: Session,
    *,
    event_key: str,
    event_type: str,
    reservation_id: Optional[int] = None,
    scheduled_for: Optional[date] = None,
) -> bool:
    row = PushNotificationLog(
        event_key=event_key,
        event_type=event_type,
        reservation_id=reservation_id,
        scheduled_for=scheduled_for,
    )
    session.add(row)
    try:
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False

def get_policy_alert_window_dates(ref_day: date) -> tuple[date, date] | None:
    """
    Regra:
    - Segunda: alerta para cirurgias de quinta desta semana até quarta da semana seguinte
    - Terça: mesmo conjunto da segunda imediatamente anterior
    - Outros dias: não gera lembrete
    """
    wd = ref_day.weekday()  # 0=seg, 1=ter, ..., 6=dom

    if wd == 0:
        monday = ref_day
    elif wd == 1:
        monday = ref_day - timedelta(days=1)
    else:
        return None

    start_day = monday + timedelta(days=3)   # quinta
    end_day = monday + timedelta(days=9)     # quarta seguinte
    return start_day, end_day


def build_policy_tasks(
    session: Session,
    *,
    ref_day: date,
    seller_user_id: int | None = None,
) -> list[dict[str, Any]]:
    window = get_policy_alert_window_dates(ref_day)
    if not window:
        return []

    start_surgery_day, end_surgery_day = window

    surgeons = session.exec(
        select(User).where(User.role == "doctor", User.is_active == True)
    ).all()
    surgeons_by_id = {u.id: u for u in surgeons if u.id is not None}

    sellers = session.exec(
        select(User).where(User.role == "surgery", User.is_active == True)
    ).all()
    sellers_by_id = {u.id: u for u in sellers if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= start_surgery_day,
            SurgicalMapEntry.day <= end_surgery_day,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()

    grouped: dict[str, dict[str, Any]] = {}

    for row in entries:
        surgeon = surgeons_by_id.get(row.surgeon_id)
        if not surgeon:
            continue

        if seller_user_id is not None and row.created_by_id != seller_user_id:
            continue

        task_key = build_chart_task_key(
            task_type="policy_issue",
            seller_id=row.created_by_id,
            surgery_day=row.day,
            patient_name=row.patient_name,
        )

        seller = sellers_by_id.get(row.created_by_id)

        if task_key not in grouped:
            grouped[task_key] = {
                "task_key": task_key,
                "task_type": "policy_issue",
                "task_type_label": "Apólice de Seguro",
                "patient_name": (row.patient_name or "").strip().upper(),
                "surgery_day": row.day,
                "alert_day": ref_day,
                "seller_id": row.created_by_id,
                "seller_name": seller.full_name if seller else "Sem vendedor",
                "surgeons": set(),
                "message": f"Solicitar emissão da apólice de seguro da paciente {(row.patient_name or '').strip().upper()}.",
            }

        grouped[task_key]["surgeons"].add(surgeon.full_name)

    completed_map = load_completed_chart_tasks(session)

    result: list[dict[str, Any]] = []
    for item in grouped.values():
        completion = completed_map.get(item["task_key"])
        item["surgeons"] = sorted(item["surgeons"])
        item["completed"] = bool(completion)
        item["completed_at"] = completion.get("completed_at") if completion else None
        item["completed_by"] = completion.get("completed_by") if completion else None
        result.append(item)

    result.sort(key=lambda x: (x["surgery_day"], x["patient_name"]))
    return result

def send_push_payload_to_all_active_subscriptions(session: Session, payload: dict) -> None:
    print("[PUSH] entrando em send_push_payload_to_all_active_subscriptions", flush=True)

    if not webpush_is_configured():
        audit_logger.info("WEBPUSH: ignorado (VAPID não configurado).")
        print("[PUSH] VAPID não configurado", flush=True)
        return

    print("[PUSH] VAPID configurado", flush=True)

    subs = session.exec(
        select(PushSubscription).where(PushSubscription.is_active == True)
    ).all()

    print(f"[PUSH] inscrições ativas encontradas: {len(subs)}", flush=True)

    if not subs:
        audit_logger.info("WEBPUSH: nenhuma inscrição ativa.")
        print("[PUSH] nenhuma inscrição ativa", flush=True)
        return

    changed = False

    for sub in subs:
        try:
            print(f"[PUSH] enviando para endpoint: {sub.endpoint[:120]}", flush=True)

            response = webpush(
                subscription_info={
                    "endpoint": sub.endpoint,
                    "keys": {
                        "p256dh": sub.p256dh,
                        "auth": sub.auth,
                    },
                },
                data=json.dumps(payload, ensure_ascii=False),
                vapid_private_key=WEBPUSH_VAPID_PRIVATE_KEY,
                vapid_claims={"sub": WEBPUSH_VAPID_SUBJECT},
                ttl=60,
            )

            print(f"[PUSH] envio realizado com sucesso | response={response}", flush=True)

        except WebPushException as e:
            status_code = getattr(getattr(e, "response", None), "status_code", None)
            audit_logger.exception(
                f"WEBPUSH_SEND_ERROR: endpoint={sub.endpoint[:80]} status={status_code} err={e}"
            )
            print(f"[PUSH] WebPushException | status={status_code} | erro={e}", flush=True)

            if status_code in (404, 410):
                sub.is_active = False
                sub.updated_at = datetime.utcnow()
                session.add(sub)
                changed = True

        except Exception as e:
            audit_logger.exception(f"WEBPUSH_SEND_GENERIC_ERROR: {e}")
            print(f"[PUSH] erro genérico ao enviar | tipo={type(e).__name__} | erro={e}", flush=True)

    if changed:
        session.commit()
        print("[PUSH] inscrições inválidas atualizadas no banco", flush=True)

def send_lodging_push_event(
    session: Session,
    *,
    event_type: str,
    row,
    event_key: Optional[str] = None,
    scheduled_for: Optional[date] = None,
) -> None:
    print(f"[PUSH] send_lodging_push_event chamado | event_type={event_type} | row_id={getattr(row, 'id', None)} | event_key={event_key}")

    if event_key:
        ok = register_push_dispatch_once(
            session,
            event_key=event_key,
            event_type=event_type,
            reservation_id=getattr(row, "id", None),
            scheduled_for=scheduled_for,
        )
        print(f"[PUSH] register_push_dispatch_once => {ok}")

        if not ok:
            audit_logger.info(f"WEBPUSH_SKIP_DUPLICATE: {event_key}")
            print(f"[PUSH] duplicado, envio cancelado")
            return

    payload = build_lodging_push_payload(event_type, row)
    print(f"[PUSH] payload montado => {payload}")

    send_push_payload_to_all_active_subscriptions(session, payload)


def dispatch_daily_lodging_pushes(session: Session, ref_day: date) -> None:
    checkins = session.exec(
        select(LodgingReservation).where(LodgingReservation.check_in == ref_day)
    ).all()

    checkouts = session.exec(
        select(LodgingReservation).where(LodgingReservation.check_out == ref_day)
    ).all()

    arrivals_3d = session.exec(
        select(LodgingReservation).where(LodgingReservation.check_in == (ref_day + timedelta(days=3)))
    ).all()

    for row in checkins:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="checkin_today",
            row=row,
            event_key=f"lodging:checkin_today:{ref_day.isoformat()}:{row.id}:{patient_key}",
            scheduled_for=ref_day,
        )

    for row in checkouts:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="checkout_today",
            row=row,
            event_key=f"lodging:checkout_today:{ref_day.isoformat()}:{row.id}:{patient_key}",
            scheduled_for=ref_day,
        )

    for row in arrivals_3d:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="arrival_3d",
            row=row,
            event_key=f"lodging:arrival_3d:{ref_day.isoformat()}:{row.id}:{patient_key}",
            scheduled_for=ref_day,
        )

def _next_run_lodging_push_sp(now_sp: datetime) -> datetime:
    run_today = now_sp.replace(hour=8, minute=0, second=0, microsecond=0)
    if now_sp < run_today:
        return run_today
    return run_today + timedelta(days=1)


def start_lodging_push_scheduler() -> None:
    def runner():
        while True:
            now_sp = datetime.now(TZ)
            today_sp = now_sp.date()

            if now_sp.hour >= 8:
                with Session(engine) as session:
                    try:
                        dispatch_daily_lodging_pushes(session, today_sp)
                    except Exception as e:
                        audit_logger.exception(f"WEBPUSH_DAILY_FALLBACK_ERROR: {e}")

            nxt = _next_run_lodging_push_sp(datetime.now(TZ))
            seconds = max(5, int((nxt - datetime.now(TZ)).total_seconds()))
            audit_logger.info(f"WEBPUSH_DAILY: próximo disparo em {nxt.isoformat()} (sleep {seconds}s).")
            pytime.sleep(seconds)

            run_day = datetime.now(TZ).date()
            with Session(engine) as session:
                try:
                    dispatch_daily_lodging_pushes(session, run_day)
                except Exception as e:
                    audit_logger.exception(f"WEBPUSH_DAILY_ERROR: {e}")

    t = threading.Thread(target=runner, daemon=True)
    t.start()

# ============================
# RELATÓRIO DR. GUSTAVO (snapshot diário às 19h)
# ============================

GUSTAVO_REPORT_CFG_PATH = (Path(__file__).resolve().parent / "gustavo_report_config.json")

# OVERRIDES (override vence tudo) - por dia e por médico
GUSTAVO_REPORT_OVERRIDES_PATH = (Path(__file__).resolve().parent / "gustavo_report_overrides.json")

# Ordem fixa (hierarquia/faturamento) — NÃO MUDAR
GUSTAVO_REPORT_SURGEONS = [
    ("drgustavo", "Gustavo"),
    ("drricardo", "Ricardo"),
    ("draalice", "Alice"),
    ("dramelina", "Melina"),
    ("drathamilys", "Thamilys"),
    ("dravanessa", "Vanessa"),
]

# Emojis permitidos (no relatório)
REPORT_EMOJIS = {"🟢", "🟡", "🔴", "🔵", "⚫️"}

def load_gustavo_overrides() -> dict:
    """
    Estrutura:
    {
      "YYYY-MM-DD": {
        "drgustavo": {"emoji": "🟢", "reason": "texto", "by": "johnny.ge", "at": "iso"},
        ...
      }
    }
    """
    try:
        if not GUSTAVO_REPORT_OVERRIDES_PATH.exists():
            return {}
        raw = json.loads(GUSTAVO_REPORT_OVERRIDES_PATH.read_text(encoding="utf-8") or "{}")
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}

def save_gustavo_overrides(data: dict) -> None:
    GUSTAVO_REPORT_OVERRIDES_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

def _default_gustavo_month_keys(snapshot_day_sp: date) -> list[str]:
    y0, m0 = snapshot_day_sp.year, snapshot_day_sp.month
    y1, m1 = _add_months(y0, m0, 1)
    y2, m2 = _add_months(y0, m0, 2)
    return [f"{y0:04d}-{m0:02d}", f"{y1:04d}-{m1:02d}", f"{y2:04d}-{m2:02d}"]

def load_gustavo_selected_month_keys(snapshot_day_sp: date) -> list[str]:
    try:
        if not GUSTAVO_REPORT_CFG_PATH.exists():
            return _default_gustavo_month_keys(snapshot_day_sp)
        data = json.loads(GUSTAVO_REPORT_CFG_PATH.read_text(encoding="utf-8") or "{}")
        keys = data.get("selected_months", [])
        if not isinstance(keys, list) or not keys:
            return _default_gustavo_month_keys(snapshot_day_sp)
        # filtra apenas strings tipo YYYY-MM
        ok = []
        for k in keys:
            if isinstance(k, str) and len(k) == 7 and k[4] == "-":
                ok.append(k)
        return ok or _default_gustavo_month_keys(snapshot_day_sp)
    except Exception:
        return _default_gustavo_month_keys(snapshot_day_sp)

def save_gustavo_selected_month_keys(keys: list[str]) -> None:
    payload = {"selected_months": keys, "updated_at": datetime.utcnow().isoformat()}
    GUSTAVO_REPORT_CFG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def _keys_to_month_tuples(keys: list[str]) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    for k in keys:
        try:
            yy, mm = k.split("-")
            y = int(yy)
            m = int(mm)
            if 1 <= m <= 12:
                months.append((y, m))
        except Exception:
            continue
    # ordena e remove duplicados mantendo ordem
    seen = set()
    out = []
    for ym in sorted(months):
        if ym not in seen:
            seen.add(ym)
            out.append(ym)
    return out

PT_MONTHS = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
]
DOW_ABBR = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]

def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    """Soma delta meses em (year, month). Retorna (new_year, new_month)."""
    m = month + delta
    y = year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    return y, m

def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)

def _month_end(year: int, month: int) -> date:
    import calendar as _cal
    last_day = _cal.monthrange(year, month)[1]
    return date(year, month, last_day)

def _month_label_pt(year: int, month: int) -> str:
    # você pode escolher title() no display
    return PT_MONTHS[month-1].upper()

def _proc_bucket(procedure_type: str | None) -> str:
    """
    Retorna 'cir' | 'ref' | 'simp' baseado no texto.
    - Cirurgia: 'cirurgia'
    - Refinamento: contém 'ref'
    - Procedimento simples: contém 'simp' ou 'proced'
    """
    if not procedure_type:
        return "cir"
    pt = procedure_type.strip().lower()
    if pt == "cirurgia":
        return "cir"
    if "ref" in pt:
        return "ref"
    if "simp" in pt or "proced" in pt:
        return "simp"
    return "cir"

def build_gustavo_whatsapp_messages(
    session: Session,
    snapshot_day_sp: date,
    month_keys: list[str] | None = None,
) -> tuple[str, str, dict]:
    """
    Gera as duas mensagens (Panorama + Detalhe)

    Regras:
    - meses: vêm da configuração (seleção) ou default (mês atual + 2)
    - aparecem SOMENTE Segunda (0) e Quarta (2)
    - Emojis: ✅ cheio | 🟡 parcial | 🔴 livre | 🔵 bloqueio/recesso
    - Sem descrições extras (apenas as bolinhas)
    - Sem linhas em branco entre dias do mesmo mês (apenas entre meses)
    """

    gustavo = session.exec(select(User).where(User.username == "drgustavo")).first()
    if not gustavo:
        raise RuntimeError("Usuário drgustavo não encontrado no banco.")

    # 1) resolve meses a usar
    if month_keys is None:
        month_keys = load_gustavo_selected_month_keys(snapshot_day_sp)
    months = _keys_to_month_tuples(month_keys)
    if not months:
        months = _keys_to_month_tuples(_default_gustavo_month_keys(snapshot_day_sp))

    months_titles = " • ".join(
        f"{PT_MONTHS[mm-1].title()}/{str(yy)[2:]}" for (yy, mm) in months
    )

    period_start = _month_start(months[0][0], months[0][1])
    period_end = _month_end(months[-1][0], months[-1][1])

    # --- coleta dados para o relatório (6 médicos) ---
    # carrega usuários
    surgeons_map: dict[str, User] = {}
    for (uname, _lbl) in GUSTAVO_REPORT_SURGEONS:
        u = session.exec(select(User).where(User.username == uname)).first()
        if u:
            surgeons_map[uname] = u

    surgeon_ids = [u.id for u in surgeons_map.values() if u.id is not None]

    # pega todos os agendamentos no período (somente dos 6 médicos)
    all_entries = []
    if surgeon_ids:
        all_entries = session.exec(
            select(SurgicalMapEntry).where(
                SurgicalMapEntry.day >= period_start,
                SurgicalMapEntry.day <= period_end,
                SurgicalMapEntry.surgeon_id.in_(surgeon_ids),
                or_(
                    SurgicalMapEntry.status == None,        # compat com registros antigos
                    SurgicalMapEntry.status == "approved",  # só conta aprovados
                )
            )
        ).all()

    # organiza por dia e por username (NÃO contar pre-reservation)
    entries_by_day_user: dict[date, dict[str, list[SurgicalMapEntry]]] = {}
    month_real_counts: dict[tuple[int, int], dict[str, int]] = {}

    id_to_username = {u.id: uname for (uname, _lbl) in GUSTAVO_REPORT_SURGEONS for u in [surgeons_map.get(uname)] if u}

    for e in all_entries:
        if getattr(e, "is_pre_reservation", False):
            continue
        if getattr(e, "status", None) == "pending_approval":
            continue
        if not getattr(e, "day", None) or not getattr(e, "surgeon_id", None):
            continue

        uname = id_to_username.get(e.surgeon_id)
        if not uname:
            continue

        entries_by_day_user.setdefault(e.day, {}).setdefault(uname, []).append(e)

        ym = (e.day.year, e.day.month)
        month_real_counts.setdefault(ym, {})
        month_real_counts[ym][uname] = month_real_counts[ym].get(uname, 0) + 1

    # overrides (vence tudo)
    overrides = load_gustavo_overrides()

    pano_lines: list[str] = [
        "RELATÓRIO – VISÃO GERAL (AGENDA CIRÚRGICA)",
        f"📅 {months_titles}",
        ""
    ]


    detail_parts: list[str] = []
    months_payload = []

    for (yy, mm) in months:
        m_start = _month_start(yy, mm)
        m_end = _month_end(yy, mm)

        # Cabeçalho do mês
        detail_parts.append(f"*{_month_label_pt(yy, mm)} – VISÃO GERAL*")
        detail_parts.append("Legenda: Gustavo-Ricardo-Alice-Melina-Thamilys-Vanessa")

        # mês “todo azul” por médico se não teve NENHUM agendamento real no mês
        month_counts = month_real_counts.get((yy, mm), {})
        month_all_blue = {u: (month_counts.get(u, 0) == 0) for (u, _lbl) in GUSTAVO_REPORT_SURGEONS}

        lines: list[str] = []

        d = m_start
        while d <= m_end:
            dow = d.weekday()  # 0=Seg ... 5=Sáb ... 6=Dom

            # mostra Seg-Sex sempre
            show_day = dow in (0, 1, 2, 3, 4)

            # Sábado só aparece se houver agendamento real no sistema (qualquer um dos 6)
            if dow == 5:
                any_real_sat = False
                day_bucket = entries_by_day_user.get(d, {})
                for (uname, _lbl) in GUSTAVO_REPORT_SURGEONS:
                    if len(day_bucket.get(uname, [])) > 0:
                        any_real_sat = True
                        break
                show_day = any_real_sat

            # Domingo nunca
            if dow == 6:
                show_day = False

            if not show_day:
                d += timedelta(days=1)
                continue

            day_bucket = entries_by_day_user.get(d, {})
            day_over = (overrides.get(d.isoformat()) or {})

            # atalhos p/ Ricardo x Alice
            ric_real = len(day_bucket.get("drricardo", []))
            ali_real = len(day_bucket.get("draalice", []))

            emojis_line: list[str] = []

            for (uname, _lbl) in GUSTAVO_REPORT_SURGEONS:
                # override vence tudo
                if uname in day_over:
                    ov_emoji = (day_over[uname] or {}).get("emoji")
                    if isinstance(ov_emoji, str) and ov_emoji in REPORT_EMOJIS:
                        emojis_line.append(ov_emoji)
                        continue

                # mês inteiro azul se não operou nada
                if uname == "drgustavo" and month_all_blue.get(uname, False):
                    emojis_line.append("🔵")
                    continue

                uobj = surgeons_map.get(uname)
                if not uobj:
                    emojis_line.append("🔴")
                    continue

                # bloqueio por agenda (azul)
                if validate_mapa_block_rules(session, d, uobj.id):
                    emojis_line.append("🔵")
                    continue

                real_cnt = len(day_bucket.get(uname, []))

                # -------------------------
                # REGRAS POR MÉDICO
                # -------------------------

                # GUSTAVO
                if uname == "drgustavo":
                    if dow in (0, 2):  # Seg/Qua
                        if real_cnt >= 2:
                            emojis_line.append("🟢")
                        elif real_cnt == 1:
                            emojis_line.append("🟡")
                        else:
                            emojis_line.append("🔴")
                    elif dow in (1, 3):  # Ter/Qui (auxilia) => default preto
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")
                    elif dow == 4:  # Sex (refino) => default preto; se tiver agendamento => verde
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")
                    else:
                        # Sábado (se apareceu) entra na lógica “sem destaques”: aberto
                        emojis_line.append("🟢" if real_cnt >= 1 else "🔴")
                    continue

                # RICARDO / ALICE
                if uname in ("drricardo", "draalice"):

                    # SEGUNDA E QUARTA → AUXILIAM GUSTAVO
                    if dow in (0, 2):
                        # default preto, verde apenas se houver agendamento próprio
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    # TERÇA, QUINTA E SEXTA → OPERÁVEIS COM EXCLUSIVIDADE
                    elif dow in (1, 3, 4):
                        if uname == "drricardo":
                            if ric_real > 0:
                                emojis_line.append("🟢")
                            elif ali_real > 0:
                                emojis_line.append("⚫️")
                            else:
                                emojis_line.append("🔴")
                        else:  # draalice
                            if ali_real > 0:
                                emojis_line.append("🟢")
                            elif ric_real > 0:
                                emojis_line.append("⚫️")
                            else:
                                emojis_line.append("🔴")

                    # SÁBADO → PRETO, VERDE SE HOUVER AGENDAMENTO
                    elif dow == 5:
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    else:
                        emojis_line.append("🔴")

                    continue

                # THAMILYS
                if uname == "drathamilys":

                    # SEG / QUA → AUXILIA GUSTAVO
                    if dow in (0, 2):
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    # TERÇA → SEMPRE PRETO
                    elif dow == 1:
                        emojis_line.append("⚫️")

                    # QUINTA E SEXTA → OPERÁVEL
                    elif dow in (3, 4):
                        emojis_line.append("🟢" if real_cnt >= 1 else "🔴")

                    # SÁBADO
                    elif dow == 5:
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    else:
                        emojis_line.append("🔴")

                    continue

                # MELLINA
                if uname in ("dramelina","dravanessa"):

                    # SEG / QUA → AUXILIA GUSTAVO
                    if dow in (0, 2):
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    # TER / QUI / SEX → OPERÁVEL
                    elif dow in (1, 3, 4):
                        emojis_line.append("🟢" if real_cnt >= 1 else "🔴")

                    # SÁBADO
                    elif dow == 5:
                        emojis_line.append("🟢" if real_cnt >= 1 else "⚫️")

                    else:
                        emojis_line.append("🔴")

                    continue


            lines.append(f"{DOW_ABBR[dow]} {d.strftime('%d/%m')}  {''.join(emojis_line)}")
            d += timedelta(days=1)

        detail_parts.extend(lines)

        # separador SOMENTE entre meses (uma linha em branco)
        detail_parts.append("")

    message_1 = "\n".join(detail_parts).strip()
    message_2 = ""

    payload = {
        "doctor_username": "drgustavo",
        "snapshot_day_sp": snapshot_day_sp.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
    }

    return message_1, message_2, payload

def _whatsapp_send(message_1: str, message_2: str) -> None:
    """
    Disparo via API (opcional).
    Só envia se WHATSAPP_API_URL / WHATSAPP_API_TOKEN / WHATSAPP_TO estiverem configuradas.
    """
    import requests

    url = os.getenv("WHATSAPP_API_URL", "").strip()
    token = os.getenv("WHATSAPP_API_TOKEN", "").strip()
    to = os.getenv("WHATSAPP_TO", "").strip()

    if not url or not token or not to:
        audit_logger.info("WHATSAPP: envio ignorado (WHATSAPP_API_URL/TOKEN/TO não configurados).")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Ajuste conforme seu provedor (BotConversa/Twilio/etc.)
    payload = {"to": to, "messages": [message_1, message_2]}

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        audit_logger.info(f"WHATSAPP: status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        audit_logger.exception(f"WHATSAPP: erro ao enviar: {e}")

def save_gustavo_snapshot_and_send(session: Session, snapshot_day_sp: date) -> GustavoAgendaSnapshot:
    """Gera e salva snapshot do dia (idempotente por snapshot_date)."""

    existing = session.exec(
        select(GustavoAgendaSnapshot).where(GustavoAgendaSnapshot.snapshot_date == snapshot_day_sp)
    ).first()
    if existing:
        return existing

    msg1, msg2, payload = build_gustavo_whatsapp_messages(session, snapshot_day_sp, month_keys=None)

    snap = GustavoAgendaSnapshot(
        snapshot_date=snapshot_day_sp,
        generated_at=datetime.utcnow(),
        period_start=date.fromisoformat(payload["period_start"]),
        period_end=date.fromisoformat(payload["period_end"]),
        message_1=msg1,
        message_2=msg2,
        payload=payload,
    )

    session.add(snap)
    try:
        session.commit()
    except IntegrityError:
        # idempotência em ambientes com +1 worker (Render/Uvicorn)
        session.rollback()
        existing = session.exec(
            select(GustavoAgendaSnapshot).where(GustavoAgendaSnapshot.snapshot_date == snapshot_day_sp)
        ).first()
        if existing:
            return existing
        raise

    session.refresh(snap)

    # dispara WhatsApp usando o texto salvo
    _whatsapp_send(msg1, msg2)

    return snap

def _next_run_19h_sp(now_sp: datetime) -> datetime:
    run_today = now_sp.replace(hour=19, minute=0, second=0, microsecond=0)
    if now_sp < run_today:
        return run_today
    return run_today + timedelta(days=1)

def start_gustavo_snapshot_scheduler() -> None:
    """
    Scheduler simples (thread)
    - roda diariamente às 19h (horário SP)
    - fallback (Opção A): ao subir, se já passou de 19h e ainda não existe snapshot de hoje, gera imediatamente
    """

    def runner():
        while True:
            now_sp = datetime.now(TZ)
            today_sp = now_sp.date()

            # fallback: se já passou de 19h e não existe snapshot hoje, gera agora
            if now_sp.hour >= 19:
                with Session(engine) as session:
                    exists = session.exec(
                        select(GustavoAgendaSnapshot).where(GustavoAgendaSnapshot.snapshot_date == today_sp)
                    ).first()
                    if not exists:
                        audit_logger.info(f"GUSTAVO_SNAPSHOT: fallback do dia {today_sp} (app subiu após 19h).")
                        save_gustavo_snapshot_and_send(session, today_sp)

            # dorme até o próximo 19h
            nxt = _next_run_19h_sp(datetime.now(TZ))
            seconds = max(5, int((nxt - datetime.now(TZ)).total_seconds()))
            audit_logger.info(f"GUSTAVO_SNAPSHOT: próximo disparo em {nxt.isoformat()} (sleep {seconds}s).")
            pytime.sleep(seconds)

            # roda o snapshot do dia (19h)
            run_day = datetime.now(TZ).date()
            with Session(engine) as session:
                try:
                    audit_logger.info(f"GUSTAVO_SNAPSHOT: gerando snapshot do dia {run_day} (19h).")
                    save_gustavo_snapshot_and_send(session, run_day)
                except Exception as e:
                    audit_logger.exception(f"GUSTAVO_SNAPSHOT: erro ao gerar/enviar: {e}")

    t = threading.Thread(target=runner, daemon=True)
    t.start()

def validate_mapa_block_rules(session: Session, day: date, surgeon_id: int) -> str | None:
    # pega qualquer bloqueio que intersecte o dia
    blocks = session.exec(
        select(AgendaBlock).where(
            AgendaBlock.start_date <= day,
            AgendaBlock.end_date >= day,
        )
    ).all()

    if not blocks:
        return None

    # se existir algum "applies_to_all" no dia, já bloqueia
    for b in blocks:
        if b.applies_to_all:
            return f"Data bloqueada: {b.reason}"

    # caso contrário, bloqueia se o cirurgião estiver no grupo do bloqueio
    block_ids = [b.id for b in blocks if b.id is not None]
    if not block_ids:
        return None

    rel = session.exec(
        select(AgendaBlockSurgeon).where(
            AgendaBlockSurgeon.block_id.in_(block_ids),
            AgendaBlockSurgeon.surgeon_id == surgeon_id,
        )
    ).first()

    if rel:
        return "Data bloqueada para este profissional."

    return None

def compute_month_availability(
    session: Session,
    surgeon_id: int,
    month_ym: str,
    procedure_type: str,
) -> list[dict[str, str]]:
    """
    Retorna lista de datas operáveis no mês para o cirurgião + tipo de procedimento,
    respeitando:
      - validate_mapa_rules
      - validate_mapa_block_rules
      - reserva = agendamento
    Mostra só 🔴 (livre) e 🟡 (parcial). Dias lotados NÃO retornam.
    """

    selected_month, first_day, next_first, days = safe_selected_month(month_ym)

    surgeon = session.exec(select(User).where(User.id == surgeon_id)).first()
    if not surgeon:
        return []

    results: list[dict[str, str]] = []

    weekday_map = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]

    # Para o emoji 🟡 precisamos saber a capacidade do dia (no caso do Gustavo)
    gustavo = session.exec(select(User).where(User.full_name == "Dr. Gustavo Aquino")).first()

    for d in days:
        # 1) bloqueios
        block_err = validate_mapa_block_rules(session, d, surgeon_id)
        if block_err:
            continue

        # 2) regras de agenda (usa o mesmo motor do create/edit)
        err = validate_mapa_rules(
            session=session,
            day=d,
            surgeon_id=surgeon_id,
            procedure_type=procedure_type,
            uses_hsr=False,   # consulta não define HSR; se quiser, adiciona no card depois
            exclude_entry_id=None,
        )
        if err:
            # inclui "dia fora do padrão" e "dia lotado" -> não aparece
            continue

        # 3) conta ocupações do cirurgião no dia (inclui reservas)
        cnt = session.exec(
            select(func.count()).select_from(SurgicalMapEntry).where(
                SurgicalMapEntry.day == d,
                SurgicalMapEntry.surgeon_id == surgeon_id,
                SurgicalMapEntry.status == "approved",
            )
        ).one()

        # 4) define capacidade do dia para o emoji (só Gustavo pode gerar 🟡 com cap=2)
        cap = 1
        if gustavo and surgeon_id == gustavo.id:
            wd = d.weekday()
            if procedure_type == "Refinamento" and wd == 4:
                cap = 1
            else:
                cap = 2

        # só 🔴 e 🟡 (dias lotados não chegam aqui, mas garantimos)
        if cnt <= 0:
            emoji = "🔴"
        elif cnt < cap:
            emoji = "🟡"
        else:
            continue  # lotado -> não aparece

        results.append(
            {
                "day_iso": d.isoformat(),
                "label": d.strftime("%d/%m"),
                "human": f"{d.strftime('%d/%m/%Y')} - {weekday_map[d.weekday()]}",
                "emoji": emoji,
            }
        )

    return results

def compute_priority_card(session: Session) -> dict:
    today = datetime.now(TZ).date()
    end = today + timedelta(days=90)  # janela “hoje até +90”

    gustavo = session.exec(select(User).where(User.full_name == "Dr. Gustavo Aquino")).first()
    if not gustavo:
        return {"mode": "red", "items": []}

    # 1) pega bloqueios que intersectam a janela
    blocks = session.exec(
        select(AgendaBlock).where(
            AgendaBlock.start_date <= end,
            AgendaBlock.end_date >= today,
        )
    ).all()

    block_ids = [b.id for b in blocks if b.id is not None]

    rels = []
    if block_ids:
        rels = session.exec(
            select(AgendaBlockSurgeon).where(AgendaBlockSurgeon.block_id.in_(block_ids))
        ).all()

    surgeons_by_block: dict[int, list[int]] = {}
    for r in rels:
        surgeons_by_block.setdefault(r.block_id, []).append(r.surgeon_id)
        
    # ✅ precisamos do "surgeons" aqui dentro (escopo da função)
    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    surgeons_name_by_id = {s.id: s.full_name for s in surgeons if s.id is not None}
    block_surgeons_map: dict[int, list[str]] = {}

    for b in blocks:
        if not b.id:
            continue
        if b.applies_to_all:
            block_surgeons_map[b.id] = ["Todos"]
        else:
            ids = surgeons_by_block.get(b.id, [])
            names = [surgeons_name_by_id.get(sid) for sid in ids]
            block_surgeons_map[b.id] = [n for n in names if n] or ["—"]

    blocked_days: set[date] = set()

    for b in blocks:
        # bloqueio geral
        if b.applies_to_all:
            start = max(b.start_date, today)
            finish = min(b.end_date, end)
            d = start
            while d <= finish:
                blocked_days.add(d)
                d += timedelta(days=1)
            continue

        # bloqueio por grupo: só conta se o Gustavo estiver no grupo
        if gustavo and gustavo.id in surgeons_by_block.get(b.id or -1, []):
            start = max(b.start_date, today)
            finish = min(b.end_date, end)
            d = start
            while d <= finish:
                blocked_days.add(d)
                d += timedelta(days=1)

    days = []
    for i in range(0, 91):  # inclui a data final (ex.: 04/12 a 04/03)
        d = today + timedelta(days=i)
        if d.weekday() not in (0, 2):  # só segunda (0) e quarta (2)
            continue
        if d in blocked_days:
            continue
        days.append(d)

    counts: dict[date, int] = {}
    for d in session.exec(
        select(SurgicalMapEntry.day).where(
            SurgicalMapEntry.day >= today,
            SurgicalMapEntry.day <= end,
            SurgicalMapEntry.surgeon_id == gustavo.id,
            SurgicalMapEntry.status == "approved",
        )
    ).all():
        counts[d] = counts.get(d, 0) + 1

    zeros = [d for d in days if counts.get(d, 0) == 0]
    if zeros:
        return {
            "mode": "red",
            "items": [
                f"{d.strftime('%d/%m/%Y')} - Em {(d - today).days} dias"
                for d in zeros
            ],
        }

    ones = [d for d in days if counts.get(d, 0) == 1]
    if ones:
        return {
            "mode": "yellow",
            "items": [
                f"{d.strftime('%d/%m/%Y')} - Em {(d - today).days} dias"
                for d in ones
            ],
        }

    # se não tem zeros nem ones, então está tudo com 2+
    return {"mode": "green", "items": []}

def migrate_sqlite_schema(engine):
    """
    Migração idempotente do SQLite.
    Ajusta a tabela agendablock (antiga) para o novo modelo:
      - start_date / end_date
      - reason
      - applies_to_all
    E cria a tabela de relação AgendaBlockSurgeon se não existir.
    """

    def _has_column(conn, table: str, col: str) -> bool:
        rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
        return any(r[1] == col for r in rows)  # r[1] = nome da coluna

    def _add_column_if_missing(conn, table: str, col: str, col_type: str):
        if not _has_column(conn, table, col):
            conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    with engine.begin() as conn:
        # =========================
        # Migração de agendablock
        # =========================
        agenda_tables = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agendablock';"
        ).fetchall()

        if agenda_tables:
            # --- Novas colunas do modelo atual ---
            _add_column_if_missing(conn, "agendablock", "start_date", "DATE")
            _add_column_if_missing(conn, "agendablock", "end_date", "DATE")
            _add_column_if_missing(conn, "agendablock", "reason", "TEXT")
            _add_column_if_missing(conn, "agendablock", "applies_to_all", "INTEGER DEFAULT 0")

            # --- Backfill a partir do schema antigo, se existir ---
            has_old_date = _has_column(conn, "agendablock", "data")
            has_old_reason = _has_column(conn, "agendablock", "motivo")
            has_old_prof = _has_column(conn, "agendablock", "profissional")

            if has_old_date:
                conn.exec_driver_sql("""
                    UPDATE agendablock
                    SET start_date = COALESCE(start_date, data),
                        end_date   = COALESCE(end_date, data)
                    WHERE data IS NOT NULL;
                """)

            if has_old_reason:
                conn.exec_driver_sql("""
                    UPDATE agendablock
                    SET reason = COALESCE(reason, motivo)
                    WHERE motivo IS NOT NULL;
                """)

            if has_old_prof:
                conn.exec_driver_sql("""
                    UPDATE agendablock
                    SET applies_to_all = CASE
                            WHEN applies_to_all IS NULL THEN
                                CASE WHEN lower(profissional)='todos' THEN 1 ELSE 0 END
                            ELSE applies_to_all
                        END;
                """)

            conn.exec_driver_sql("""
                CREATE TABLE IF NOT EXISTS agendablocksurgeon (
                    block_id INTEGER NOT NULL,
                    surgeon_id INTEGER NOT NULL,
                    PRIMARY KEY (block_id, surgeon_id)
                );
            """)

        # =========================
        # Migração de reservationrequest
        # =========================
        request_tables = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='reservationrequest';"
        ).fetchall()

        if request_tables:
            _add_column_if_missing(conn, "reservationrequest", "decide_by_id", "INTEGER")
            _add_column_if_missing(conn, "reservationrequest", "decided_at", "DATETIME")

def get_commercial_period(month_year: str) -> tuple[datetime, datetime]:
    """
    Retorna (start_datetime_utc_naive, end_datetime_utc_naive) do período comercial:
    - padrão: dia 25 do mês anterior até dia 24 do mês selecionado
    - exceção: Janeiro/2026 começa em 06/01/2026
    """

    tz = ZoneInfo("America/Sao_Paulo")
    year, month = map(int, month_year.split("-"))

    # início padrão: dia 25 do mês anterior (em horário SP)
    if month == 1:
        start_sp = datetime(year - 1, 12, 25, 0, 0, 0, tzinfo=tz)
    else:
        start_sp = datetime(year, month - 1, 25, 0, 0, 0, tzinfo=tz)

    # fim padrão: dia 24 do mês atual (em horário SP)
    end_sp = datetime(year, month, 24, 23, 59, 59, tzinfo=tz)

    # 🚨 EXCEÇÃO: Janeiro/2026
    if year == 2026 and month == 1:
        start_sp = datetime(2026, 1, 6, 0, 0, 0, tzinfo=tz)

    # Converte para UTC e remove tzinfo (para bater com created_at = utcnow() naive)
    start_utc_naive = start_sp.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc_naive = end_sp.astimezone(timezone.utc).replace(tzinfo=None)

    return start_utc_naive, end_utc_naive
@app.get("/slot_hsr", response_class=HTMLResponse)
def slot_hsr_page(
    request: Request,
    year: Optional[int] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    if not year:
        year = date.today().year

    payload = build_slot_hsr_data(session, year)

    return templates.TemplateResponse(
        "slot_hsr.html",
        {
            "request": request,
            "current_user": user,
            **payload,
        },
    )

@app.post("/slot_hsr/{entry_id}/refresh")
def slot_hsr_refresh_entry(
    entry_id: int,
    request: Request,
    year: Optional[int] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    if user.username != "johnny.ge":
        raise HTTPException(status_code=403, detail="Apenas o usuário master pode reprocessar slots.")

    entry = session.get(SurgicalMapEntry, entry_id)
    if not entry:
        return redirect(f"/slot_hsr?year={year or date.today().year}")

    before_items = session.exec(
        select(SurgeryProcedureItem)
        .where(SurgeryProcedureItem.surgery_entry_id == entry_id)
        .order_by(SurgeryProcedureItem.id)
    ).all()
    before_slot_type = classify_hsr_slot_from_items(before_items)

    refresh_surgery_procedure_items(session, surgery_entry_id=entry_id)

    after_items = session.exec(
        select(SurgeryProcedureItem)
        .where(SurgeryProcedureItem.surgery_entry_id == entry_id)
        .order_by(SurgeryProcedureItem.id)
    ).all()
    after_slot_type = classify_hsr_slot_from_items(after_items)

    audit_event(
        request,
        user,
        "slot_hsr_refresh",
        success=True,
        target_type="surgical_map_entry",
        target_id=entry_id,
        message=f"Slot HSR reprocessado: {before_slot_type} -> {after_slot_type}",
        extra={
            "entry_id": entry_id,
            "before_slot_type": before_slot_type,
            "after_slot_type": after_slot_type,
        },
    )

    return redirect(f"/slot_hsr?year={year or date.today().year}")

@app.get("/consulta_disponibilidade", response_class=HTMLResponse)
def consulta_disponibilidade_page(
    request: Request,
    month: Optional[str] = None,
    do_search: Optional[str] = None,
    surgeon_id: Optional[int] = None,
    procedure_type: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"), "Acesso restrito à Consulta de Disponibilidade.")

    selected_month, _, _, _ = safe_selected_month(month)

    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    selected_procedure_type = procedure_type or "Cirurgia"
    results: list[dict[str, str]] = []

    if do_search == "1" and surgeon_id:
        results = compute_month_availability(
            session=session,
            surgeon_id=int(surgeon_id),
            month_ym=selected_month,
            procedure_type=selected_procedure_type,
        )

    return templates.TemplateResponse(
        "consulta_disponibilidade.html",
        {
            "request": request,
            "current_user": user,
            "title": "Consulta de Disponibilidade",
            "selected_month": selected_month,
            "surgeons": surgeons,
            "selected_surgeon_id": surgeon_id,
            "selected_procedure_type": selected_procedure_type,
            "results": results,
            "did_search": do_search == "1",
        },
    )

@app.get("/comissoes")
def comissoes_page(
    request: Request,
    month_year: str,
    seller_id: str  | None = None,
    session: Session = Depends(get_session),
):
    """
    Relatório de comissões por cirurgia agendada:
    - procedure_type == "Cirurgia"
    - não pode ser reserva (is_pre_reservation == False)
    - período comercial (25->24, com exceção jan/2026 a partir de 06/01/2026)
    - agrupado por vendedor (created_by_id)
    """

    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "comissao"))

    period_start, period_end = get_commercial_period(month_year)
    
    seller_id_int: int | None = None
    if seller_id and seller_id.strip():
        try:
            seller_id_int = int(seller_id)
        except ValueError:
            seller_id_int = None

    # 1) Subquery: pega o primeiro agendamento (created_at mais antigo) por paciente
    first_created_subq = (
        select(
            SurgicalMapEntry.patient_name,
            func.min(SurgicalMapEntry.created_at).label("first_created_at"),
        )
        .where(
            SurgicalMapEntry.procedure_type == "Cirurgia",
            SurgicalMapEntry.is_pre_reservation == False,
            SurgicalMapEntry.patient_name.is_not(None),
            SurgicalMapEntry.patient_name != "",
        )
        .group_by(SurgicalMapEntry.patient_name)
        .subquery()
    )

    # 2) Query principal: só traz as cirurgias que são o PRIMEIRO agendamento do paciente
    q = (
        select(SurgicalMapEntry)
        .join(
            first_created_subq,
            (SurgicalMapEntry.patient_name == first_created_subq.c.patient_name)
            & (SurgicalMapEntry.created_at == first_created_subq.c.first_created_at),
        )
        .where(
            SurgicalMapEntry.created_at >= period_start,
            SurgicalMapEntry.created_at <= period_end,
        )
    )

    if seller_id_int is not None:
        q = q.where(SurgicalMapEntry.created_by_id == seller_id_int)

    entries = session.exec(q).all()

    # mapa de usuários (para resolver nome do vendedor pelo created_by_id)
    users = session.exec(select(User)).all()
    users_by_id = {u.id: u for u in users}

    # lista de vendedores para o filtro (somente quem pode “vender”)
    sellers = [u for u in users if u.role in ("admin", "surgery") and u.is_active]

    # Agrupamento por vendedor (nome vem do users_by_id)
    grouped: dict[str, list[SurgicalMapEntry]] = {}

    for e in entries:
        seller_name = "Sem vendedor"
        if e.created_by_id and e.created_by_id in users_by_id:
            seller_name = users_by_id[e.created_by_id].full_name

        grouped.setdefault(seller_name, []).append(e)

    # Ordenar cirurgias dentro de cada vendedor (mais recentes primeiro)
    for k in grouped:
        grouped[k].sort(key=lambda x: x.created_at, reverse=True)

    return templates.TemplateResponse(
        "comissoes.html",
        {
            "request": request,
            "current_user": user,
            "month_year": month_year,
            "period_start": period_start,
            "period_end": period_end,
            "grouped": grouped,
            "total": len(entries),
            "sellers": sellers,
            "seller_id": seller_id,
            "users_by_id": users_by_id,  # opcional (se quiser mostrar algo extra no template)
        },
    )

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

    # ✅ MIGRAÇÃO DO BANCO ANTIGO -> NOVO
    migrate_sqlite_schema(engine)

    with Session(engine) as session:
        seed_if_empty(session)

    # ✅ Snapshot diário (19h) - Relatório Dr. Gustavo
    start_gustavo_snapshot_scheduler()
    
    # ✅ Push diário de hospedagem
    start_lodging_push_scheduler()

    # ✅ Push diário das tasks do mapa cirúrgico
    start_chart_tasks_push_scheduler()

@app.get("/", response_class=HTMLResponse)
def home(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    if user.role == "admin":
        return redirect("/admin")
    if user.role == "doctor":
        return redirect("/doctor")
    if user.role == "surgery":
        return redirect("/mapa")
    if user.role == "viewer":
        return redirect("/hotel_mobile")
    if user.role == "comissao":
        # redireciona para o mês atual (você pode manter manual também)
        today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
        # regra do “mês comercial”: se hoje >= 25, isso pertence ao próximo month_year
        if today.day >= 25:
            y = today.year + (1 if today.month == 12 else 0)
            m = 1 if today.month == 12 else today.month + 1
        else:
            y = today.year
            m = today.month
        month_year = f"{y:04d}-{m:02d}"
        return redirect(f"/comissoes?month_year={month_year}")

    return redirect("/login")


@app.get("/app", response_class=HTMLResponse)
def app_entry(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login?next=/app")

    if user.role == "viewer":
        return redirect("/hotel_mobile")

    if user.role == "surgery":
        return redirect("/tasks")

    return redirect("/mapa")


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = ""):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "current_user": None,
            "next": next or "",
        },
    )


@app.post("/login", response_class=HTMLResponse)
def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form(""),
    session: Session = Depends(get_session),
):
    user = session.exec(
        select(User).where(User.username == username, User.is_active == True)
    ).first()
    if not user or not verify_password(password, user.password_hash):
        audit_event(
            request,
            user,  # pode ser None (ok)
            "login_failed",
            success=False,
            message="Usuário ou senha inválidos.",
            extra={"username": username},
        )
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Usuário ou senha inválidos.",
                "current_user": None,
                "next": next or "",
            },
            status_code=401,
        )

    request.session["user_id"] = user.id
    audit_event(request, user, "login_success")

    pending_feegow_alerts = get_pending_feegow_alerts_for_user(session, user.id)
    if pending_feegow_alerts:
        request.session["feegow_alert_gate_required"] = True
        return redirect("/auditoria_feegow/alertas")

    request.session["feegow_alert_gate_required"] = False

    next_path = (next or "").strip()
    if next_path.startswith("/") and not next_path.startswith("//"):
        return redirect(next_path)

    return redirect("/")

@app.post("/logout")
def logout(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    audit_event(request, user, "logout")
    request.session.clear()
    return redirect("/login")

@app.get("/auditoria_feegow/alertas", response_class=HTMLResponse)
def auditoria_feegow_alertas_page(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    pending_alerts = get_pending_feegow_alerts_for_user(session, user.id)

    if not pending_alerts:
        request.session["feegow_alert_gate_required"] = False
        return redirect("/")

    request.session["feegow_alert_gate_required"] = True

    latest_run = get_latest_feegow_run(session)

    audit_event(
        request,
        user,
        "feegow_alert_gate_view",
        target_type="feegow_validation",
        target_id=latest_run.id if latest_run else None,
        message="Usuário visualizou a tela obrigatória de alertas da Auditoria Feegow.",
        extra={"pending_count": len(pending_alerts)},
    )

    return templates.TemplateResponse(
        "auditoria_feegow_alertas.html",
        {
            "request": request,
            "current_user": user,
            "title": "Alertas da Auditoria Feegow",
            "pending_alerts": pending_alerts,
            "latest_run": latest_run,
        },
    )

@app.post("/auditoria_feegow/alertas/ciencia")
def auditoria_feegow_alertas_ciencia(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    pending_alerts = get_pending_feegow_alerts_for_user(session, user.id)

    if not pending_alerts:
        return redirect("/")

    for alert in pending_alerts:
        session.add(
            FeegowValidationAcknowledgement(
                validation_result_id=alert.id,
                ack_user_id=user.id,
                ack_message="Usuário deu ciência dos alertas pendentes no login.",
            )
        )

    session.commit()

    request.session["feegow_alert_gate_required"] = False

    latest_run = get_latest_feegow_run(session)

    audit_event(
        request,
        user,
        "feegow_alert_gate_ack",
        target_type="feegow_validation",
        target_id=latest_run.id if latest_run else None,
        message="Usuário confirmou ciência dos alertas pendentes da Auditoria Feegow.",
        extra={"ack_count": len(pending_alerts)},
    )

    return redirect("/")

USER_ROLE_OPTIONS = [
    ("admin", "Admin"),
    ("surgery", "Cirúrgico"),
    ("doctor", "Médico"),
    ("viewer", "Visualizador"),
    ("comissao", "Comissão"),
]


@app.get("/usuarios", response_class=HTMLResponse)
def usuarios_page(
    request: Request,
    err: str = "",
    ok: str = "",
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge", "Acesso restrito.")

    users = session.exec(
        select(User).order_by(User.is_active.desc(), User.full_name, User.username)
    ).all()

    return templates.TemplateResponse(
        "usuarios.html",
        {
            "request": request,
            "current_user": user,
            "title": "Usuários e Acessos",
            "users": users,
            "role_options": USER_ROLE_OPTIONS,
            "err": err,
            "ok": ok,
        },
    )


@app.post("/usuarios/create")
def usuarios_create(
    request: Request,
    full_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    is_active: Optional[str] = Form(None),
    session: Session = Depends(get_session),
):
    actor = get_current_user(request, session)
    if not actor:
        return redirect("/login")
    require(actor.username == "johnny.ge", "Acesso restrito.")

    full_name = (full_name or "").strip()
    username = (username or "").strip().lower()
    password = (password or "").strip()
    role = (role or "").strip()

    valid_roles = {value for value, _ in USER_ROLE_OPTIONS}

    if not full_name:
        return redirect("/usuarios?err=" + quote("Informe o nome completo."))
    if not username:
        return redirect("/usuarios?err=" + quote("Informe o username."))
    if not password:
        return redirect("/usuarios?err=" + quote("Informe a senha inicial."))
    if role not in valid_roles:
        return redirect("/usuarios?err=" + quote("Perfil inválido."))

    exists = session.exec(select(User).where(User.username == username)).first()
    if exists:
        return redirect("/usuarios?err=" + quote("Já existe um usuário com esse username."))

    row = User(
        full_name=full_name,
        username=username,
        role=role,
        password_hash=hash_password(password),
        is_active=bool(is_active),
    )
    session.add(row)
    session.commit()
    session.refresh(row)

    audit_event(
        request,
        actor,
        "user_created",
        target_type="user",
        target_id=row.id,
        message=f"Usuário criado: {row.username}",
        extra={
            "full_name": row.full_name,
            "username": row.username,
            "role": row.role,
            "is_active": row.is_active,
        },
    )

    return redirect("/usuarios?ok=" + quote("Usuário criado com sucesso."))


@app.post("/usuarios/update/{user_id}")
def usuarios_update(
    request: Request,
    user_id: int,
    full_name: str = Form(...),
    username: str = Form(...),
    role: str = Form(...),
    new_password: str = Form(""),
    is_active: Optional[str] = Form(None),
    session: Session = Depends(get_session),
):
    actor = get_current_user(request, session)
    if not actor:
        return redirect("/login")
    require(actor.username == "johnny.ge", "Acesso restrito.")

    row = session.get(User, user_id)
    if not row:
        return redirect("/usuarios?err=" + quote("Usuário não encontrado."))

    full_name = (full_name or "").strip()
    username = (username or "").strip().lower()
    role = (role or "").strip()
    new_password = (new_password or "").strip()

    valid_roles = {value for value, _ in USER_ROLE_OPTIONS}

    if not full_name:
        return redirect("/usuarios?err=" + quote("Informe o nome completo."))
    if not username:
        return redirect("/usuarios?err=" + quote("Informe o username."))
    if role not in valid_roles:
        return redirect("/usuarios?err=" + quote("Perfil inválido."))

    other = session.exec(
        select(User).where(User.username == username, User.id != user_id)
    ).first()
    if other:
        return redirect("/usuarios?err=" + quote("Já existe outro usuário com esse username."))

    if row.username == "johnny.ge" and is_active is None:
        forced_active = True
    else:
        forced_active = bool(is_active)

    before = {
        "full_name": row.full_name,
        "username": row.username,
        "role": row.role,
        "is_active": row.is_active,
    }

    row.full_name = full_name
    row.username = username
    row.role = role
    row.is_active = forced_active

    if row.username == "johnny.ge":
        row.is_active = True

    if new_password:
        row.password_hash = hash_password(new_password)

    session.add(row)
    session.commit()
    session.refresh(row)

    after = {
        "full_name": row.full_name,
        "username": row.username,
        "role": row.role,
        "is_active": row.is_active,
        "password_changed": bool(new_password),
    }

    audit_event(
        request,
        actor,
        "user_updated",
        target_type="user",
        target_id=row.id,
        message=f"Usuário atualizado: {row.username}",
        extra={
            "before": before,
            "after": after,
        },
    )

    return redirect("/usuarios?ok=" + quote("Usuário atualizado com sucesso."))


@app.post("/usuarios/toggle/{user_id}")
def usuarios_toggle(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    actor = get_current_user(request, session)
    if not actor:
        return redirect("/login")
    require(actor.username == "johnny.ge", "Acesso restrito.")

    row = session.get(User, user_id)
    if not row:
        return redirect("/usuarios?err=" + quote("Usuário não encontrado."))

    if row.username == "johnny.ge":
        return redirect("/usuarios?err=" + quote("O usuário master não pode ser desativado."))

    before = row.is_active
    row.is_active = not bool(row.is_active)

    session.add(row)
    session.commit()
    session.refresh(row)

    audit_event(
        request,
        actor,
        "user_toggled",
        target_type="user",
        target_id=row.id,
        message=f"Usuário {'ativado' if row.is_active else 'desativado'}: {row.username}",
        extra={
            "before_is_active": before,
            "after_is_active": row.is_active,
            "username": row.username,
        },
    )

    return redirect("/usuarios?ok=" + quote("Status do usuário atualizado."))

def availability_context(session: Session, day: date, role: str):
    rooms = session.exec(select(Room).order_by(Room.id)).all()
    slots = build_slots_for_day(day)

    day_start = datetime.combine(day, time(0, 0))   # NAIVE p/ casar com o SQLite
    day_end = day_start + timedelta(days=1)

    reservations = session.exec(
        select(Reservation).where(
            Reservation.start_time >= day_start, Reservation.start_time < day_end
        )
    ).all()

    pending_reqs = session.exec(
        select(ReservationRequest).where(
            ReservationRequest.status == "pending",
            ReservationRequest.requested_start >= day_start,
            ReservationRequest.requested_start < day_end,
        )
    ).all()

    occupancy: Dict[int, Dict[str, Dict[str, Any]]] = {}
    
    # Mapa de usuários por id (para mostrar o nome do médico nas reservas)
    user_by_id = {u.id: u for u in session.exec(select(User)).all()}

    for r in reservations:
        for k in slot_keys(r.start_time):
            occupancy.setdefault(r.room_id, {})[k] = {
                "type": "reservation",
                "doctor_name": user_by_id.get(r.doctor_id).full_name if user_by_id.get(r.doctor_id) else "Médico",
            }

    for rq in pending_reqs:
        for k in slot_keys(rq.requested_start):
            occupancy.setdefault(rq.room_id, {})[k] = {
                "type": "request",
                "doctor_name": user_by_id.get(rq.doctor_id).full_name if user_by_id.get(rq.doctor_id) else "Médico",
            }

    doctors = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    weekday_map = [
        "segunda-feira",
        "terça-feira",
        "quarta-feira",
        "quinta-feira",
        "sexta-feira",
        "sábado",
        "domingo",
    ]
    date_human = f"{day.strftime('%d/%m/%Y')} · {weekday_map[day.weekday()]}"

    return {
        "rooms": rooms,
        "slots": slots,
        "occupancy": occupancy,
        "doctors": doctors,
        "role": role,
        "date_human": date_human,
    }

@app.get("/bloqueios", response_class=HTMLResponse)
def bloqueios_page(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito.")

    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    blocks = session.exec(
        select(AgendaBlock).order_by(AgendaBlock.start_date.asc())
    ).all()
    
        # ===== MAPA DE CIRURGIÕES POR BLOQUEIO =====
    block_ids = [b.id for b in blocks if b.id is not None]

    rels = []
    if block_ids:
        rels = session.exec(
            select(AgendaBlockSurgeon).where(
                AgendaBlockSurgeon.block_id.in_(block_ids)
            )
        ).all()

    # block_id -> lista de nomes dos cirurgiões
    block_surgeons_map: dict[int, list[str]] = {}

    if rels:
        surgeons_by_id = {s.id: s.full_name for s in surgeons}

        for r in rels:
            name = surgeons_by_id.get(r.surgeon_id)
            if name:
                block_surgeons_map.setdefault(r.block_id, []).append(name)


    # ===== SUPORTE A EDIÇÃO DE BLOQUEIO =====
    edit_block = None
    selected_surgeons = []

    edit_id = request.query_params.get("edit")
    if edit_id and edit_id.isdigit():
        edit_block = session.get(AgendaBlock, int(edit_id))

        if edit_block and edit_block.id:
            rels = session.exec(
                select(AgendaBlockSurgeon).where(
                    AgendaBlockSurgeon.block_id == edit_block.id
                )
            ).all()
            selected_surgeons = [r.surgeon_id for r in rels]

    return templates.TemplateResponse(
        "bloqueios.html",
        {
            "request": request,
            "current_user": user,
            "surgeons": surgeons,
            "blocks": blocks,
            "edit_block": edit_block,
            "selected_surgeons": selected_surgeons,
            "block_surgeons_map": block_surgeons_map,
        },
    )
    

@app.post("/bloqueios")
def registrar_bloqueio(
    request: Request,
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    motivo: str = Form(...),
    surgeons: list[str] = Form([]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito.")

    # converte "YYYY-MM-DD" para date
    start_date = date.fromisoformat(data_inicio)
    end_date = date.fromisoformat(data_fim)
    
    if end_date < start_date:
        return redirect("/bloqueios")
    
    applies_all = (len(surgeons) == 0)

    row = AgendaBlock(
        day=start_date,
        start_date=start_date,
        end_date=end_date,
        reason=motivo.strip(),
        applies_to_all=applies_all,
        created_by_id=user.id,
    )
    session.add(row)
    session.commit()

    if not applies_all:
        for sid in surgeons:
            session.add(AgendaBlockSurgeon(block_id=row.id, surgeon_id=int(sid)))
        session.commit()

    return redirect("/bloqueios")

@app.post("/bloqueios/{block_id}/update")
def bloqueio_update(
    request: Request,
    block_id: int,
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    motivo: str = Form(...),
    surgeons: list[str] = Form([]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito.")

    b = session.get(AgendaBlock, block_id)
    if not b:
        return redirect("/bloqueios")

    b.start_date = date.fromisoformat(data_inicio)
    b.day = b.start_date
    b.end_date = date.fromisoformat(data_fim)
    if b.end_date < b.start_date:
        return redirect("/bloqueios")
    b.reason = motivo.strip()
    b.applies_to_all = (len(surgeons) == 0)

    session.add(b)
    session.commit()

    # limpa relações antigas
    session.exec(
        delete(AgendaBlockSurgeon).where(AgendaBlockSurgeon.block_id == block_id)
    )
    session.commit()

    # recria relações
    if not b.applies_to_all:
        for sid in surgeons:
            session.add(AgendaBlockSurgeon(block_id=block_id, surgeon_id=int(sid)))
        session.commit()

    return redirect("/bloqueios")

@app.post("/bloqueios/{block_id}/delete")
def bloqueio_delete(
    request: Request,
    block_id: int,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito.")

    # apaga relações
    session.exec(
        delete(AgendaBlockSurgeon).where(AgendaBlockSurgeon.block_id == block_id)
    )
    session.commit()

    # apaga bloco
    b = session.get(AgendaBlock, block_id)
    if b:
        session.delete(b)
        session.commit()

    return redirect("/bloqueios")

@app.get("/doctor", response_class=HTMLResponse)
def doctor_page(
    request: Request,
    date: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "doctor", "Acesso restrito aos médicos.")

    selected, day = safe_selected_and_day(date)
    ctx = availability_context(session, day, role="doctor")
    audit_event(request, user, "doctor_page_view", extra={"date": selected})

    return templates.TemplateResponse(
        "doctor.html",
        {
            "request": request,
            "current_user": user,
            "title": "Agenda",
            "selected_date": selected,
            **ctx,
        },
    )

@app.get("/doctor/availability", response_class=HTMLResponse)
def doctor_availability(
    request: Request,
    date: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "doctor", "Acesso restrito aos médicos.")

    _, day = safe_selected_and_day(date)
    ctx = availability_context(session, day, role="doctor")

    return templates.TemplateResponse(
        "partials/availability.html",
        {"request": request, "current_user": user, **ctx},
    )


@app.post("/doctor/request")
def doctor_request(
    request: Request,
    room_id: int = Form(...),
    start_iso: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "doctor", "Acesso restrito aos médicos.")

    start_dt = to_db_dt(datetime.fromisoformat(start_iso))
    end_dt = start_dt + timedelta(minutes=SLOT_MINUTES)

    existing_res = session.exec(
        select(Reservation).where(
            Reservation.room_id == room_id, Reservation.start_time == start_dt
        )
    ).first()
    existing_req = session.exec(
        select(ReservationRequest).where(
            ReservationRequest.room_id == room_id,
            ReservationRequest.requested_start == start_dt,
            ReservationRequest.status == "pending",
        )
    ).first()
    if existing_res or existing_req:
        audit_event(
            request,
            user,
            "request_conflict",
            success=False,
            message="Slot já ocupado (reserva ou solicitação pendente).",
            room_id=room_id,
            start_time=start_dt,
            end_time=end_dt,
        )
        return redirect(f"/doctor?date={start_dt.date().isoformat()}")


    rq = ReservationRequest(
        room_id=room_id,
        doctor_id=user.id,
        requested_start=start_dt,
        requested_end=end_dt,
        status="pending",
    )
    session.add(rq)
    session.commit()

    audit_event(
        request,
        user,
        "request_created",
        room_id=room_id,
        target_type="request",
        target_id=rq.id,
        start_time=start_dt,
        end_time=end_dt,
    )

    return redirect("/doctor")


@app.get("/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    date: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "admin", "Acesso restrito à secretaria/admin.")

    selected, day = safe_selected_and_day(date)
    ctx = availability_context(session, day, role="admin")

    pending = session.exec(
        select(ReservationRequest)
        .where(ReservationRequest.status == "pending")
        .order_by(ReservationRequest.created_at.desc())
    ).all()

    rooms = {r.id: r for r in session.exec(select(Room)).all()}
    users = {u.id: u for u in session.exec(select(User)).all()}

    pending_view = []
    audit_event(request, user, "admin_page_view", extra={"date": selected})
    for r in pending:
        dt = r.requested_start.replace(tzinfo=TZ)
        pending_view.append(
            {
                "id": r.id,
                "doctor_name": users.get(r.doctor_id).full_name
                if users.get(r.doctor_id)
                else "Médico",
                "room_name": rooms.get(r.room_id).name if rooms.get(r.room_id) else "Sala",
                "date_str": dt.strftime("%d/%m/%Y"),
                "time_str": dt.strftime("%H:%M"),
            }
        )

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "current_user": user,
            "title": "Agenda",
            "selected_date": selected,
            "pending_requests": pending_view,
            **ctx,
        },
    )


@app.get("/admin/availability", response_class=HTMLResponse)
def admin_availability(
    request: Request,
    date: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "admin", "Acesso restrito à secretaria/admin.")

    _, day = safe_selected_and_day(date)
    ctx = availability_context(session, day, role="admin")

    return templates.TemplateResponse(
        "partials/availability.html",
        {"request": request, "current_user": user, **ctx},
    )


@app.post("/admin/reserve")
def admin_reserve(
    request: Request,
    room_id: int = Form(...),
    doctor_id: int = Form(...),
    start_iso: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "admin", "Acesso restrito à secretaria/admin.")

    start_dt = to_db_dt(datetime.fromisoformat(start_iso))
    end_dt = start_dt + timedelta(minutes=SLOT_MINUTES)

    existing = session.exec(
        select(Reservation).where(
            Reservation.room_id == room_id, Reservation.start_time == start_dt
        )
    ).first()
    if existing:
        audit_event(
            request,
            user,
            "admin_reserve_conflict",
            success=False,
            message="Já existe reserva nesse horário.",
            room_id=room_id,
            start_time=start_dt,
            end_time=end_dt,
            extra={"doctor_id": doctor_id},
        )
        return redirect(f"/admin?date={start_dt.date().isoformat()}")


    res = Reservation(
        room_id=room_id,
        doctor_id=doctor_id,
        created_by_id=user.id,
        start_time=start_dt,
        end_time=end_dt,
    )
    session.add(res)
    session.commit()

    audit_event(
        request,
        user,
        "admin_reserve_created",
        room_id=room_id,
        target_type="reservation",
        target_id=res.id,
        start_time=start_dt,
        end_time=end_dt,
        extra={"doctor_id": doctor_id},
    )

    return redirect("/admin")


@app.post("/admin/requests/{request_id}/approve")
def approve_request(request: Request, request_id: int, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "admin")

    rq = session.get(ReservationRequest, request_id)
    if not rq or rq.status != "pending":
        return redirect("/admin")

    existing = session.exec(
        select(Reservation).where(
            Reservation.room_id == rq.room_id,
            Reservation.start_time == rq.requested_start,
        )
    ).first()

    if existing:
        rq.status = "denied"
        rq.decided_by_id = user.id
        rq.decided_at = datetime.utcnow()
        session.add(rq)
        session.commit()
        audit_event(
            request,
            user,
            "request_approve_conflict_denied",
            success=False,
            message="Havia reserva no slot; solicitação negada automaticamente.",
            room_id=rq.room_id,
            target_type="request",
            target_id=rq.id,
            start_time=rq.requested_start,
            end_time=rq.requested_end,
        )
        return redirect("/admin")

    res = Reservation(
        room_id=rq.room_id,
        doctor_id=rq.doctor_id,
        created_by_id=user.id,
        start_time=rq.requested_start,
        end_time=rq.requested_end,
    )
    session.add(res)

    rq.status = "approved"
    rq.decided_by_id = user.id
    rq.decided_at = datetime.utcnow()
    session.add(rq)

    session.commit()
    audit_event(
        request,
        user,
        "request_approved",
        room_id=rq.room_id,
        target_type="request",
        target_id=rq.id,
        start_time=rq.requested_start,
        end_time=rq.requested_end,
        extra={"reservation_id": res.id},
    )

    return redirect("/admin")


@app.post("/admin/requests/{request_id}/deny")
def deny_request(request: Request, request_id: int, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role == "admin")

    rq = session.get(ReservationRequest, request_id)
    if rq and rq.status == "pending":
        rq.status = "denied"
        rq.decided_by_id = user.id
        rq.decided_at = datetime.utcnow()
        session.add(rq)
        session.commit()
        audit_event(
            request,
            user,
            "request_denied",
            room_id=rq.room_id,
            target_type="request",
            target_id=rq.id,
            start_time=rq.requested_start,
            end_time=rq.requested_end,
        )

    return redirect("/admin")

@app.get("/mapa", response_class=HTMLResponse)
def mapa_page(
    request: Request,
    month: Optional[str] = None,
    err: str | None = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito ao Mapa Cirúrgico.")

    selected_month, first_day, next_first, days = safe_selected_month(month)

    audit_event(
        request,
        user,
        "mapa_page_view",
        extra={"month": selected_month},
    )
    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()
    
    sellers = session.exec(
        select(User).where(User.role == "surgery", User.is_active == True).order_by(User.full_name)
    ).all()

    procedure_catalog = session.exec(
        select(ProcedureCatalog)
        .where(ProcedureCatalog.is_active == True)
        .order_by(ProcedureCatalog.nucleus, ProcedureCatalog.name)
    ).all()
    
    users_all = session.exec(select(User)).all()
    users_by_id = {u.id: u for u in users_all if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= first_day,
            SurgicalMapEntry.day < next_first,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()
    
    entry_ids = [e.id for e in entries if e.id is not None]

    procedure_items = []
    if entry_ids:
        procedure_items = session.exec(
            select(SurgeryProcedureItem)
            .where(SurgeryProcedureItem.surgery_entry_id.in_(entry_ids))
            .order_by(SurgeryProcedureItem.surgery_entry_id, SurgeryProcedureItem.id)
        ).all()

    procedures_by_entry: dict[int, list[dict]] = defaultdict(list)

    for item in procedure_items:
        procedures_by_entry[item.surgery_entry_id].append({
            "procedure_id": item.procedure_id,
            "procedure_name": item.procedure_name_snapshot,
            "amount": item.amount,
            "nucleus": item.nucleus_snapshot,
        })

    entries_by_day: dict[str, list[SurgicalMapEntry]] = {}
    for e in entries:
        entries_by_day.setdefault(e.day.isoformat(), []).append(e)

    # pega bloqueios que intersectam o mês
    blocks = session.exec(
        select(AgendaBlock)
        .where(
            AgendaBlock.start_date <= (next_first - timedelta(days=1)),
            AgendaBlock.end_date >= first_day,
        )
        .order_by(AgendaBlock.start_date, AgendaBlock.created_at)
    ).all()

    # relações (multi-cirurgiões)
    block_ids = [b.id for b in blocks if b.id is not None]
    rels = []
    if block_ids:
        rels = session.exec(
            select(AgendaBlockSurgeon).where(AgendaBlockSurgeon.block_id.in_(block_ids))
        ).all()

    surgeons_by_block: dict[int, list[int]] = {}
    for r in rels:
        surgeons_by_block.setdefault(r.block_id, []).append(r.surgeon_id)
    
    # ✅ block_id -> lista de nomes dos cirurgiões (para exibir no mapa.html)
    surgeons_by_id = {s.id: s.full_name for s in surgeons if s.id is not None}
    block_surgeons_map: dict[int, list[str]] = {}

    for b in blocks:
        if not b.id:
            continue
        if b.applies_to_all:
            block_surgeons_map[b.id] = ["Todos"]
        else:
            ids = surgeons_by_block.get(b.id, [])
            names = [surgeons_by_id.get(sid) for sid in ids]
            block_surgeons_map[b.id] = [n for n in names if n] or ["—"]

    blocks_by_day: dict[str, list[AgendaBlock]] = {}
    blocked_all_days: set[str] = set()
    blocked_surgeons_by_day: dict[str, list[int]] = {}

    # expande cada bloqueio para os dias do mês (no máximo 31 dias)
    month_end = next_first - timedelta(days=1)

    for b in blocks:
        start = max(b.start_date, first_day)
        end = min(b.end_date, month_end)

        d = start
        while d <= end:
            k = d.isoformat()
            blocks_by_day.setdefault(k, []).append(b)

            if b.applies_to_all:
                blocked_all_days.add(k)
            else:
                ids = surgeons_by_block.get(b.id or -1, [])
                if ids:
                    blocked_surgeons_by_day.setdefault(k, []).extend(ids)

            d += timedelta(days=1)

    priority = compute_priority_card(session)
    
    mapa_hsr_summary = get_mapa_hsr_month_summary(session, selected_month)

    weekday_map = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]
    feegow_alert_count = get_latest_feegow_alert_count(session)
    latest_feegow_status_by_entry = get_latest_feegow_status_by_entry(session)
        
    return templates.TemplateResponse(
        "mapa.html",
        {
            "request": request,
            "current_user": user,
            "fmt_brasilia": fmt_brasilia,
            "err": err,
            "title": "Mapa Cirúrgico",
            "feegow_alert_count": feegow_alert_count,
            "latest_feegow_status_by_entry": latest_feegow_status_by_entry,
            "selected_month": selected_month,   # YYYY-MM
            "days": days,
            "entries_by_day": entries_by_day,   # dict[str, list]
            "surgeons": surgeons,
            "weekday_map": weekday_map,
            "users_by_id": users_by_id,
            "blocks": blocks,
            "blocks_by_day": blocks_by_day,
            "block_surgeons_map": block_surgeons_map,  # ✅ NOVO
            "blocked_all_days": blocked_all_days,
            "blocked_surgeons_by_day": blocked_surgeons_by_day,
            "priority_mode": priority["mode"],
            "priority_items": priority["items"],
            "sellers": sellers,
            "procedure_catalog": procedure_catalog,
            "procedures_by_entry": procedures_by_entry,
            "blocked_all_days": blocked_all_days,  # set[str] -> "2026-01-15"
            "blocked_surgeons_by_day": blocked_surgeons_by_day,  # dict[str, list[int]]
            "mapa_hsr_summary": mapa_hsr_summary,
        },
    )

def save_surgery_procedure_items(
    session: Session,
    *,
    surgery_entry_id: int,
    procedure_ids: list[int] | None,
    form_data: dict,
):
    """
    Salva os procedimentos selecionados no modal do mapa cirúrgico.
    - procedure_ids: lista dos IDs marcados
    - form_data: dados do form (para ler procedure_amount_{id})
    """

    session.exec(
        delete(SurgeryProcedureItem).where(
            SurgeryProcedureItem.surgery_entry_id == surgery_entry_id
        )
    )

    if not procedure_ids:
        session.commit()
        return

    entry = session.get(SurgicalMapEntry, surgery_entry_id)
    surgeon = None
    if entry and entry.surgeon_id:
        surgeon = session.get(User, entry.surgeon_id)

    catalog_rows = session.exec(
        select(ProcedureCatalog).where(ProcedureCatalog.id.in_(procedure_ids))
    ).all()

    catalog_by_id = {p.id: p for p in catalog_rows if p.id is not None}
    resolved_nuclei = resolve_procedure_nuclei_for_entry(catalog_rows, surgeon)

    items_to_add = []

    for pid in procedure_ids:
        proc = catalog_by_id.get(pid)
        if not proc:
            continue

        raw_amount = form_data.get(f"procedure_amount_{pid}", "") or ""
        raw_amount = str(raw_amount).strip().replace(",", ".")

        try:
            amount = float(raw_amount) if raw_amount != "" else 0.0
        except ValueError:
            amount = 0.0

        resolved_nucleus = resolved_nuclei.get(pid, proc.nucleus)

        items_to_add.append(
            SurgeryProcedureItem(
                surgery_entry_id=surgery_entry_id,
                procedure_id=pid,
                procedure_name_snapshot=proc.name,
                nucleus_snapshot=resolved_nucleus,
                amount=amount,
            )
        )

    if items_to_add:
        session.add_all(items_to_add)

    session.commit()
    
def save_surgery_procedure_items(
    session: Session,
    *,
    surgery_entry_id: int,
    procedure_ids: list[int] | None,
    form_data: dict,
):
    """
    Salva os procedimentos selecionados no modal do mapa cirúrgico.
    - procedure_ids: lista dos IDs marcados
    - form_data: dados do form (para ler procedure_amount_{id})
    """

    session.exec(
        delete(SurgeryProcedureItem).where(
            SurgeryProcedureItem.surgery_entry_id == surgery_entry_id
        )
    )

    if not procedure_ids:
        session.commit()
        return

    entry = session.get(SurgicalMapEntry, surgery_entry_id)
    surgeon = None
    if entry and entry.surgeon_id:
        surgeon = session.get(User, entry.surgeon_id)

    catalog_rows = session.exec(
        select(ProcedureCatalog).where(ProcedureCatalog.id.in_(procedure_ids))
    ).all()

    catalog_by_id = {p.id: p for p in catalog_rows if p.id is not None}
    resolved_nuclei = resolve_procedure_nuclei_for_entry(catalog_rows, surgeon)

    items_to_add = []

    for pid in procedure_ids:
        proc = catalog_by_id.get(pid)
        if not proc:
            continue

        raw_amount = form_data.get(f"procedure_amount_{pid}", "") or ""
        raw_amount = str(raw_amount).strip().replace(",", ".")

        try:
            amount = float(raw_amount) if raw_amount != "" else 0.0
        except ValueError:
            amount = 0.0

        resolved_nucleus = resolved_nuclei.get(pid, proc.nucleus)

        items_to_add.append(
            SurgeryProcedureItem(
                surgery_entry_id=surgery_entry_id,
                procedure_id=pid,
                procedure_name_snapshot=proc.name,
                nucleus_snapshot=resolved_nucleus,
                amount=amount,
            )
        )

    if items_to_add:
        session.add_all(items_to_add)

    session.commit()

def refresh_surgery_procedure_items(session: Session, *, surgery_entry_id: int) -> None:
    """
    Reprocessa os procedimentos já gravados de uma cirurgia,
    preservando os mesmos procedimentos e valores, mas recalculando
    o nucleus_snapshot com base no catálogo atual e no cirurgião.
    """
    entry = session.get(SurgicalMapEntry, surgery_entry_id)
    if not entry:
        return

    current_items = session.exec(
        select(SurgeryProcedureItem)
        .where(SurgeryProcedureItem.surgery_entry_id == surgery_entry_id)
        .order_by(SurgeryProcedureItem.id)
    ).all()

    if not current_items:
        return

    procedure_ids = [item.procedure_id for item in current_items if item.procedure_id]
    if not procedure_ids:
        return

    amount_by_pid = {}
    for item in current_items:
        if item.procedure_id is not None:
            amount_by_pid[item.procedure_id] = float(item.amount or 0)

    surgeon = None
    if entry.surgeon_id:
        surgeon = session.get(User, entry.surgeon_id)

    catalog_rows = session.exec(
        select(ProcedureCatalog).where(ProcedureCatalog.id.in_(procedure_ids))
    ).all()

    catalog_by_id = {p.id: p for p in catalog_rows if p.id is not None}
    resolved_nuclei = resolve_procedure_nuclei_for_entry(catalog_rows, surgeon)

    session.exec(
        delete(SurgeryProcedureItem).where(
            SurgeryProcedureItem.surgery_entry_id == surgery_entry_id
        )
    )

    items_to_add = []

    for pid in procedure_ids:
        proc = catalog_by_id.get(pid)
        if not proc:
            continue

        items_to_add.append(
            SurgeryProcedureItem(
                surgery_entry_id=surgery_entry_id,
                procedure_id=pid,
                procedure_name_snapshot=proc.name,
                nucleus_snapshot=resolved_nuclei.get(pid, proc.nucleus),
                amount=amount_by_pid.get(pid, 0.0),
            )
        )

    if items_to_add:
        session.add_all(items_to_add)

    session.commit()

@app.post("/mapa/create")
async def mapa_create(
    request: Request,
    day_iso: str = Form(...),
    mode: str = Form("book"),
    time_hhmm: Optional[str] = Form(None),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    has_lodging: Optional[str] = Form(None),
    seller_id: Optional[int] = Form(None),
    force_override: Optional[str] = Form(None),
    procedure_id: Optional[list[int]] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))
    
    is_johnny = (user.username == "johnny.ge")
    override = is_johnny and bool(force_override)

    # ✅ regra do vendedor (depois do user existir!)
    if user.username != "johnny.ge":
        seller_id_final = user.id
    else:
        seller_id_final = int(seller_id) if seller_id else user.id

    day = date.fromisoformat(day_iso)
    
    is_pre = (mode == "reserve")

    block_err = validate_mapa_block_rules(session, day, surgeon_id)
    if block_err and not override:
        month = day.strftime("%Y-%m")
        audit_event(request, user, "surgical_map_blocked_by_agenda_block", success=False, message=block_err)

        form = await request.form()

        proc_qs = ""
        for pid in (procedure_id or []):
            proc_qs += f"&procedure_id={pid}"
            proc_qs += f"&procedure_amount_{pid}={quote(str(form.get(f'procedure_amount_{pid}', '') or ''))}"

        return redirect(
            f"/mapa?month={month}&open=1"
            f"&err={quote(block_err)}"
            f"&day_iso={quote(day_iso)}"
            f"&mode={quote(mode)}"
            f"&time_hhmm={quote(time_hhmm or '')}"
            f"&patient_name={quote(patient_name)}"
            f"&surgeon_id={surgeon_id}"
            f"&procedure_type={quote(procedure_type)}"
            f"&location={quote(location)}"
            f"&uses_hsr={1 if uses_hsr else 0}"
            f"&has_lodging={1 if has_lodging else 0}"
            f"&seller_id={seller_id_final}"
            f"{proc_qs}"
        )

    # se passou com override, registra auditoria
    if block_err and override:
        audit_event(request, user, "surgical_map_override_agenda_block", success=True, message=block_err)

    form = await request.form()

    err = validate_mapa_rules(session, day, surgeon_id, procedure_type, uses_hsr=bool(uses_hsr))
    hsr_err, requested_slot_type, hsr_used, hsr_total = validate_hsr_slot_availability(
        session,
        day=day,
        uses_hsr=bool(uses_hsr),
        procedure_ids=procedure_id,
        exclude_entry_id=None,
    )

    if (err or hsr_err) and not override:
        month = day.strftime("%Y-%m")
        audit_event(
            request,
            user,
            "surgical_map_create_validation_error",
            success=False,
            message=(hsr_err or err),
            extra={
                "day": day_iso,
                "time_hhmm": time_hhmm,
                "patient_name": patient_name,
                "surgeon_id": surgeon_id,
                "procedure_type": procedure_type,
                "location": location,
                "uses_hsr": bool(uses_hsr),
                "requested_slot_type": requested_slot_type,
                "hsr_used": hsr_used,
                "hsr_total": hsr_total,
                "mode": mode,
            },
        )

        proc_qs = build_hsr_proc_qs(procedure_id, dict(form))

        return redirect(
            f"/mapa?month={month}&open=1"
            f"&err={quote(hsr_err or err)}"
            f"&day_iso={quote(day_iso)}"
            f"&mode={quote(mode)}"
            f"&time_hhmm={quote(time_hhmm or '')}"
            f"&patient_name={quote(patient_name)}"
            f"&surgeon_id={surgeon_id}"
            f"&procedure_type={quote(procedure_type)}"
            f"&location={quote(location)}"
            f"&uses_hsr={1 if uses_hsr else 0}"
            f"&has_lodging={1 if has_lodging else 0}"
            f"&seller_id={seller_id_final}"
            f"{proc_qs}"
        )
    
    time_hhmm = (time_hhmm or "").strip()  # normaliza
    
    row = SurgicalMapEntry(
        day=day,
        time_hhmm=(time_hhmm or None),
        patient_name=patient_name.strip().upper(),
        surgeon_id=surgeon_id,
        procedure_type=procedure_type,
        location=location,
        uses_hsr=bool(uses_hsr),
        is_pre_reservation=is_pre,
        created_by_id=seller_id_final,
    )
    
    session.add(row)
    session.commit()
    session.refresh(row)

    form = await request.form()

    save_surgery_procedure_items(
        session,
        surgery_entry_id=row.id,
        procedure_ids=procedure_id,
        form_data=dict(form),
    )

    snapshot_after = build_surgical_map_snapshot(session, row.id)

    audit_event(
        request,
        user,
        "surgical_map_created",
        target_type="surgical_map",
        target_id=row.id,
        message=f"Card criado para {row.patient_name}",
        extra={
            "snapshot_after": snapshot_after,
        },
    )

    month = day.strftime("%Y-%m")
    if has_lodging:
        # check-in e check-out default: 1 dia (você pode mudar depois)
        ci = day.isoformat()
        co = (day + timedelta(days=1)).isoformat()
        return redirect(
            f"/hospedagem?month={quote(month)}&open=1"
            f"&unit={quote('')}"
            f"&check_in={quote(ci)}&check_out={quote(co)}"
            f"&patient_name={quote(patient_name.strip().upper())}"
            f"&is_pre_reservation={(1 if is_pre else 0)}"
            f"&surgery_entry_id={row.id}"
        )

    return redirect(f"/mapa?month={month}")

@app.post("/mapa/request")
async def mapa_request_authorization(
    request: Request,
    day_iso: str = Form(...),
    mode: str = Form("book"),
    time_hhmm: Optional[str] = Form(None),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    has_lodging: Optional[str] = Form(None),
    seller_id: Optional[int] = Form(None),
    procedure_id: Optional[list[int]] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    # ✅ Funcionário não pode “pedir autorização” sendo o Johnny
    if user.username == "johnny.ge":
        return redirect("/mapa")

    # ✅ regra do vendedor: sempre grava como o próprio usuário (funcionário)
    seller_id_final = user.id

    day = date.fromisoformat(day_iso)
    is_pre = (mode == "reserve")
    time_hhmm = (time_hhmm or "").strip()

    form = await request.form()

    block_err = validate_mapa_block_rules(session, day, surgeon_id)
    rule_err = validate_mapa_rules(session, day, surgeon_id, procedure_type, uses_hsr=bool(uses_hsr))
    hsr_err, requested_slot_type, hsr_used, hsr_total = validate_hsr_slot_availability(
        session,
        day=day,
        uses_hsr=bool(uses_hsr),
        procedure_ids=procedure_id,
        exclude_entry_id=None,
    )

    effective_err = block_err or hsr_err or rule_err

    if not effective_err:
        return redirect(f"/mapa?month={day.strftime('%Y-%m')}")

    row = SurgicalMapEntry(
        day=day,
        time_hhmm=(time_hhmm or None),
        patient_name=patient_name.strip().upper(),
        surgeon_id=surgeon_id,
        procedure_type=procedure_type,
        location=location,
        uses_hsr=bool(uses_hsr),
        is_pre_reservation=is_pre,
        status="pending",                 # ✅ fica roxo / pendente
        created_by_id=seller_id_final,    # ✅ fica com o usuário do funcionário
    )

    session.add(row)
    session.commit()
    session.refresh(row)

    form = await request.form()

    save_surgery_procedure_items(
        session,
        surgery_entry_id=row.id,
        procedure_ids=procedure_id,
        form_data=dict(form),
    )

    snapshot_after = build_surgical_map_snapshot(session, row.id)

    audit_event(
        request,
        user,
        "surgical_map_auth_requested",
        target_type="surgical_map",
        target_id=row.id,
        success=True,
        message=(block_err or rule_err),
        extra={
            "snapshot_after": snapshot_after,
        },
    )

    # Hospedagem: eu recomendo NÃO abrir hospedagem automaticamente quando está pending
    # porque ainda não é um agendamento válido.
    return redirect(f"/mapa?month={day.strftime('%Y-%m')}")

@app.post("/mapa/approve/{entry_id}")
def mapa_approve(request: Request, entry_id: int, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))
    require(user.username == "johnny.ge")  # ✅ SOMENTE VOCÊ

    row = session.get(SurgicalMapEntry, entry_id)
    if not row:
        return redirect("/mapa")

    # Só aprova se estiver pendente
    if row.status != "pending":
        return redirect(f"/mapa?month={row.day.strftime('%Y-%m')}")

    before_snapshot = build_surgical_map_snapshot(session, row.id)

    row.status = "approved"
    row.decide_by_id = user.id
    row.decided_at = datetime.utcnow()

    session.add(row)
    session.commit()
    session.refresh(row)

    after_snapshot = build_surgical_map_snapshot(session, row.id)

    audit_event(
        request,
        user,
        "surgical_map_auth_approved",
        target_type="surgical_map",
        target_id=row.id,
        message="Solicitação aprovada pelo Johnny",
        extra={
            "snapshot_before": before_snapshot,
            "snapshot_after": after_snapshot,
            "changes": build_surgical_map_changes(before_snapshot, after_snapshot),
        },
    )
    return redirect(f"/mapa?month={row.day.strftime('%Y-%m')}")


@app.post("/mapa/deny/{entry_id}")
def mapa_deny(request: Request, entry_id: int, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))
    require(user.username == "johnny.ge")  # ✅ SOMENTE VOCÊ

    row = session.get(SurgicalMapEntry, entry_id)
    if not row:
        return redirect("/mapa")

    month = row.day.strftime("%Y-%m")

    before_snapshot = build_surgical_map_snapshot(session, row.id)

    row.status = "denied"
    session.add(row)
    session.commit()
    session.refresh(row)

    after_snapshot = build_surgical_map_snapshot(session, row.id)

    audit_event(
        request,
        user,
        "surgical_map_auth_denied",
        target_type="surgical_map",
        target_id=row.id,
        message="Solicitação reprovada pelo Johnny",
        extra={
            "snapshot_before": before_snapshot,
            "snapshot_after": after_snapshot,
            "changes": build_surgical_map_changes(before_snapshot, after_snapshot),
        },
    )

    return redirect(f"/mapa?month={month}")

@app.post("/mapa/update/{entry_id}")
async def mapa_update(
    request: Request,
    entry_id: int,
    day_iso: str = Form(...),
    mode: str = Form("book"),
    time_hhmm: Optional[str] = Form(None),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    has_lodging: Optional[str] = Form(None),  
    seller_id: Optional[int] = Form(None),
    force_override: Optional[str] = Form(None),
    procedure_id: Optional[list[int]] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))
    
    is_johnny = (user.username == "johnny.ge")
    override = is_johnny and bool(force_override)
    
    # ✅ regra do vendedor (mesma do /mapa/create)
    if user.username != "johnny.ge":
        seller_id_final = user.id
    else:
        seller_id_final = int(seller_id) if seller_id else user.id

    row = session.get(SurgicalMapEntry, entry_id)
    if not row:
        return redirect("/mapa")

    day = date.fromisoformat(day_iso)
    is_pre = (mode == "reserve")

    # valida regras EXCLUINDO o próprio item (pra não bloquear edição à toa)
    form = await request.form()

    err = validate_mapa_rules(
        session,
        day,
        surgeon_id,
        procedure_type,
        uses_hsr=bool(uses_hsr),
        exclude_entry_id=entry_id,
    )

    hsr_err, requested_slot_type, hsr_used, hsr_total = validate_hsr_slot_availability(
        session,
        day=day,
        uses_hsr=bool(uses_hsr),
        procedure_ids=procedure_id,
        exclude_entry_id=entry_id,
    )

    if err or hsr_err:
        month = day.strftime("%Y-%m")

        proc_qs = build_hsr_proc_qs(procedure_id, dict(form))

        return redirect(
            f"/mapa?month={month}&open=1&edit_id={entry_id}"
            f"&err={quote(hsr_err or err)}"
            f"&day_iso={quote(day_iso)}"
            f"&mode={quote(mode)}"
            f"&time_hhmm={quote(time_hhmm or '')}"
            f"&patient_name={quote(patient_name)}"
            f"&surgeon_id={surgeon_id}"
            f"&procedure_type={quote(procedure_type)}"
            f"&location={quote(location)}"
            f"&uses_hsr={1 if uses_hsr else 0}"
            f"&has_lodging={1 if has_lodging else 0}"
            f"&seller_id={seller_id_final}"
            f"{proc_qs}"
        )

    # snapshot (opcional) pra auditoria
    before_snapshot = build_surgical_map_snapshot(session, row.id)

    time_hhmm = (time_hhmm or "").strip()  # normaliza

    # aplica alterações
    row.day = day
    row.time_hhmm = time_hhmm or None
    row.patient_name = patient_name.strip().upper()
    row.surgeon_id = surgeon_id
    row.procedure_type = procedure_type
    row.location = location
    row.uses_hsr = bool(uses_hsr)
    row.is_pre_reservation = is_pre
    row.created_by_id = seller_id_final 

    session.add(row)
    session.commit()
    session.refresh(row)

    form = await request.form()

    save_surgery_procedure_items(
        session,
        surgery_entry_id=row.id,
        procedure_ids=procedure_id,
        form_data=dict(form),
    )

    after_snapshot = build_surgical_map_snapshot(session, row.id)

    audit_event(
        request,
        user,
        "surgical_map_updated",
        target_type="surgical_map",
        target_id=row.id,
        message=f"Card editado: {row.patient_name}",
        extra={
            "snapshot_before": before_snapshot,
            "snapshot_after": after_snapshot,
            "changes": build_surgical_map_changes(before_snapshot, after_snapshot),
        },
    )

    month = day.strftime("%Y-%m")

    # ✅ Se marcou "Hospedagem", abre a tela de hospedagem depois de salvar
    if has_lodging:

        # regra padrão: check-in 2 dias após a cirurgia; check-out 1 dia depois (ajuste se quiser)
        check_in = (day + timedelta(days=2)).isoformat()
        check_out = (day + timedelta(days=3)).isoformat()

        return redirect(
            f"/hospedagem?month={month}&open=1"
            f"&unit="  # vazio (usuário escolhe suite/apto no modal)
            f"&check_in={quote(check_in)}"
            f"&check_out={quote(check_out)}"
            f"&patient_name={quote(patient_name.strip().upper())}"
            f"&is_pre_reservation={1 if is_pre else 0}"
        )

    return redirect(f"/mapa?month={month}")

@app.post("/mapa/delete/{entry_id}")
def mapa_delete(
    request: Request,
    entry_id: int,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    row = session.get(SurgicalMapEntry, entry_id)
    if row:
        month = row.day.strftime("%Y-%m")
        before_snapshot = build_surgical_map_snapshot(session, row.id)

        row.status = "deleted"
        session.add(row)
        session.commit()
        session.refresh(row)

        after_snapshot = build_surgical_map_snapshot(session, row.id)

        audit_event(
            request,
            user,
            "surgical_map_deleted",
            target_type="surgical_map",
            target_id=entry_id,
            message="Card ocultado do mapa (soft delete)",
            extra={
                "snapshot_before": before_snapshot,
                "snapshot_after": after_snapshot,
                "changes": build_surgical_map_changes(before_snapshot, after_snapshot),
            },
        )
        return redirect(f"/mapa?month={month}")

    audit_event(
        request,
        user,
        "surgical_map_delete_not_found",
        success=False,
        message="Tentou apagar um agendamento que não existe (ou já foi removido).",
        target_type="surgical_map",
        target_id=entry_id,
    )
    return redirect("/mapa")

@app.get("/meus_clientes", response_class=HTMLResponse)
def meus_clientes_page(
    request: Request,
    month: Optional[str] = None,
    year: Optional[str] = None,
    search: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"), "Acesso restrito ao Mapa Cirúrgico.")

    search_term = (search or "").strip().upper()

    selected_month = ""
    selected_year = ""

    query = (
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.created_by_id == user.id,
            visible_surgical_map_status_clause(),
        )
    )

    if month and year:
        try:
            month_int = int(month)
            year_int = int(year)
            first_day = date(year_int, month_int, 1)
            if month_int == 12:
                next_first = date(year_int + 1, 1, 1)
            else:
                next_first = date(year_int, month_int + 1, 1)

            query = query.where(
                SurgicalMapEntry.day >= first_day,
                SurgicalMapEntry.day < next_first,
            )

            selected_month = f"{month_int:02d}"
            selected_year = str(year_int)
        except ValueError:
            pass

    if search_term:
        query = query.where(SurgicalMapEntry.patient_name.contains(search_term))

    audit_event(
        request,
        user,
        "meus_clientes_page_view",
        extra={
            "month": selected_month,
            "year": selected_year,
            "search": search_term,
        },
    )

    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    users_all = session.exec(select(User)).all()
    users_by_id = {u.id: u for u in users_all if u.id is not None}

    entries = session.exec(
        query.order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()

    entry_ids = [e.id for e in entries if e.id is not None]

    procedure_items = []
    if entry_ids:
        procedure_items = session.exec(
            select(SurgeryProcedureItem)
            .where(SurgeryProcedureItem.surgery_entry_id.in_(entry_ids))
            .order_by(SurgeryProcedureItem.surgery_entry_id, SurgeryProcedureItem.id)
        ).all()

    procedures_by_entry: dict[int, list[dict]] = defaultdict(list)

    for item in procedure_items:
        procedures_by_entry[item.surgery_entry_id].append({
            "procedure_id": item.procedure_id,
            "procedure_name": item.procedure_name_snapshot,
            "amount": item.amount,
            "nucleus": item.nucleus_snapshot,
        })

    entries_by_day: dict[str, list[SurgicalMapEntry]] = {}
    ordered_days: list[date] = []

    for e in entries:
        key = e.day.isoformat()
        if key not in entries_by_day:
            entries_by_day[key] = []
            ordered_days.append(e.day)
        entries_by_day[key].append(e)

    weekday_map = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]

    total_clientes = len(entries)
    total_confirmados = len([e for e in entries if e.status != "pending" and not e.is_pre_reservation])
    total_pre_reservas = len([e for e in entries if e.is_pre_reservation])
    total_pendentes = len([e for e in entries if e.status == "pending"])

    return templates.TemplateResponse(
        "meus_clientes.html",
        {
            "request": request,
            "current_user": user,
            "fmt_brasilia": fmt_brasilia,
            "title": "Meus Clientes",
            "days": ordered_days,
            "entries_by_day": entries_by_day,
            "surgeons": surgeons,
            "weekday_map": weekday_map,
            "users_by_id": users_by_id,
            "procedures_by_entry": procedures_by_entry,
            "search": search or "",
            "selected_month": selected_month,
            "selected_year": selected_year,
            "total_clientes": total_clientes,
            "total_confirmados": total_confirmados,
            "total_pre_reservas": total_pre_reservas,
            "total_pendentes": total_pendentes,
        },
    )

@app.get("/validacao_feegow", response_class=HTMLResponse)
def validacao_feegow_page(
    request: Request,
    run_id: Optional[int] = None,
    err: str = "",
    ok: str = "",
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"), "Acesso restrito à Auditoria Feegow.")

    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    mappings = session.exec(
        select(FeegowProfessionalMap).order_by(FeegowProfessionalMap.surgeon_user_id)
    ).all()
    mappings_by_user_id = {m.surgeon_user_id: m for m in mappings}

    selected_run = None
    if run_id:
        selected_run = session.get(FeegowValidationRun, run_id)

    if not selected_run:
        selected_run = session.exec(
            select(FeegowValidationRun).order_by(FeegowValidationRun.id.desc())
        ).first()

    results = []
    if selected_run:
        results = session.exec(
            select(FeegowValidationResult)
            .where(FeegowValidationResult.run_id == selected_run.id)
            .order_by(
                FeegowValidationResult.map_day,
                FeegowValidationResult.map_surgeon_name,
                FeegowValidationResult.map_patient_name,
            )
        ).all()

    today_sp = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    default_end = today_sp + timedelta(days=30)
    feegow_alert_count = get_latest_feegow_alert_count(session)

    audit_event(
        request,
        user,
        "feegow_validation_page_view",
        target_type="feegow_validation",
        target_id=selected_run.id if selected_run else None,
    )

    return templates.TemplateResponse(
        "validacao_feegow.html",
        {
            "request": request,
            "current_user": user,
            "title": "Auditoria Feegow",
            "feegow_alert_count": feegow_alert_count,
            "surgeons": surgeons,
            "mappings_by_user_id": mappings_by_user_id,
            "selected_run": selected_run,
            "results": results,
            "today_iso": today_sp.isoformat(),
            "default_end_iso": default_end.isoformat(),
            "err": err,
            "ok": ok,
            "feegow_token_configured": bool(FEEGOW_API_TOKEN),
        },
    )

@app.post("/validacao_feegow/mapeamentos")
async def validacao_feegow_save_mappings(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"), "Acesso restrito à Auditoria Feegow.")

    form = await request.form()

    surgeons = session.exec(
        select(User)
        .where(User.role == "doctor", User.is_active == True)
        .order_by(User.full_name)
    ).all()

    existing = session.exec(select(FeegowProfessionalMap)).all()
    existing_by_user_id = {row.surgeon_user_id: row for row in existing}

    for surgeon in surgeons:
        raw_value = str(form.get(f"feegow_professional_id_{surgeon.id}", "") or "").strip()
        existing_row = existing_by_user_id.get(surgeon.id)

        if not raw_value:
            if existing_row:
                session.delete(existing_row)
            continue

        try:
            feegow_professional_id = int(raw_value)
        except ValueError:
            return redirect(
                "/validacao_feegow?err=" + quote(f"ID inválido informado para {surgeon.full_name}.")
            )

        if existing_row:
            existing_row.feegow_professional_id = feegow_professional_id
            existing_row.surgeon_name_snapshot = surgeon.full_name
            existing_row.updated_by_id = user.id
            existing_row.updated_at = datetime.utcnow()
            session.add(existing_row)
        else:
            session.add(
                FeegowProfessionalMap(
                    surgeon_user_id=surgeon.id,
                    feegow_professional_id=feegow_professional_id,
                    surgeon_name_snapshot=surgeon.full_name,
                    created_by_id=user.id,
                    updated_by_id=user.id,
                )
            )

    session.commit()

    audit_event(
        request,
        user,
        "feegow_validation_mapping_saved",
        target_type="feegow_validation",
        message="Mapeamentos Feegow atualizados manualmente.",
    )

    return redirect("/validacao_feegow?ok=" + quote("Mapeamentos salvos com sucesso."))

@app.post("/validacao_feegow/executar")
def validacao_feegow_executar(
    request: Request,
    start_date: str = Form(...),
    end_date: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"), "Acesso restrito à Auditoria Feegow.")

    if not FEEGOW_API_TOKEN:
        return redirect(
            "/validacao_feegow?err=" + quote("A variável FEEGOW_API_TOKEN não está configurada no ambiente.")
        )

    try:
        period_start = datetime.fromisoformat(start_date).date()
        period_end = datetime.fromisoformat(end_date).date()
    except ValueError:
        return redirect(
            "/validacao_feegow?err=" + quote("Período inválido. Informe datas válidas.")
        )

    if period_end < period_start:
        return redirect(
            "/validacao_feegow?err=" + quote("A data final não pode ser menor que a data inicial.")
        )

    range_days = (period_end - period_start).days
    if range_days > FEEGOW_VALIDATION_MAX_DAYS:
        return redirect(
            "/validacao_feegow?err=" + quote("Na primeira versão, a validação aceita no máximo 30 dias por execução.")
        )

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= period_start,
            SurgicalMapEntry.day <= period_end,
            visible_surgical_map_status_clause(),
        )
        .order_by(
            SurgicalMapEntry.day,
            SurgicalMapEntry.surgeon_id,
            SurgicalMapEntry.time_hhmm,
            SurgicalMapEntry.patient_name,
        )
    ).all()

    mappings = session.exec(select(FeegowProfessionalMap)).all()
    mappings_by_surgeon_id = {m.surgeon_user_id: m for m in mappings}

    surgeons = session.exec(select(User).where(User.role == "doctor")).all()
    surgeons_by_id = {s.id: s for s in surgeons if s.id is not None}

    run = FeegowValidationRun(
        period_start=period_start,
        period_end=period_end,
        status="completed",
        created_by_id=user.id,
        total_entries=len(entries),
        notes_json={"executed_by": user.username},
    )
    session.add(run)
    session.commit()
    session.refresh(run)

    patient_cache: dict[int, str] = {}
    appointments_by_professional: dict[int, list[dict[str, Any]]] = {}
    results_to_save: list[FeegowValidationResult] = []

    total_ok = 0
    total_alert = 0
    total_unmapped = 0
    total_api_error = 0

    for entry in entries:
        surgeon = surgeons_by_id.get(entry.surgeon_id)
        mapping = mappings_by_surgeon_id.get(entry.surgeon_id)

        if not mapping:
            total_unmapped += 1
            results_to_save.append(
                FeegowValidationResult(
                    run_id=run.id,
                    surgical_entry_id=entry.id,
                    map_day=entry.day,
                    map_patient_name=entry.patient_name,
                    map_surgeon_id=entry.surgeon_id,
                    map_surgeon_name=surgeon.full_name if surgeon else "—",
                    validation_status="surgeon_not_mapped",
                    detail_message="Cirurgião sem mapeamento manual para o Feegow.",
                )
            )
            continue

        professional_id = mapping.feegow_professional_id

        if professional_id not in appointments_by_professional:
            try:
                appointments_by_professional[professional_id] = fetch_feegow_appointments_for_professional(
                    professional_id=professional_id,
                    start_date=period_start,
                    end_date=period_end,
                    patient_cache=patient_cache,
                )
            except Exception as exc:
                appointments_by_professional[professional_id] = [{"__api_error__": str(exc)}]

        professional_rows = appointments_by_professional.get(professional_id, [])

        if professional_rows and professional_rows[0].get("__api_error__"):
            total_api_error += 1
            results_to_save.append(
                FeegowValidationResult(
                    run_id=run.id,
                    surgical_entry_id=entry.id,
                    map_day=entry.day,
                    map_patient_name=entry.patient_name,
                    map_surgeon_id=entry.surgeon_id,
                    map_surgeon_name=surgeon.full_name if surgeon else "—",
                    validation_status="api_error",
                    detail_message=professional_rows[0]["__api_error__"],
                    matched_feegow_professional_id=professional_id,
                )
            )
            continue

        match, detail_message = find_matching_feegow_appointment(entry, professional_rows)

        if match:
            total_ok += 1
            results_to_save.append(
                FeegowValidationResult(
                    run_id=run.id,
                    surgical_entry_id=entry.id,
                    map_day=entry.day,
                    map_patient_name=entry.patient_name,
                    map_surgeon_id=entry.surgeon_id,
                    map_surgeon_name=surgeon.full_name if surgeon else "—",
                    validation_status="ok",
                    detail_message=detail_message,
                    matched_feegow_professional_id=professional_id,
                    matched_feegow_agendamento_id=match.get("agendamento_id"),
                    matched_feegow_patient_id=match.get("paciente_id"),
                    matched_feegow_patient_name=match.get("patient_name"),
                    matched_feegow_date=match.get("data"),
                    raw_match_json=match,
                )
            )
        else:
            total_alert += 1
            results_to_save.append(
                FeegowValidationResult(
                    run_id=run.id,
                    surgical_entry_id=entry.id,
                    map_day=entry.day,
                    map_patient_name=entry.patient_name,
                    map_surgeon_id=entry.surgeon_id,
                    map_surgeon_name=surgeon.full_name if surgeon else "—",
                    validation_status="alert",
                    detail_message=detail_message,
                    matched_feegow_professional_id=professional_id,
                )
            )

    if results_to_save:
        session.add_all(results_to_save)

    run.total_ok = total_ok
    run.total_alert = total_alert
    run.total_unmapped = total_unmapped
    run.total_api_error = total_api_error
    run.notes_json = {
        "executed_by": user.username,
        "patient_cache_size": len(patient_cache),
        "professionals_queried": list(appointments_by_professional.keys()),
    }

    session.add(run)
    session.commit()

    audit_event(
        request,
        user,
        "feegow_validation_executed",
        target_type="feegow_validation",
        target_id=run.id,
        message="Auditoria Feegow executada.",
        extra={
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "total_entries": len(entries),
            "total_ok": total_ok,
            "total_alert": total_alert,
            "total_unmapped": total_unmapped,
            "total_api_error": total_api_error,
        },
    )

    return redirect("/validacao_feegow?run_id=" + str(run.id) + "&ok=" + quote("Auditoria executada com sucesso."))
        
@app.get("/logs", response_class=HTMLResponse)
def logs_page(
    request: Request,
    entry_id: Optional[int] = None,
    action: str = "",
    patient_name: str = "",
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.username == "johnny.ge", "Acesso restrito aos logs.")

    q = (
        select(AuditLog)
        .where(AuditLog.target_type == "surgical_map")
        .order_by(AuditLog.id.desc())
    )

    if entry_id:
        q = q.where(AuditLog.target_id == entry_id)

    if action.strip():
        q = q.where(AuditLog.action == action.strip())

    raw_logs = session.exec(q).all()

    parsed_logs = []
    entry_ids = set()

    for log in raw_logs:
        extra = {}
        raw_extra = getattr(log, "extra_json", None)

        if raw_extra:
            try:
                extra = json.loads(raw_extra)
            except Exception:
                extra = {}

        snapshot_before = extra.get("snapshot_before") or {}
        snapshot_after = extra.get("snapshot_after") or {}
        snapshot_ref = snapshot_after or snapshot_before or {}

        target_id = getattr(log, "target_id", None) or snapshot_ref.get("entry_id")
        if target_id:
            entry_ids.add(target_id)

        parsed_logs.append({
            "id": getattr(log, "id", None),
            "target_id": target_id,
            "action": getattr(log, "action", ""),
            "actor_username": getattr(log, "actor_username", None),
            "actor_role": getattr(log, "actor_role", None),
            "success": getattr(log, "success", True),
            "message": getattr(log, "message", None),
            "when": fmt_brasilia(getattr(log, "created_at", None)) if getattr(log, "created_at", None) else "—",
            "changes": extra.get("changes", []) or [],
            "snapshot_before": snapshot_before,
            "snapshot_after": snapshot_after,
            "path": getattr(log, "path", None),
            "method": getattr(log, "method", None),
        })

    entries_map: dict[int, SurgicalMapEntry] = {}
    if entry_ids:
        rows = session.exec(
            select(SurgicalMapEntry).where(SurgicalMapEntry.id.in_(list(entry_ids)))
        ).all()
        entries_map = {r.id: r for r in rows if r.id is not None}

    if patient_name.strip():
        needle = patient_name.strip().upper()
        filtered = []
        for log in parsed_logs:
            s_after = log["snapshot_after"] or {}
            s_before = log["snapshot_before"] or {}
            patient = (s_after.get("patient_name") or s_before.get("patient_name") or "").upper()
            if needle in patient:
                filtered.append(log)
        parsed_logs = filtered

    action_labels = {
        "surgical_map_created": "Criação",
        "surgical_map_updated": "Edição",
        "surgical_map_deleted": "Exclusão lógica",
        "surgical_map_auth_requested": "Pedido de autorização",
        "surgical_map_auth_approved": "Aprovação",
        "surgical_map_auth_denied": "Reprovação",
        "surgical_map_override_agenda_block": "Override de bloqueio",
        "surgical_map_create_validation_error": "Tentativa bloqueada por regra",
        "surgical_map_blocked_by_agenda_block": "Tentativa bloqueada por agenda",
    }

    grouped: dict[int, dict[str, Any]] = {}

    for log in parsed_logs:
        tid = log["target_id"]
        if not tid:
            continue

        entry = entries_map.get(tid)
        snap = log["snapshot_after"] or log["snapshot_before"] or {}

        group = grouped.setdefault(
            tid,
            {
                "entry_id": tid,
                "patient_name": snap.get("patient_name") or (entry.patient_name if entry else "—"),
                "day": snap.get("day") or (entry.day.isoformat() if entry and entry.day else "—"),
                "status": snap.get("status") or (entry.status if entry else "—") or "active",
                "events": [],
                "last_log_id": log["id"] or 0,
            },
        )

        group["patient_name"] = snap.get("patient_name") or group["patient_name"]
        group["day"] = snap.get("day") or group["day"]
        group["status"] = snap.get("status") or group["status"]

        group["events"].append({
            **log,
            "action_label": action_labels.get(log["action"], log["action"]),
        })

        if (log["id"] or 0) > group["last_log_id"]:
            group["last_log_id"] = log["id"] or 0

    grouped_cards = sorted(grouped.values(), key=lambda x: x["last_log_id"], reverse=True)

    return templates.TemplateResponse(
        "logs.html",
        {
            "request": request,
            "current_user": user,
            "title": "Logs",
            "grouped_cards": grouped_cards,
            "entry_id_filter": entry_id or "",
            "action_filter": action,
            "patient_name_filter": patient_name,
            "action_options": [
                ("", "Todas as ações"),
                ("surgical_map_created", "Criação"),
                ("surgical_map_updated", "Edição"),
                ("surgical_map_deleted", "Exclusão lógica"),
                ("surgical_map_auth_requested", "Pedido de autorização"),
                ("surgical_map_auth_approved", "Aprovação"),
                ("surgical_map_auth_denied", "Reprovação"),
            ],
        },
    )

@app.get("/calculadora")
def calculadora_page(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    return templates.TemplateResponse(
        "calculadora.html",
        {"request": request, "current_user": user},
    )

def build_relatorio_execucao_data(session: Session, month: Optional[str] = None) -> dict:
    today = date.today()

    if not month:
        month = today.strftime("%Y-%m")

    year, m = month.split("-")
    year = int(year)
    m = int(m)

    first_day = date(year, m, 1)

    if m == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, m + 1, 1)

    rows = session.exec(
        select(
            SurgicalMapEntry.day,
            SurgicalMapEntry.patient_name,
            SurgicalMapEntry.surgeon_id,
            SurgicalMapEntry.created_by_id,
            SurgeryProcedureItem.procedure_name_snapshot,
            SurgeryProcedureItem.nucleus_snapshot,
            SurgeryProcedureItem.amount,
        )
        .join(
            SurgeryProcedureItem,
            SurgeryProcedureItem.surgery_entry_id == SurgicalMapEntry.id,
        )
        .where(
            SurgicalMapEntry.day >= first_day,
            SurgicalMapEntry.day < next_month,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.patient_name)
    ).all()

    surgeons = session.exec(
        select(User).where(User.role == "doctor")
    ).all()
    surgeons_map = {s.id: s.full_name for s in surgeons}

    users = session.exec(select(User)).all()
    users_map = {u.id: u.full_name for u in users}

    data = []
    totals_by_nucleus = defaultdict(float)
    totals_by_surgeon = defaultdict(float)
    procedure_counter = defaultdict(int)

    total_amount = 0.0

    for r in rows:
        amount = float(r.amount or 0)

        row = {
            "date": r.day.strftime("%d/%m/%Y"),
            "patient": r.patient_name,
            "surgeon": surgeons_map.get(r.surgeon_id, "—"),
            "creator_full_name": users_map.get(r.created_by_id, "—"),
            "procedure": r.procedure_name_snapshot,
            "nucleus": r.nucleus_snapshot,
            "amount": amount,
        }
        data.append(row)

        total_amount += amount
        totals_by_nucleus[r.nucleus_snapshot] += amount
        totals_by_surgeon[surgeons_map.get(r.surgeon_id, "—")] += amount
        procedure_counter[r.procedure_name_snapshot] += 1

    procedure_count = len(data)
    ticket_avg = (total_amount / procedure_count) if procedure_count else 0.0

    nucleus_chart = [
        {"label": k, "value": v}
        for k, v in sorted(totals_by_nucleus.items(), key=lambda x: x[1], reverse=True)
    ]

    surgeon_chart = [
        {"label": k, "value": v}
        for k, v in sorted(totals_by_surgeon.items(), key=lambda x: x[1], reverse=True)
    ]

    top_procedures = [
        {"label": k, "count": v}
        for k, v in sorted(procedure_counter.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    max_nucleus = max([x["value"] for x in nucleus_chart], default=0)
    max_surgeon = max([x["value"] for x in surgeon_chart], default=0)

    for item in nucleus_chart:
        item["percent"] = (item["value"] / max_nucleus * 100) if max_nucleus else 0

    for item in surgeon_chart:
        item["percent"] = (item["value"] / max_surgeon * 100) if max_surgeon else 0

    return {
        "selected_month": month,
        "data": data,
        "total_amount": total_amount,
        "procedure_count": procedure_count,
        "ticket_avg": ticket_avg,
        "nucleus_chart": nucleus_chart,
        "surgeon_chart": surgeon_chart,
        "top_procedures": top_procedures,
    }

def classify_hsr_slot_from_items(items: list[SurgeryProcedureItem]) -> str:
    """
    Regras:
    - ignora 'Hospedagem' como núcleo
    - sem itens => Slot não identificado
    - se houver mais de 1 núcleo cirúrgico => não usa slot
    - Corporal + 'abdominoplastia' => Abdominoplastia
    - Corporal sem 'abdominoplastia' => Lipo
    - Mama + 'mastopexia' => Mastopexia
    - Mama sem 'mastopexia' => Mama
    - se não for possível classificar, mas uses_hsr existir => Slot não identificado
    """
    if not items:
        return "Slot não identificado"

    nuclei = set()
    names = []

    for item in items:
        nucleus = (item.nucleus_snapshot or "").strip().lower()
        name = (item.procedure_name_snapshot or "").strip().lower()

        if name:
            names.append(name)

        if nucleus and nucleus != "hospedagem":
            nuclei.add(nucleus)

    # cirurgia combinada continua SEM slot
    if len(nuclei) > 1:
        return "Slot bloqueado"

    # não conseguiu identificar núcleo cirúrgico
    if len(nuclei) == 0:
        return "Slot não identificado"

    only_nucleus = next(iter(nuclei))
    names_join = " ".join(names)

    if only_nucleus == "corporal":

        abd_keywords = [
            "abdominoplastia",
            "abdomen total",
            "abdomem total",
            "miniabdomen",
            "miniabdomem",
        ]

        for k in abd_keywords:
            if k in names_join:
                return "Abdominoplastia"

        return "Lipo"

    if only_nucleus == "mama":
        if "mastopexia" in names_join:
            return "Mastopexia"
        return "Mama"

    return "Slot não identificado"

def classify_hsr_slot_from_catalog(procedures: list[ProcedureCatalog]) -> str:
    fake_items = [
        SimpleNamespace(
            nucleus_snapshot=(p.nucleus or ""),
            procedure_name_snapshot=(p.name or ""),
        )
        for p in procedures
    ]
    return classify_hsr_slot_from_items(fake_items)


def get_hsr_slot_config() -> tuple[set[int], list[str], list[str]]:
    blocked_months = {1, 6, 7, 12}
    limited_slot_types = ["Abdominoplastia", "Lipo", "Mastopexia", "Mama"]
    slot_types = limited_slot_types + ["Slot não identificado", "Slot bloqueado"]
    return blocked_months, limited_slot_types, slot_types


def build_hsr_proc_qs(procedure_ids: list[int] | None, form_data: dict) -> str:
    proc_qs = ""
    for pid in (procedure_ids or []):
        proc_qs += f"&procedure_id={pid}"
        proc_qs += f"&procedure_amount_{pid}={quote(str(form_data.get(f'procedure_amount_{pid}', '') or ''))}"
    return proc_qs


def validate_hsr_slot_availability(
    session: Session,
    *,
    day: date,
    uses_hsr: bool,
    procedure_ids: list[int] | None,
    exclude_entry_id: int | None = None,
) -> tuple[str | None, str | None, int, int]:
    if not uses_hsr:
        return None, None, 0, 0

    blocked_months, limited_slot_types, _ = get_hsr_slot_config()

    if day.month in blocked_months:
        return "Regra: não é permitido agendar Slot HSR neste mês.", None, 0, 0

    if not procedure_ids:
        return "Selecione ao menos um procedimento para classificar o Slot HSR.", None, 0, 0

    procedures = session.exec(
        select(ProcedureCatalog).where(
            ProcedureCatalog.id.in_(procedure_ids),
            ProcedureCatalog.is_active == True,
        )
    ).all()

    slot_type = classify_hsr_slot_from_catalog(procedures)

    if slot_type == "Slot bloqueado":
        return "Os procedimentos selecionados não se enquadram em um Slot HSR válido.", slot_type, 0, 0

    if slot_type not in limited_slot_types:
        return None, slot_type, 0, 0

    start_day = date(day.year, day.month, 1)
    if day.month == 12:
        end_day = date(day.year + 1, 1, 1)
    else:
        end_day = date(day.year, day.month + 1, 1)

    entries = session.exec(
        select(SurgicalMapEntry).where(
            SurgicalMapEntry.day >= start_day,
            SurgicalMapEntry.day < end_day,
            SurgicalMapEntry.uses_hsr == True,
        )
    ).all()

    if exclude_entry_id is not None:
        entries = [e for e in entries if e.id != exclude_entry_id]

    entry_ids = [e.id for e in entries if e.id is not None]

    items = []
    if entry_ids:
        items = session.exec(
            select(SurgeryProcedureItem).where(
                SurgeryProcedureItem.surgery_entry_id.in_(entry_ids)
            )
        ).all()

    items_by_entry: dict[int, list[SurgeryProcedureItem]] = defaultdict(list)
    for item in items:
        items_by_entry[item.surgery_entry_id].append(item)

    used = 0
    total = 4

    for entry in entries:
        entry_items = items_by_entry.get(entry.id or 0, [])
        existing_slot_type = classify_hsr_slot_from_items(entry_items)
        if existing_slot_type == slot_type:
            used += 1

    if used >= total:
        return f"Quantidade máximas de slots de {slot_type} excedidas.", slot_type, used, total

    return None, slot_type, used, total


def get_mapa_hsr_month_summary(session: Session, selected_month: str) -> dict:
    year_str, month_str = selected_month.split("-")
    year = int(year_str)
    month = int(month_str)

    data = build_slot_hsr_data(session, year)
    month_data = next((m for m in data["months"] if m["month"] == month), None)

    if not month_data:
        return {
            "month_label": selected_month,
            "blocked": False,
            "rows": [],
        }

    return {
        "month_label": month_data["name"],
        "blocked": month_data["blocked"],
        "rows": month_data["by_type"],
    }

def build_slot_hsr_data(session: Session, year: int) -> dict:
    blocked_months = {1, 6, 7, 12}
    limited_slot_types = ["Abdominoplastia", "Lipo", "Mastopexia", "Mama"]
    slot_types = limited_slot_types + ["Slot não identificado", "Slot bloqueado"]

    start_day = date(year, 1, 1)
    end_day = date(year + 1, 1, 1)

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(
            SurgicalMapEntry.day >= start_day,
            SurgicalMapEntry.day < end_day,
            SurgicalMapEntry.uses_hsr == True,
            visible_surgical_map_status_clause(),
        )
        .order_by(SurgicalMapEntry.day)
    ).all()

    surgeons = session.exec(select(User).where(User.role == "doctor")).all()
    surgeons_map = {s.id: s.full_name for s in surgeons if s.id is not None}

    entry_ids = [e.id for e in entries if e.id is not None]

    items = []
    if entry_ids:
        items = session.exec(
            select(SurgeryProcedureItem)
            .where(SurgeryProcedureItem.surgery_entry_id.in_(entry_ids))
            .order_by(SurgeryProcedureItem.surgery_entry_id)
        ).all()

    items_by_entry: dict[int, list[SurgeryProcedureItem]] = defaultdict(list)
    for item in items:
        items_by_entry[item.surgery_entry_id].append(item)

    month_names = [
        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
    ]

    months = []
    details = []

    annual_total_slots = 0
    annual_used_slots = 0

    for month in range(1, 13):
        month_capacity_by_type = {slot: 0 for slot in slot_types}
        month_used_by_type = {slot: 0 for slot in slot_types}

        if month not in blocked_months:
            for slot in limited_slot_types:
                month_capacity_by_type[slot] = 4

        month_entries = [e for e in entries if e.day.month == month]

        for entry in month_entries:
            entry_items = items_by_entry.get(entry.id or 0, [])
            slot_type = classify_hsr_slot_from_items(entry_items)

            if month in blocked_months:
                continue

            procedure_label = ", ".join(
                [x.procedure_name_snapshot for x in entry_items if x.procedure_name_snapshot]
            ).strip()

            if not procedure_label:
                procedure_label = "Sem procedimento cadastrado"

            if slot_type in month_used_by_type:
                month_used_by_type[slot_type] += 1

            details.append({
                "entry_id": entry.id,
                "date": entry.day.strftime("%d/%m/%Y"),
                "patient": entry.patient_name,
                "surgeon": surgeons_map.get(entry.surgeon_id, "—"),
                "procedure": procedure_label,
                "slot_type": slot_type,
                "month_label": month_names[month - 1],
                "is_blocked": slot_type == "Slot bloqueado",
            })

        total_slots = sum(month_capacity_by_type[slot] for slot in limited_slot_types)
        used_slots = sum(month_used_by_type[slot] for slot in limited_slot_types)
        available_slots = max(total_slots - used_slots, 0)

        annual_total_slots += total_slots
        annual_used_slots += used_slots

        months.append({
            "month": month,
            "name": month_names[month - 1],
            "blocked": month in blocked_months,
            "total_slots": total_slots,
            "used_slots": used_slots,
            "available_slots": available_slots,
            "usage_percent": (used_slots / total_slots * 100) if total_slots else 0,
            "by_type": [
                {
                    "label": slot,
                    "used": month_used_by_type[slot],
                    "total": month_capacity_by_type[slot],
                }
                for slot in slot_types
            ],
        })

    annual_available_slots = max(annual_total_slots - annual_used_slots, 0)

    return {
        "selected_year": year,
        "months": months,
        "details": details,
        "annual_total_slots": annual_total_slots,
        "annual_used_slots": annual_used_slots,
        "annual_available_slots": annual_available_slots,
    }
    
@app.get("/relatorio_execucao", response_class=HTMLResponse)
def relatorio_execucao(
    request: Request,
    month: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    report = build_relatorio_execucao_data(session, month)

    return templates.TemplateResponse(
        "relatorio_execucao.html",
        {
            "request": request,
            "current_user": user,
            **report,
        },
    )

@app.get("/relatorio_execucao/export")
def relatorio_execucao_export(
    request: Request,
    month: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    report = build_relatorio_execucao_data(session, month)

    wb = Workbook()
    ws = wb.active
    ws.title = "Relatório Execução"

    ws.append(["Data", "Paciente", "Cirurgião", "Vendedor", "Procedimento", "Núcleo", "Valor"])

    for row in report["data"]:
        ws.append([
            row["date"],
            row["patient"],
            row["surgeon"],
            row["creator_full_name"],
            row["procedure"],
            row["nucleus"],
            row["amount"],
        ])

    ws.append([])
    ws.append(["TOTAL DO PERÍODO", "", "", "", "", "", report["total_amount"]])

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"relatorio_execucao_{report['selected_month'].replace('-', '_')}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
    
@app.get("/relatorio_gustavo", response_class=HTMLResponse)
def relatorio_gustavo_page(
    request: Request,
    snapshot_date: str = "",
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge")

    snaps = session.exec(
        select(GustavoAgendaSnapshot).order_by(GustavoAgendaSnapshot.snapshot_date.desc())
    ).all()
    available_dates = [s.snapshot_date.isoformat() for s in snaps]

    selected = None
    if snapshot_date:
        try:
            y, m, d = map(int, snapshot_date.split("-"))
            sel = date(y, m, d)
            selected = session.exec(
                select(GustavoAgendaSnapshot).where(GustavoAgendaSnapshot.snapshot_date == sel)
            ).first()
        except Exception:
            selected = None
    today_sp = datetime.now(TZ).date()
    selected_keys = set(load_gustavo_selected_month_keys(today_sp))

    # opções: Jan..Dez do ano atual
    yy = today_sp.year
    pt_abbr = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    month_options = []
    for i in range(1, 13):
        key = f"{yy:04d}-{i:02d}"
        label = f"{pt_abbr[i-1]}/{str(yy)[2:]}"
        month_options.append({"key": key, "label": label})
        
    overrides = load_gustavo_overrides()

    return templates.TemplateResponse(
        "relatorio_gustavo.html",
        {
            "request": request,
            "current_user": user,
            "available_dates": available_dates,
            "snapshot": selected,
            "snapshot_date": snapshot_date or "",
            "month_options": month_options,
            "selected_months": selected_keys,
            "surgeons": GUSTAVO_REPORT_SURGEONS,
            "overrides": overrides,
        },
    )

@app.post("/relatorio_gustavo/config")
def relatorio_gustavo_save_config(
    request: Request,
    selected_months: list[str] = Form(default=[]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge")

    # salva exatamente o que veio marcado (se vier vazio, cai no default na geração)
    keys = []
    for k in selected_months or []:
        if isinstance(k, str) and len(k) == 7 and k[4] == "-":
            keys.append(k)

    save_gustavo_selected_month_keys(keys)
    return redirect("/relatorio_gustavo")


@app.post("/relatorio_gustavo/override")
def relatorio_gustavo_save_override(
    request: Request,
    day_iso: str = Form(...),
    surgeon_username: str = Form(...),
    emoji: str = Form(...),
    reason: str = Form(default=""),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge")

    day_iso = (day_iso or "").strip()
    surgeon_username = (surgeon_username or "").strip()
    emoji = (emoji or "").strip()
    reason = (reason or "").strip()

    # valida data
    try:
        _ = date.fromisoformat(day_iso)
    except Exception:
        raise HTTPException(status_code=400, detail="Data inválida (use YYYY-MM-DD).")

    # valida médico (somente os 6 do relatório)
    allowed = {u for (u, _lbl) in GUSTAVO_REPORT_SURGEONS}
    if surgeon_username not in allowed:
        raise HTTPException(status_code=400, detail="Cirurgião inválido para override.")

    # valida emoji
    if emoji not in REPORT_EMOJIS:
        raise HTTPException(status_code=400, detail="Emoji inválido para override.")

    data = load_gustavo_overrides()
    data.setdefault(day_iso, {})
    data[day_iso][surgeon_username] = {
        "emoji": emoji,
        "reason": reason,
        "by": user.username,
        "at": datetime.utcnow().isoformat(),
    }
    save_gustavo_overrides(data)

    audit_logger.info(
        f"GUSTAVO_REPORT_OVERRIDE: day={day_iso} surgeon={surgeon_username} emoji={emoji} by={user.username}"
    )
    return redirect("/relatorio_gustavo")


@app.post("/relatorio_gustavo/override/delete")
def relatorio_gustavo_delete_override(
    request: Request,
    day_iso: str = Form(...),
    surgeon_username: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge")

    day_iso = (day_iso or "").strip()
    surgeon_username = (surgeon_username or "").strip()

    data = load_gustavo_overrides()
    if day_iso in data and surgeon_username in (data.get(day_iso) or {}):
        data[day_iso].pop(surgeon_username, None)
        if not data[day_iso]:
            data.pop(day_iso, None)
        save_gustavo_overrides(data)

        audit_logger.info(
            f"GUSTAVO_REPORT_OVERRIDE_DELETE: day={day_iso} surgeon={surgeon_username} by={user.username}"
        )

    return redirect("/relatorio_gustavo")

@app.get("/relatorio_gustavo/preview", response_class=HTMLResponse)
def relatorio_gustavo_preview(
    request: Request,
    months: list[str] = Query(default=[]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.username == "johnny.ge")

    # ✅ gera na hora (não salva snapshot e não envia WhatsApp)
    today_sp = datetime.now(TZ).date()
    month_keys = months or None  # None => usa config salva (ou default)
    msg1, msg2, _payload = build_gustavo_whatsapp_messages(session, today_sp,month_keys=month_keys)

    preview_snapshot = SimpleNamespace(message_1=msg1, message_2=msg2)

    # mantém dropdown funcionando (com datas já salvas), mas exibe preview no corpo
    snaps = session.exec(
        select(GustavoAgendaSnapshot).order_by(GustavoAgendaSnapshot.snapshot_date.desc())
    ).all()
    available_dates = [s.snapshot_date.isoformat() for s in snaps]

    today_sp = datetime.now(TZ).date()

    # selecionados do preview: se veio query ?months=... usa ela; senão usa config salva
    selected_keys = set(months or load_gustavo_selected_month_keys(today_sp))

    yy = today_sp.year
    pt_abbr = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    month_options = []
    for i in range(1, 13):
        key = f"{yy:04d}-{i:02d}"
        label = f"{pt_abbr[i-1]}/{str(yy)[2:]}"
        month_options.append({"key": key, "label": label})

    overrides = load_gustavo_overrides()

    return templates.TemplateResponse(
        "relatorio_gustavo.html",
        {
            "request": request,
            "current_user": user,
            "available_dates": available_dates,
            "snapshot": preview_snapshot,
            "snapshot_date": "",  # não “seleciona” nenhuma data salva
            "month_options": month_options,
            "selected_months": selected_keys,
            "surgeons": GUSTAVO_REPORT_SURGEONS,
            "overrides": overrides,
        },
    )


@app.post("/relatorio_gustavo/run-now")
def relatorio_gustavo_run_now(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    # Somente admin ou surgery podem gerar manualmente
    require(user.username == "johnny.ge")

    # Data de hoje no fuso de SP
    now_sp = datetime.now(TZ)
    today_sp = now_sp.date()

    audit_logger.info(
        f"GUSTAVO_SNAPSHOT: geração manual solicitada por {user.username} em {today_sp}"
    )

    try:
        save_gustavo_snapshot_and_send(session, today_sp)
    except Exception as e:
        audit_logger.exception("Erro ao gerar snapshot manualmente")
        raise HTTPException(status_code=500, detail="Erro ao gerar snapshot")

    # Volta para a tela já selecionando a data gerada
    return redirect(f"/relatorio_gustavo?snapshot_date={today_sp.isoformat()}")

# ============================================================
# HOSPEDAGEM
# ============================================================

def normalize_unit(raw: Optional[str]) -> str:
    v = (raw or "").strip().lower()
    v = v.replace("suíte", "suite").replace("suíte", "suite")
    v = v.replace("-", " ").replace("_", " ")
    v = " ".join(v.split())  # colapsa múltiplos espaços

    if v in ("suite 1", "suite1", "suíte 1", "s1", "1", "01"):
        return "suite_1"
    if v in ("suite 2", "suite2", "suíte 2", "s2", "2", "02"):
        return "suite_2"
    if v in ("apto", "apt", "apartamento", "apartmento"):
        return "apto"

    # se já vier no padrão
    if v in ("suite_1", "suite_2", "apto"):
        return v

    return v
@app.get("/hotel_mobile", response_class=HTMLResponse)
def hotel_mobile_page(
    request: Request,
    day: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login?next=/hotel_mobile")

    require(user.role in ("admin", "surgery", "viewer"))

    try:
        ref_day = date.fromisoformat(day) if day else datetime.now(TZ).date()
    except Exception:
        ref_day = datetime.now(TZ).date()

    dashboard = build_hotel_dashboard_data(session, ref_day)

    return templates.TemplateResponse(
        "hotel_mobile.html",
        {
            "request": request,
            "current_user": user,
            "push_public_key": WEBPUSH_VAPID_PUBLIC_KEY,
            "push_configured": webpush_is_configured(),
            **dashboard,
        },
    )


@app.get("/tasks", response_class=HTMLResponse)
def tasks_page(
    request: Request,
    day: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login?next=/tasks")

    require(user.role in ("admin", "surgery"), "Acesso restrito às tarefas do mapa cirúrgico.")

    try:
        ref_day = date.fromisoformat(day) if day else datetime.now(TZ).date()
    except Exception:
        ref_day = datetime.now(TZ).date()

    seller_filter = None if user.role == "admin" else user.id

    tasks = build_all_chart_tasks(
        session,
        start_day=ref_day,
        end_day=ref_day + timedelta(days=60),
        seller_user_id=seller_filter,
    )

    tasks_today = [t for t in tasks if t["alert_day"] == ref_day]
    upcoming_tasks = [t for t in tasks if t["alert_day"] > ref_day]

    return templates.TemplateResponse(
        "tasks.html",
        {
            "request": request,
            "current_user": user,
            "ref_day": ref_day,
            "tasks_today": tasks_today,
            "upcoming_tasks": upcoming_tasks,
            "push_public_key": WEBPUSH_VAPID_PUBLIC_KEY,
            "push_configured": webpush_is_configured(),
            "fmt_brasilia": fmt_brasilia,
        },
    )


@app.post("/tasks/complete")
def complete_task(
    request: Request,
    task_key: str = Form(...),
    ref_day: str = Form(""),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"), "Acesso restrito às tarefas do mapa cirúrgico.")

    try:
        day_ref = date.fromisoformat(ref_day) if ref_day else datetime.now(TZ).date()
    except Exception:
        day_ref = datetime.now(TZ).date()

    seller_filter = None if user.role == "admin" else user.id
    tasks = build_all_chart_tasks(
        session,
        start_day=day_ref - timedelta(days=7),
        end_day=day_ref + timedelta(days=30),
        seller_user_id=seller_filter,
    )

    task = next((t for t in tasks if t["task_key"] == task_key), None)

    if not task:
        return redirect(f"/tasks?day={day_ref.isoformat()}")

    if task.get("completed"):
        return redirect(f"/tasks?day={day_ref.isoformat()}")

    audit_event(
        request,
        user,
        "chart_task_completed",
        target_type="chart_task",
        message=f"Tarefa concluída: {task['patient_name']} • {task.get('task_type_label', 'Task')}",
        extra={
            "task_key": task["task_key"],
            "task_type": task.get("task_type"),
            "task_type_label": task.get("task_type_label"),
            "patient_name": task["patient_name"],
            "seller_id": task["seller_id"],
            "seller_name": task["seller_name"],
            "surgery_day": task["surgery_day"].isoformat(),
            "alert_day": task["alert_day"].isoformat(),
            "surgeons": task["surgeons"],
        },
    )

    return redirect(f"/tasks?day={day_ref.isoformat()}")

@app.post("/api/push/subscribe")
async def push_subscribe(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        raise HTTPException(status_code=401, detail="Não autenticado")

    require(user.role in ("admin", "surgery", "viewer"))

    if not webpush_is_configured():
        return JSONResponse(
            {"ok": False, "message": "Push não configurado no servidor."},
            status_code=503,
        )

    payload = await request.json()

    endpoint = ((payload or {}).get("endpoint") or "").strip()
    keys = (payload or {}).get("keys") or {}
    p256dh = (keys.get("p256dh") or "").strip()
    auth = (keys.get("auth") or "").strip()

    if not endpoint or not p256dh or not auth:
        raise HTTPException(status_code=400, detail="Subscription inválida")

    row = session.exec(
        select(PushSubscription).where(PushSubscription.endpoint == endpoint)
    ).first()

    if row:
        row.user_id = user.id
        row.p256dh = p256dh
        row.auth = auth
        row.is_active = True
        row.user_agent = request.headers.get("user-agent")
        row.updated_at = datetime.utcnow()
    else:
        row = PushSubscription(
            user_id=user.id,
            endpoint=endpoint,
            p256dh=p256dh,
            auth=auth,
            is_active=True,
            user_agent=request.headers.get("user-agent"),
        )
        session.add(row)

    session.commit()
    return {"ok": True}


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        raise HTTPException(status_code=401, detail="Não autenticado")

    require(user.role in ("admin", "surgery", "viewer"))

    payload = await request.json()
    endpoint = ((payload or {}).get("endpoint") or "").strip()

    if not endpoint:
        raise HTTPException(status_code=400, detail="Endpoint não informado")

    row = session.exec(
        select(PushSubscription).where(PushSubscription.endpoint == endpoint)
    ).first()

    if row:
        row.is_active = False
        row.updated_at = datetime.utcnow()
        session.add(row)
        session.commit()

    return {"ok": True}
    
@app.get("/hospedagem", response_class=HTMLResponse)
def hospedagem_page(
    request: Request,
    month: Optional[str] = None,
    err: Optional[str] = None,
    open: Optional[str] = None,
    unit: Optional[str] = None,
    check_in: Optional[str] = None,
    check_out: Optional[str] = None,
    patient_name: Optional[str] = None,
    is_pre_reservation: Optional[str] = None,
    conflict_id: Optional[str] = None,
    note: Optional[str] = None,
    edit_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    allow_override = (user.username == "johnny.ge")

    conflict_obj = None
    if allow_override and conflict_id:
        try:
            cid = int(conflict_id)
            row = session.get(LodgingReservation, cid)
            if row:
                conflict_obj = {
                    "id": row.id,
                    "patient_name": row.patient_name,
                    "unit": row.unit,
                    "check_in": row.check_in.strftime("%d/%m/%Y"),
                    "check_out": row.check_out.strftime("%d/%m/%Y"),
                    "is_pre": 1 if row.is_pre_reservation else 0,
                }
        except Exception:
            conflict_obj = None

    require(user.role in ("admin", "surgery"))

    selected_month, first_day, next_month_first, days = safe_selected_month(month)
    # anos para o dropdown (ano atual até +5)
    years = list(range(first_day.year, first_day.year + 6))
    day_index = {d: i for i, d in enumerate(days)}

    units = ["suite_2", "suite_1", "apto"]
    
    audit_logger.info(
        f"HOSPEDAGEM_PAGE: selected_month={selected_month} "
        f"first_day={first_day} next_month_first={next_month_first}"
    )
    
    # busca reservas que encostam no mês (por período)
    q = select(LodgingReservation).where(
        LodgingReservation.check_in < next_month_first,
        LodgingReservation.check_out > first_day,
    )
    reservations = session.exec(q).all()

    # pré-carrega usuários criadores (para exibir no template)
    creator_ids = list({getattr(r, "created_by_id", None) for r in reservations if getattr(r, "created_by_id", None)})
    users_by_id: dict[int, User] = {}
    if creator_ids:
        users = session.exec(select(User).where(User.id.in_(creator_ids))).all()
        users_by_id = {u.id: u for u in users if u.id is not None}

    # barras por unidade (grid com colunas = dias)
    bars_by_unit: dict[str, list[dict]] = {u: [] for u in units}

    for r in reservations:
        u = normalize_unit(getattr(r, "unit", None))
        if u not in bars_by_unit:
            audit_logger.warning(f"HOSPEDAGEM_PAGE: unit_desconhecida_no_db id={r.id} unit={getattr(r,'unit',None)}")
            continue

        # clamp dentro do mês visível
        start = max(r.check_in, first_day)
        end = min(r.check_out, next_month_first)
        if start >= end:
            continue

        start_col = (start - first_day).days + 2
        end_col = (end - first_day).days + 2
        if end_col <= start_col:
            continue

        # resolve creator (robusto)
        creator_username = ""
        cid = getattr(r, "created_by_id", None)
        if isinstance(cid, str) and cid.isdigit():
            cid = int(cid)

        if cid is not None and cid in users_by_id:
            creator_username = users_by_id[cid].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None)
        if created_at:
            try:
                created_at_str = created_at.strftime("%d/%m/%Y %H:%M")
            except Exception:
                created_at_str = str(created_at)

        bars_by_unit[u].append(
            {
                "id": r.id,
                "patient_name": r.patient_name,
                "patient_cpf": getattr(r, "patient_cpf", "") or "",
                "patient_phone": getattr(r, "patient_phone", "") or "",
                "check_in": r.check_in.strftime("%d/%m/%Y"),
                "check_out": r.check_out.strftime("%d/%m/%Y"),
                "start_col": start_col,
                "end_col": end_col,
                "is_pre": 1 if r.is_pre_reservation else 0,
                "note": getattr(r, "note", "") or "",
                "created_by_username": creator_username,
                "created_at_str": created_at_str,
            }
        )

    # ✅ lista para exibir "Reservas do mês" abaixo do quadro
    reservations_list = []
    for r in reservations:
        u = normalize_unit(getattr(r, "unit", None))
        if u not in ("suite_1", "suite_2", "apto"):
            continue

        created_by_username = ""
        cid = getattr(r, "created_by_id", None)
        if isinstance(cid, str) and cid.isdigit():
            cid = int(cid)

        if cid is not None and cid in users_by_id:
            created_by_username = users_by_id[cid].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None)
        if created_at:
            try:
                created_at_str = created_at.strftime("%d/%m/%Y %H:%M")
            except Exception:
                created_at_str = str(created_at)

        reservations_list.append(
            {
                "id": r.id,
                "unit": u,
                "unit_label": human_unit(u),
                "patient_name": r.patient_name or "",
                "patient_cpf": getattr(r, "patient_cpf", "") or "",
                "patient_phone": getattr(r, "patient_phone", "") or "",
                "check_in": r.check_in.strftime("%d/%m/%Y"),
                "check_out": r.check_out.strftime("%d/%m/%Y"),
                "check_in_iso": r.check_in.strftime("%Y-%m-%d"),
                "check_out_iso": r.check_out.strftime("%Y-%m-%d"),
                "is_pre": 1 if r.is_pre_reservation else 0,
                "note": getattr(r, "note", "") or "",
                "created_by_username": created_by_username,
                "created_at_str": created_at_str,
            }
        )

    reservations_list.sort(key=lambda x: (x["check_in"], x["unit"], x["patient_name"]))

    audit_logger.info(f"HOSPEDAGEM_PAGE: reservations_found={len(reservations)}")
    if reservations:
        audit_logger.info(
            "HOSPEDAGEM_PAGE_SAMPLE: " +
            " | ".join([
                f"id={r.id},unit={r.unit},ci={r.check_in},co={r.check_out}"
                for r in reservations[:5]
            ])
        )
    
    # barras por unidade (grid com colunas = dias)
    bars_by_unit: dict[str, list[dict]] = {u: [] for u in units}

    # lista do mês (para exibir abaixo do grid)
    month_reservations = []
    for r in reservations:
        u = normalize_unit(getattr(r, "unit", None))
        if u not in ("suite_1", "suite_2", "apto"):
            continue

        creator_username = ""
        if getattr(r, "created_by_id", None) in users_by_id:
            creator_username = users_by_id[r.created_by_id].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None)
        if created_at:
            try:
                created_at_str = datetime.fromtimestamp(created_at.timestamp(), TZ).strftime("%d/%m/%Y %H:%M")
            except Exception:
                created_at_str = created_at.strftime("%d/%m/%Y %H:%M")

        month_reservations.append({
            "id": r.id,
            "unit": u,
            "unit_label": human_unit(u),
            "patient_name": r.patient_name,
            "check_in": r.check_in.strftime("%d/%m/%Y"),
            "check_out": r.check_out.strftime("%d/%m/%Y"),
            "is_pre": 1 if r.is_pre_reservation else 0,
            "note": getattr(r, "note", "") or "",
            "created_by": creator_username,
            "created_at": created_at_str,
        })

    month_reservations.sort(key=lambda x: (x["check_in"], x["unit"]))

    # pré-carrega usuários criadores (para exibir no template)
    creator_ids = list({r.created_by_id for r in reservations if getattr(r, "created_by_id", None)})
    users_by_id: dict[int, User] = {}
    if creator_ids:
        users = session.exec(select(User).where(User.id.in_(creator_ids))).all()
        users_by_id = {u.id: u for u in users if u.id is not None}

    for r in reservations:
        u = normalize_unit(getattr(r, "unit", None))
        if u not in bars_by_unit:
            # loga pra você enxergar se aparecer algum valor novo inesperado
            audit_logger.warning(f"HOSPEDAGEM_PAGE: unit_desconhecida_no_db id={r.id} unit={getattr(r,'unit',None)}")
            continue

        # clamp dentro do mês visível
        start = max(r.check_in, first_day)
        end = min(r.check_out, next_month_first)
        if start >= end:
            continue

        start_col = (start - first_day).days + 2
        end_col = (end - first_day).days + 2
        if end_col <= start_col:
            continue

        creator_username = ""
        if getattr(r, "created_by_id", None) in users_by_id:
            creator_username = users_by_id[r.created_by_id].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None)
        if created_at:
            # se você já usa TZ no projeto, mantém padrão (SP)
            try:
                created_at_str = datetime.fromtimestamp(created_at.timestamp(), TZ).strftime("%d/%m/%Y %H:%M")
            except Exception:
                created_at_str = created_at.strftime("%d/%m/%Y %H:%M")

        bars_by_unit[u].append(
            {
                "id": r.id,
                "patient_name": r.patient_name,
                "check_in": r.check_in.strftime("%d/%m/%Y"),
                "check_out": r.check_out.strftime("%d/%m/%Y"),
                "start_col": start_col,
                "end_col": end_col,
                "is_pre": 1 if r.is_pre_reservation else 0,

                # ✅ novos campos (seu template já tenta usar note/created_by)
                "note": getattr(r, "note", "") or "",
                "created_by_id": getattr(r, "created_by_id", None),
                "created_by_username": creator_username,
                "created_at": created_at_str,
            }
        )
    # ✅ lista para exibir "Reservas do mês" abaixo do quadro
    reservations_list = []
    for r in reservations:
        u = normalize_unit(getattr(r, "unit", None))
        if u not in ("suite_1", "suite_2", "apto"):
            continue

        created_by_username = ""
        cid = getattr(r, "created_by_id", None)
        if cid is not None and cid in users_by_id:
            created_by_username = users_by_id[cid].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None)
        if created_at:
            # created_at costuma ser datetime
            try:
                created_at_str = created_at.strftime("%d/%m/%Y %H:%M")
            except Exception:
                created_at_str = str(created_at)

        reservations_list.append(
            {
                "id": r.id,
                "unit": u,
                "patient_name": r.patient_name or "",
                "check_in_br": r.check_in.strftime("%d/%m/%Y"),
                "check_out_br": r.check_out.strftime("%d/%m/%Y"),
                "is_pre": 1 if r.is_pre_reservation else 0,
                "note": (getattr(r, "note", "") or ""),
                "created_by_username": created_by_username,
                "created_at_str": created_at_str,
            }
        )

    reservations_list.sort(key=lambda x: (x["check_in_br"], x["unit"], x["patient_name"]))


    # ordena barras na linha
    for u in bars_by_unit:
        bars_by_unit[u].sort(key=lambda b: (b["start_col"], b["end_col"]))

    
    prefill = {
        "unit": unit or "",
        "check_in": check_in or "",
        "check_out": check_out or "",
        "patient_name": patient_name or "",
        "is_pre_reservation": 1 if (is_pre_reservation == "1") else 0,
        "edit_id": edit_id or "",
    }
    
    # ✅ lista do mês (abaixo do grid)
    reservations_list = []
    for r in reservations:
        # só mostrar as que encostam no mês visível (mesma lógica do grid)
        start = max(r.check_in, first_day)
        end = min(r.check_out, next_month_first)
        if start >= end:
            continue

        created_by_username = ""
        cid = getattr(r, "created_by_id", None)
        if cid in users_by_id:
            created_by_username = users_by_id[cid].username or ""

        created_at_str = ""
        created_at = getattr(r, "created_at", None) or getattr(r, "created_at_dt", None)
        if created_at:
            try:
                # se vier datetime
                if hasattr(created_at, "astimezone"):
                    created_at_str = created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M")
                else:
                    created_at_str = str(created_at)
            except Exception:
                created_at_str = str(created_at)

        reservations_list.append({
            "id": r.id,
            "unit": normalize_unit(getattr(r, "unit", None)),
            "patient_name": (r.patient_name or "").strip(),
            "check_in_br": r.check_in.strftime("%d/%m/%Y"),
            "check_out_br": r.check_out.strftime("%d/%m/%Y"),
            "is_pre": 1 if getattr(r, "is_pre_reservation", False) else 0,
            "note": getattr(r, "note", None) or "",
            "created_by_username": created_by_username,
            "created_at_str": created_at_str,
        })

    # ordena por data
    reservations_list.sort(key=lambda x: (x["check_in_br"], x["check_out_br"], x["unit"], x["patient_name"]))

    
    return templates.TemplateResponse(
        "hospedagem.html",
        {
            "request": request,
            "current_user": user,
            "selected_month": selected_month,
            "days": days,
            "years": years,
            "units": units,
            "bars_by_unit": bars_by_unit,
            "reservations_list": reservations_list,
            "human_unit": human_unit,
            "err": err or "",
            "open": open or "",

            # mantém o que você já tinha (pode continuar usando no template, se quiser)
            "unit_prefill": unit or "",
            "check_in_prefill": check_in or "",
            "check_out_prefill": check_out or "",
            "patient_prefill": patient_name or "",
            "pre_prefill": 1 if (is_pre_reservation == "1") else 0,
            "edit_id": edit_id or "",
            
            "allow_override": allow_override,

            "conflict": conflict_obj,

            "prefill_note": note or "",

            # ✅ ADICIONE ISTO (para o template não quebrar com prefill.unit)
            "prefill": {
                "unit": unit or "",
                "check_in": check_in or "",
                "check_out": check_out or "",
                "patient_name": patient_name or "",
                "is_pre_reservation": 1 if (is_pre_reservation == "1") else 0,
            },
        },
    )

@app.get("/hospedagem/export_excel")
def hospedagem_export_excel(
    request: Request,
    month: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    require(user.role in ("admin", "surgery"))

    reservations = session.exec(
        select(LodgingReservation).order_by(LodgingReservation.check_in, LodgingReservation.patient_name)
    ).all()

    creator_ids = list({
        getattr(r, "created_by_id", None)
        for r in reservations
        if getattr(r, "created_by_id", None)
    })

    users_by_id: dict[int, User] = {}
    if creator_ids:
        users = session.exec(select(User).where(User.id.in_(creator_ids))).all()
        users_by_id = {u.id: u for u in users if u.id is not None}

    wb = Workbook()
    ws = wb.active
    ws.title = "Hospedagens"

    headers = [
        "Data da Reserva",
        "Paciente",
        "CPF",
        "Telefone",
        "Quarto Reservado",
        "Check-in",
        "Check-out",
        "Vendedor",
        "Observações",
    ]
    ws.append(headers)

    for r in reservations:
        created_at = getattr(r, "created_at", None)
        if created_at:
            try:
                data_reserva = created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M")
            except Exception:
                try:
                    data_reserva = created_at.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    data_reserva = str(created_at)
        else:
            data_reserva = ""

        vendedor = ""
        cid = getattr(r, "created_by_id", None)
        if isinstance(cid, str) and cid.isdigit():
            cid = int(cid)

        if cid is not None and cid in users_by_id:
            vendedor = users_by_id[cid].full_name or users_by_id[cid].username or ""

        ws.append([
            data_reserva,
            (r.patient_name or "").strip(),
            getattr(r, "patient_cpf", "") or "",
            getattr(r, "patient_phone", "") or "",
            human_unit(normalize_unit(getattr(r, "unit", None))),
            r.check_in.strftime("%d/%m/%Y") if r.check_in else "",
            r.check_out.strftime("%d/%m/%Y") if r.check_out else "",
            vendedor,
            getattr(r, "note", "") or "",
        ])

    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            try:
                max_length = max(max_length, len(str(cell.value or "")))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_length + 2, 35)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"hospedagens.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )
    
@app.post("/hospedagem/create")
def hospedagem_create(
    request: Request,
    month: str = Form(""),
    unit: str = Form(...),
    patient_name: str = Form(...),
    patient_cpf: Optional[str] = Form(None),
    patient_phone: Optional[str] = Form(None),
    check_in: str = Form(...),
    check_out: str = Form(...),
    is_pre_reservation: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    surgery_entry_id: Optional[int] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    try:
        ci = date.fromisoformat(check_in)
        co = date.fromisoformat(check_out)
    except Exception:
        return redirect(f"/hospedagem?err={quote('Datas inválidas.')}&open=1")

    e = validate_lodging_period(ci, co)
    if e:
        return redirect(f"/hospedagem?err={quote(e)}&open=1")

    e = validate_lodging_conflict(session, unit, ci, co)
    if e:
        month_param = (month or "").strip() or f"{ci.year:04d}-{ci.month:02d}"

        # ✅ somente o johnny.ge pode "ver o conflito" e optar por sobrepor
        if user.username == "johnny.ge":
            conflict = get_lodging_conflict_row(session, unit, ci, co)
            conflict_id = conflict.id if conflict else ""

            return redirect(
                f"/hospedagem?month={quote(month_param)}&open=1"
                f"&err={quote(e)}"
                f"&conflict_id={conflict_id}"
                f"&unit={quote(unit)}&check_in={quote(check_in)}&check_out={quote(check_out)}"
                f"&patient_name={quote(patient_name)}"
                f"&patient_cpf={quote(patient_cpf or '')}"
                f"&patient_phone={quote(patient_phone or '')}"
                f"&is_pre_reservation={(1 if is_pre_reservation else 0)}"
                f"&note={quote(note or '')}"
                f"&surgery_entry_id={surgery_entry_id or ''}"
            )

        # demais usuários: mantém o bloqueio (sem permissão)
        return redirect(
            f"/hospedagem?month={quote(month_param)}&open=1"
            f"&err={quote(e)}"
            f"&unit={quote(unit)}&check_in={quote(check_in)}&check_out={quote(check_out)}"
            f"&patient_name={quote(patient_name)}"
            f"&patient_cpf={quote(patient_cpf or '')}"
            f"&patient_phone={quote(patient_phone or '')}"
            f"&is_pre_reservation={(1 if is_pre_reservation else 0)}"
            f"&note={quote(note or '')}"
            f"&surgery_entry_id={surgery_entry_id or ''}"
        )

    row = LodgingReservation(
        unit=normalize_unit(unit),
        patient_name=patient_name.strip().upper(),
        patient_cpf=(patient_cpf or "").strip() or None,
        patient_phone=(patient_phone or "").strip() or None,
        check_in=ci,
        check_out=co,
        is_pre_reservation=bool(is_pre_reservation),
        note=(note or None),
        created_by_id=user.id,
        updated_by_id=user.id,
        surgery_entry_id=surgery_entry_id,
    )
    session.add(row)
    session.commit()
    session.refresh(row)

    try:
        body = build_lodging_email_body(
            action_label="fazer",
            requested_by_name=user.full_name or user.username,
            request_date=datetime.now(TZ),
            unit=row.unit,
            patient_name=row.patient_name,
            patient_cpf=row.patient_cpf,
            patient_phone=row.patient_phone,
            check_in=row.check_in,
            check_out=row.check_out,
        )
        send_lodging_email_notification(
            subject=f"[HOTEL] Nova reserva de hospedagem - {row.patient_name}",
            body=body,
            unit=row.unit,
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_EMAIL_HOSPEDAGEM_CREATE: {e}")
    
    print(f"[CREATE] tentando enviar push da hospedagem id={row.id}")

    try:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="create",
            row=row,
            event_key = f"lodging:create:{row.id}:{patient_key}:{row.created_at.isoformat()}",
        )
        print(f"[CREATE] push processado para hospedagem id={row.id}")
    except Exception as e:
        audit_logger.exception(f"ERRO_PUSH_HOSPEDAGEM_CREATE: {e}")
        print(f"[CREATE] erro no push: {e}")

    audit_event(
        request,
        user,
        action="lodging_create",
        success=True,
        message=None,
        target_type="lodging",
        target_id=row.id,
    )

    audit_logger.info(
        f"HOSPEDAGEM_CREATE: id={row.id} unit={row.unit} "
        f"ci={row.check_in} co={row.check_out} patient={row.patient_name}"
    )
 
    month_param = (month or "").strip() or f"{ci.year:04d}-{ci.month:02d}"
    return redirect(f"/hospedagem?month={month_param}")

@app.post("/hospedagem/override")
def hospedagem_override(
    request: Request,
    month: str = Form(""),
    conflict_id: int = Form(...),

    unit: str = Form(...),
    patient_name: str = Form(...),
    patient_cpf: Optional[str] = Form(None),
    patient_phone: Optional[str] = Form(None),
    check_in: str = Form(...),
    check_out: str = Form(...),
    is_pre_reservation: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    surgery_entry_id: Optional[str] = Form(None),

    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    # ✅ bloqueio TOTAL: só johnny.ge pode sobrepor
    require(user.username == "johnny.ge")

    # parse datas
    try:
        ci = date.fromisoformat(check_in)
        co = date.fromisoformat(check_out)
    except Exception:
        return redirect(f"/hospedagem?err={quote('Datas inválidas')}&open=1")

    # pega a reserva conflitante
    old = session.get(LodgingReservation, conflict_id)
    if not old:
        return redirect(f"/hospedagem?err={quote('Reserva conflitante não encontrada')}&open=1")

    # remove a antiga
    session.delete(old)
    session.commit()

    # valida novamente (caso exista outra reserva além da que foi apagada)
    e = validate_lodging_conflict(session, unit, ci, co)
    if e:
        month_param = (month or "").strip() or f"{ci.year:04d}-{ci.month:02d}"
        return redirect(
            f"/hospedagem?month={quote(month_param)}&open=1"
            f"&err={quote(e)}"
            f"&unit={quote(unit)}&check_in={quote(check_in)}&check_out={quote(check_out)}"
            f"&patient_name={quote(patient_name)}"
            f"&patient_cpf={quote(patient_cpf or '')}"
            f"&patient_phone={quote(patient_phone or '')}"
            f"&is_pre_reservation={(1 if is_pre_reservation else 0)}"
            f"&note={quote(note or '')}"
            f"&surgery_entry_id={surgery_entry_id or ''}"
        )
    surgery_entry_id_int: Optional[int] = None
    if surgery_entry_id is not None:
        s = str(surgery_entry_id).strip()
        if s.isdigit():
            surgery_entry_id_int = int(s)
            
    # cria a nova
    row = LodgingReservation(
        unit=normalize_unit(unit),
        patient_name=patient_name.strip().upper(),
        patient_cpf=(patient_cpf or "").strip() or None,
        patient_phone=(patient_phone or "").strip() or None,
        check_in=ci,
        check_out=co,
        is_pre_reservation=bool(is_pre_reservation),
        note=(note or "").strip() or None,
        created_by_id=user.id,
        updated_by_id=user.id,
        surgery_entry_id=surgery_entry_id_int,
    )
    session.add(row)
    session.commit()
    session.refresh(row)

    try:
        body = build_lodging_email_body(
            action_label="refazer / sobrepor",
            requested_by_name=user.full_name or user.username,
            request_date=datetime.now(TZ),
            unit=row.unit,
            patient_name=row.patient_name,
            patient_cpf=row.patient_cpf,
            patient_phone=row.patient_phone,
            check_in=row.check_in,
            check_out=row.check_out,
        )
        send_lodging_email_notification(
            subject=f"[HOTEL] Reserva sobreposta - {row.patient_name}",
            body=body,
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_EMAIL_HOSPEDAGEM_OVERRIDE: {e}")

    try:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="override",
            row=row,
            event_key = f"lodging:override:{row.id}:{patient_key}:{row.created_at.isoformat()}",
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_PUSH_HOSPEDAGEM_OVERRIDE: {e}")
    
    audit_logger.info(
        f"HOSPEDAGEM_OVERRIDE: deleted_id={conflict_id} | new_id={row.id} unit={row.unit} ci={row.check_in} co={row.check_out} patient={row.patient_name}"
    )

    month_param = (month or "").strip() or f"{ci.year:04d}-{ci.month:02d}"
    return redirect(f"/hospedagem?month={month_param}")

@app.post("/hospedagem/update/{res_id}")
def hospedagem_update(
    request: Request,
    res_id: int,
    unit: str = Form(...),
    patient_name: str = Form(...),
    patient_cpf: Optional[str] = Form(None),
    patient_phone: Optional[str] = Form(None),
    check_in: str = Form(...),
    check_out: str = Form(...),
    is_pre_reservation: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    row = session.get(LodgingReservation, res_id)
    if not row:
        raise HTTPException(status_code=404, detail="Reserva não encontrada")

    try:
        ci = date.fromisoformat(check_in)
        co = date.fromisoformat(check_out)
    except Exception:
        return redirect(f"/hospedagem?err={quote('Datas inválidas.')}&open=1")

    e = validate_lodging_period(ci, co)
    if e:
        return redirect(f"/hospedagem?err={quote(e)}&open=1&edit_id={res_id}")

    e = validate_lodging_conflict(session, unit, ci, co, exclude_id=res_id)
    if e:
        return redirect(f"/hospedagem?err={quote(e)}&open=1&edit_id={res_id}")

    row.unit = normalize_unit(unit)
    row.patient_name = patient_name.strip().upper()
    row.patient_cpf = (patient_cpf or "").strip() or None
    row.patient_phone = (patient_phone or "").strip() or None
    row.check_in = ci
    row.check_out = co
    row.is_pre_reservation = bool(is_pre_reservation)
    row.note = (note or None)
    row.updated_by_id = user.id
    row.updated_at = datetime.utcnow()

    session.add(row)
    session.commit()
    session.refresh(row)

    try:
        body = build_lodging_email_body(
            action_label="atualizar",
            requested_by_name=user.full_name or user.username,
            request_date=datetime.now(TZ),
            unit=row.unit,
            patient_name=row.patient_name,
            patient_cpf=row.patient_cpf,
            patient_phone=row.patient_phone,
            check_in=row.check_in,
            check_out=row.check_out,
        )
        send_lodging_email_notification(
            subject=f"[HOTEL] Reserva atualizada - {row.patient_name}",
            body=body,
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_EMAIL_HOSPEDAGEM_UPDATE: {e}")

    try:
        patient_key = normalize_event_key_text(getattr(row, "patient_name", None))
        send_lodging_push_event(
            session,
            event_type="update",
            row=row,
            event_key = f"lodging:update:{row.id}:{patient_key}:{row.updated_at.isoformat()}",
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_PUSH_HOSPEDAGEM_UPDATE: {e}")

    audit_event(
        request,
        user,
        action="lodging_update",
        success=True,
        message=None,
        target_type="lodging",
        target_id=row.id,
    )

    month_param = f"{ci.year:04d}-{ci.month:02d}"
    return redirect(f"/hospedagem?month={month_param}")

@app.post("/hospedagem/delete/{res_id}")
def hospedagem_delete(
    request: Request,
    res_id: int,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    row = session.get(LodgingReservation, res_id)
    if not row:
        return redirect("/hospedagem")

    month_param = f"{row.check_in.year:04d}-{row.check_in.month:02d}"

    deleted_unit = row.unit
    deleted_name = row.patient_name
    deleted_cpf = row.patient_cpf
    deleted_phone = row.patient_phone
    deleted_check_in = row.check_in
    deleted_check_out = row.check_out

    deleted_snapshot = SimpleNamespace(
        id=row.id,
        unit=row.unit,
        patient_name=row.patient_name,
        patient_cpf=row.patient_cpf,
        patient_phone=row.patient_phone,
        check_in=row.check_in,
        check_out=row.check_out,
        note=row.note,
        is_pre_reservation=row.is_pre_reservation,
    )

    session.delete(row)
    session.commit()

    try:
        body = build_lodging_email_body(
            action_label="cancelar / excluir",
            requested_by_name=user.full_name or user.username,
            request_date=datetime.now(TZ),
            unit=deleted_unit,
            patient_name=deleted_name,
            patient_cpf=deleted_cpf,
            patient_phone=deleted_phone,
            check_in=deleted_check_in,
            check_out=deleted_check_out,
        )
        send_lodging_email_notification(
            subject=f"[HOTEL] Reserva excluída - {deleted_name}",
            body=body,
            unit=deleted_unit,
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_EMAIL_HOSPEDAGEM_DELETE: {e}")
        
    try:
        deleted_patient_key = normalize_event_key_text(deleted_patient_name)
        send_lodging_push_event(
            session,
            event_type="delete",
            row=deleted_snapshot,
            event_key = f"lodging:delete:{res_id}:{deleted_patient_key}:{deleted_check_in.isoformat()}:{deleted_check_out.isoformat()}",
        )
    except Exception as e:
        audit_logger.exception(f"ERRO_PUSH_HOSPEDAGEM_DELETE: {e}")   
    
    audit_event(
        request,
        user,
        action="lodging_delete",
        success=True,
        message=None,
        target_type="lodging",
        target_id=res_id,
    )
    return redirect(f"/hospedagem?month={month_param}")

@app.get("/procedimentos", response_class=HTMLResponse)
def procedimentos_page(
    request: Request,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    db_rows = session.exec(
        select(ProcedureCatalog).order_by(ProcedureCatalog.nucleus, ProcedureCatalog.name)
    ).all()

    rows = []
    for row in db_rows:
        rows.append({
            "id": row.id,
            "name": row.name,
            "nucleus": row.nucleus,
            "allowed_nuclei_list": get_allowed_nuclei(row),
            "is_active": row.is_active,
        })

    return templates.TemplateResponse(
        "procedimentos.html",
        {
            "request": request,
            "current_user": user,
            "rows": rows,
            "nuclei_options": NUCLEI_OPTIONS,
        },
    )   

@app.post("/procedimentos/create")
def procedimentos_create(
    request: Request,
    name: str = Form(...),
    nucleus: str = Form(...),
    allowed_nuclei: list[str] = Form([]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    name = (name or "").strip()
    nucleus = (nucleus or "").strip()
    allowed_nuclei_json = build_allowed_nuclei_json(nucleus, allowed_nuclei)

    if not name or not nucleus:
        return redirect("/procedimentos")

    exists = session.exec(
        select(ProcedureCatalog).where(func.lower(ProcedureCatalog.name) == name.lower())
    ).first()

    if exists:
        return redirect("/procedimentos")

    row = ProcedureCatalog(
        name=name,
        nucleus=nucleus,
        allowed_nuclei_json=allowed_nuclei_json,
        is_active=True,
        created_by_id=user.id,
    )
    session.add(row)
    session.commit()

    audit_event(
        request,
        user,
        "procedure_catalog_created",
        target_type="procedure_catalog",
        target_id=row.id,
        extra={
            "name": row.name,
            "nucleus": row.nucleus,
            "allowed_nuclei": row.allowed_nuclei_json or [],
            "is_active": row.is_active,
        },
    )

    return redirect("/procedimentos")

@app.post("/procedimentos/update/{procedure_id}")
def procedimentos_update(
    request: Request,
    procedure_id: int,
    name: str = Form(...),
    nucleus: str = Form(...),
    allowed_nuclei: list[str] = Form([]),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    row = session.get(ProcedureCatalog, procedure_id)
    if not row:
        return redirect("/procedimentos")

    name = (name or "").strip()
    nucleus = (nucleus or "").strip()

    if not name or not nucleus:
        return redirect("/procedimentos")

    exists = session.exec(
        select(ProcedureCatalog).where(
            func.lower(ProcedureCatalog.name) == name.lower(),
            ProcedureCatalog.id != procedure_id,
        )
    ).first()

    if exists:
        return redirect("/procedimentos")

    old_name = row.name
    old_nucleus = row.nucleus
    old_allowed = row.allowed_nuclei_json

    row.name = name
    row.nucleus = nucleus
    row.allowed_nuclei_json = build_allowed_nuclei_json(nucleus, allowed_nuclei)

    session.add(row)
    session.commit()

    audit_event(
        request,
        user,
        "procedure_catalog_updated",
        target_type="procedure_catalog",
        target_id=row.id,
        extra={
            "old_name": old_name,
            "old_nucleus": old_nucleus,
            "old_allowed_nuclei": old_allowed or [],
            "new_name": row.name,
            "new_nucleus": row.nucleus,
            "new_allowed_nuclei": row.allowed_nuclei_json or [],
            "is_active": row.is_active,
        },
    )

    return redirect("/procedimentos")

@app.post("/procedimentos/toggle/{procedure_id}")
def procedimentos_toggle(
    request: Request,
    procedure_id: int,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    row = session.get(ProcedureCatalog, procedure_id)
    if not row:
        return redirect("/procedimentos")

    row.is_active = not row.is_active
    session.add(row)
    session.commit()

    audit_event(
        request,
        user,
        "procedure_catalog_toggled",
        target_type="procedure_catalog",
        target_id=row.id,
        extra={
            "name": row.name,
            "nucleus": row.nucleus,
            "is_active": row.is_active,
        },
    )

    return redirect("/procedimentos")

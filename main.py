from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

from fastapi import FastAPI, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, select, delete
from sqlalchemy import or_, text, func
from sqlalchemy.exc import IntegrityError

from db import create_db_and_tables, get_session, engine
from models import User, Room, Reservation, ReservationRequest, AuditLog, SurgicalMapEntry, AgendaBlock, AgendaBlockSurgeon, GustavoAgendaSnapshot, LodgingReservation
from auth import hash_password, verify_password, require

from pathlib import Path

import calendar
import os
import json
import logging
from logging.handlers import RotatingFileHandler

import threading
import time as pytime

TZ = timezone(timedelta(hours=-3))  # Brasil (-03:00)
SLOT_MINUTES = 30
START_HOUR = 7
END_HOUR = 19  # 19:00 (último slot começa 18:30)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="CHANGE_ME_SUPER_SECRET_KEY")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

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


def get_current_user(request: Request, session: Session) -> Optional[User]:
    uid = request.session.get("user_id")
    if not uid:
        return None
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
        ("drathamilys", "Dra. Thamilys Benfica"),
        ("drastela", "Dra. Stela Temponi"),
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
        if exclude_entry_id is not None:
            return q.where(SurgicalMapEntry.id != exclude_entry_id)
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
        return "Hospedagem indisponível: já existe reserva nesse período para esta acomodação."
    return None


def human_unit(unit: str) -> str:
    return {
        "suite_1": "Suíte 1",
        "suite_2": "Suíte 2",
        "apto": "Apartamento",
    }.get(unit, unit)

def _weekday_pt(idx: int) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[idx]

# ============================
# RELATÓRIO DR. GUSTAVO (snapshot diário às 19h)
# ============================

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

def build_gustavo_whatsapp_messages(session: Session, snapshot_day_sp: date) -> tuple[str, str, dict]:
    """
    Gera as duas mensagens (Panorama + Detalhe 3 meses)

    Regras:
    - meses fechados: mês atual + 2
    - Seg/Qua sempre aparecem
    - Sex só aparece se houver agendamento
    - Emojis: ✅ cheio | 🟡 parcial | 🔴 livre | 🔵 bloqueio/recesso
    - Descrição (linha 2) só aparece quando há Ref ou Proc. simples
      (se for só Cirurgia, não mostra detalhamento)
    """

    gustavo = session.exec(select(User).where(User.username == "drgustavo")).first()
    if not gustavo:
        raise RuntimeError("Usuário drgustavo não encontrado no banco.")

    y0, m0 = snapshot_day_sp.year, snapshot_day_sp.month
    months = [(y0, m0), _add_months(y0, m0, 1), _add_months(y0, m0, 2)]

    period_start = _month_start(months[0][0], months[0][1])
    period_end = _month_end(months[-1][0], months[-1][1])

    # carrega tudo do período (performance)
    entries = session.exec(
        select(SurgicalMapEntry).where(
            SurgicalMapEntry.surgeon_id == gustavo.id,
            SurgicalMapEntry.day >= period_start,
            SurgicalMapEntry.day <= period_end,
        )
    ).all()

    by_day: dict[date, list[SurgicalMapEntry]] = {}
    for e in entries:
        by_day.setdefault(e.day, []).append(e)

    pano_lines: list[str] = [
        "AGENDA DR. GUSTAVO AQUINO",
        f"📅 {PT_MONTHS[months[0][1]-1].title()} • {PT_MONTHS[months[1][1]-1].title()} • {PT_MONTHS[months[2][1]-1].title()}",
        ""
    ]

    detail_parts: list[str] = []
    months_payload = []

    for (yy, mm) in months:
        m_start = _month_start(yy, mm)
        m_end = _month_end(yy, mm)

        lines: list[str] = []
        prev_day: date | None = None
        counts = {"✅": 0, "🟡": 0, "🔴": 0, "🔵": 0}

        d = m_start
        while d <= m_end:
            dow = d.weekday()  # 0=Mon
            is_mon_wed = dow in (0, 2)
            is_fri = dow == 4

            day_entries = by_day.get(d, [])

            # sexta só aparece se tiver agendamento
            if not is_mon_wed and not (is_fri and day_entries):
                d += timedelta(days=1)
                continue

            # bloqueio? (seg/qua sempre consideram)
            block_reason = validate_mapa_block_rules(session, d, gustavo.id)
            if block_reason:
                emoji = "🔵"
            else:
                total = len(day_entries)  # conta reservas também (se está reservado, não dá pra vender)
                if is_fri:
                    emoji = "🟡" if total >= 1 else "🔴"
                else:
                    if total >= 2:
                        emoji = "✅"
                    elif total == 1:
                        emoji = "🟡"
                    else:
                        emoji = "🔴"

            counts[emoji] += 1

            # quebra visual entre semanas
            if prev_day is not None and (d - prev_day).days > 3:
                lines.append("")

            lines.append(f"{DOW_ABBR[dow]} {d.strftime('%d/%m')}  {emoji}")

            # descrição só se houver refino ou procedimento simples (para ficar limpo)
            if emoji != "🔵" and day_entries:
                cir = ref = simp = 0
                for e in day_entries:
                    b = _proc_bucket(e.procedure_type)
                    if b == "ref":
                        ref += 1
                    elif b == "simp":
                        simp += 1
                    else:
                        cir += 1

                if ref > 0 or simp > 0:
                    parts = []
                    if cir:
                        parts.append(f"{cir} Cir")
                    if ref:
                        parts.append(f"{ref} Ref")
                    if simp:
                        parts.append(f"{simp} Simp")
                    lines.append(f"({ ' + '.join(parts) })")

            prev_day = d
            d += timedelta(days=1)

        # Panorama do mês
        pano_lines.append(_month_label_pt(yy, mm))
        pano_lines.append(f"✅ {counts['✅']}")
        pano_lines.append(f"🟡 {counts['🟡']}")
        pano_lines.append(f"🔴 {counts['🔴']}")
        pano_lines.append(f"🔵 {counts['🔵']}")
        pano_lines.append("")

        # Detalhe do mês
        detail_parts.append(_month_label_pt(yy, mm))
        detail_parts.append("")
        detail_parts.extend(lines)
        detail_parts.append("")

        months_payload.append({
            "year": yy,
            "month": mm,
            "label": _month_label_pt(yy, mm),
            "counts": counts,
            "lines": lines,
        })

    message_1 = "\n".join(pano_lines).strip()
    message_2 = "\n".join(detail_parts).strip()

    payload = {
        "doctor_username": "drgustavo",
        "snapshot_day_sp": snapshot_day_sp.isoformat(),
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "months": months_payload,
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

    msg1, msg2, payload = build_gustavo_whatsapp_messages(session, snapshot_day_sp)

    y0, m0 = snapshot_day_sp.year, snapshot_day_sp.month
    y2, m2 = _add_months(y0, m0, 2)

    snap = GustavoAgendaSnapshot(
        snapshot_date=snapshot_day_sp,
        generated_at=datetime.utcnow(),
        period_start=_month_start(y0, m0),
        period_end=_month_end(y2, m2),
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
        )
    ).all():
        counts[d] = counts.get(d, 0) + 1

    zeros = [d for d in days if counts.get(d, 0) == 0]
    if zeros:
        return {"mode": "red", "items": [f"🔴 {d.strftime('%d/%m/%Y')}" for d in zeros]}

    ones = [d for d in days if counts.get(d, 0) == 1]
    if ones:
        return {
            "mode": "yellow",
            "items": [f"🟡 {_weekday_pt(d.weekday())} {d.strftime('%d/%m/%Y')}" for d in ones],
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
        # Se a tabela ainda não existir, create_db_and_tables() vai criar.
        # Aqui só migramos se ela existir.
        tables = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agendablock';"
        ).fetchall()
        if not tables:
            return

        # --- Novas colunas do modelo atual ---
        _add_column_if_missing(conn, "agendablock", "start_date", "DATE")
        _add_column_if_missing(conn, "agendablock", "end_date", "DATE")
        _add_column_if_missing(conn, "agendablock", "reason", "TEXT")
        _add_column_if_missing(conn, "agendablock", "applies_to_all", "INTEGER DEFAULT 0")

        # --- Backfill a partir do schema antigo, se existir ---
        # Antigo: data, motivo, profissional
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
            # Se profissional='todos' no schema antigo, vira applies_to_all=1
            conn.exec_driver_sql("""
                UPDATE agendablock
                   SET applies_to_all = CASE
                        WHEN applies_to_all IS NULL THEN
                            CASE WHEN lower(profissional)='todos' THEN 1 ELSE 0 END
                        ELSE applies_to_all
                       END;
            """)

        # --- Criar tabela de relacionamento (multi-cirurgião) ---
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS agendablocksurgeon (
                block_id INTEGER NOT NULL,
                surgeon_id INTEGER NOT NULL,
                PRIMARY KEY (block_id, surgeon_id)
            );
        """)

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


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(
        "login.html", {"request": request, "current_user": None}
    )


@app.post("/login", response_class=HTMLResponse)
def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
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
            {"request": request, "error": "Usuário ou senha inválidos.", "current_user": None},
            status_code=401,
        )
    request.session["user_id"] = user.id
    audit_event(request, user, "login_success")
    return redirect("/")


@app.post("/logout")
def logout(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    audit_event(request, user, "logout")
    request.session.clear()
    return redirect("/login")


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
    av_do: Optional[str] = None,
    av_surgeon_id: Optional[int] = None,
    av_month: Optional[str] = None,
    av_procedure_type: Optional[str] = None,
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
    
    users_all = session.exec(select(User)).all()
    users_by_id = {u.id: u for u in users_all if u.id is not None}

    entries = session.exec(
        select(SurgicalMapEntry)
        .where(SurgicalMapEntry.day >= first_day, SurgicalMapEntry.day < next_first)
        .order_by(SurgicalMapEntry.day, SurgicalMapEntry.time_hhmm, SurgicalMapEntry.created_at)
    ).all()

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

    weekday_map = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]

    # =========================
    # Consulta de Disponibilidade (card)
    # =========================
    av_results: list[dict[str, str]] = []
    av_selected_month = av_month or selected_month
    av_selected_surgeon_id = av_surgeon_id
    av_selected_procedure_type = av_procedure_type or "Cirurgia"

    if av_do == "1" and av_selected_surgeon_id:
        av_results = compute_month_availability(
            session=session,
            surgeon_id=int(av_selected_surgeon_id),
            month_ym=av_selected_month,
            procedure_type=av_selected_procedure_type,
        )
    
    return templates.TemplateResponse(
        "mapa.html",
        {
            "request": request,
            "current_user": user,
            "fmt_brasilia": fmt_brasilia,
            "err": err,
            "title": "Mapa Cirúrgico",
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
            "blocked_all_days": blocked_all_days,  # set[str] -> "2026-01-15"
            "blocked_surgeons_by_day": blocked_surgeons_by_day,  # dict[str, list[int]]
            "av_selected_month": av_selected_month,
            "av_selected_surgeon_id": av_selected_surgeon_id,
            "av_selected_procedure_type": av_selected_procedure_type,
            "av_results": av_results,
        },
    )


@app.post("/mapa/create")
def mapa_create(
    request: Request,
    day_iso: str = Form(...),
    mode: str = Form("book"),
    time_hhmm: Optional[str] = Form(None),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    seller_id: Optional[int] = Form(None),
    force_override: Optional[str] = Form(None),
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
        from urllib.parse import quote
        audit_event(request, user, "surgical_map_blocked_by_agenda_block", success=False, message=block_err)

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
            f"&seller_id={seller_id_final}"
        )

    # se passou com override, registra auditoria
    if block_err and override:
        audit_event(request, user, "surgical_map_override_agenda_block", success=True, message=block_err)

    err = validate_mapa_rules(session, day, surgeon_id, procedure_type, uses_hsr=bool(uses_hsr))
    if err and not override:
        month = day.strftime("%Y-%m")
        audit_event(
            request,
            user,
            "surgical_map_create_validation_error",
            success=False,
            message=err,
            extra={
                "day": day_iso,
                "time_hhmm": time_hhmm,
                "patient_name": patient_name,
                "surgeon_id": surgeon_id,
                "procedure_type": procedure_type,
                "location": location,
                "uses_hsr": bool(uses_hsr),
                "mode": mode,
            },
        )
        from urllib.parse import quote
        return redirect(
            f"/mapa?month={month}&open=1"
            f"&err={quote(err)}"
            f"&day_iso={quote(day_iso)}"
            f"&mode={quote(mode)}"
            f"&time_hhmm={quote(time_hhmm or '')}"
            f"&patient_name={quote(patient_name)}"
            f"&surgeon_id={surgeon_id}"
            f"&procedure_type={quote(procedure_type)}"
            f"&location={quote(location)}"
            f"&uses_hsr={1 if uses_hsr else 0}"
            f"&seller_id={seller_id_final}"
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

    audit_event(
        request,
        user,
        "surgical_map_created",
        target_type="surgical_map",
        target_id=row.id,
        extra={
            "day": day_iso,
            "patient_name": patient_name,
            "surgeon_id": surgeon_id,
            "procedure_type": procedure_type,
            "location": location,
            "uses_hsr": bool(uses_hsr),
        },
    )

    month = day.strftime("%Y-%m")
    return redirect(f"/mapa?month={month}")

@app.post("/mapa/update/{entry_id}")
def mapa_update(
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
    seller_id: Optional[int] = Form(None),
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
    err = validate_mapa_rules(
        session,
        day,
        surgeon_id,
        procedure_type,
        uses_hsr=bool(uses_hsr),
        exclude_entry_id=entry_id,
    )
    if err:
        month = day.strftime("%Y-%m")
        from urllib.parse import quote
        return redirect(
            f"/mapa?month={month}&open=1&edit_id={entry_id}"
            f"&err={quote(err)}"
            f"&day_iso={quote(day_iso)}"
            f"&mode={quote(mode)}"
            f"&time_hhmm={quote(time_hhmm or '')}"
            f"&patient_name={quote(patient_name)}"
            f"&surgeon_id={surgeon_id}"
            f"&procedure_type={quote(procedure_type)}"
            f"&location={quote(location)}"
            f"&uses_hsr={1 if uses_hsr else 0}"
        )

    # snapshot (opcional) pra auditoria
    before = {
        "day": row.day.isoformat(),
        "time_hhmm": row.time_hhmm,
        "patient_name": row.patient_name,
        "surgeon_id": row.surgeon_id,
        "procedure_type": row.procedure_type,
        "location": row.location,
        "uses_hsr": row.uses_hsr,
        "is_pre_reservation": row.is_pre_reservation,
    }

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

    audit_event(
        request,
        user,
        "surgical_map_updated",
        target_type="surgical_map",
        target_id=row.id,
        extra={
            "before": before,
            "after": {
                "day": row.day.isoformat(),
                "time_hhmm": row.time_hhmm,
                "patient_name": row.patient_name,
                "surgeon_id": row.surgeon_id,
                "procedure_type": row.procedure_type,
                "location": row.location,
                "uses_hsr": row.uses_hsr,
                "is_pre_reservation": row.is_pre_reservation,
            },
        },
    )

    month = day.strftime("%Y-%m")
    return redirect(f"/mapa?month={month}")

    if err and override:
        audit_event(request, user, "surgical_map_override_rule", success=True, message=err)

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
        session.delete(row)
        session.commit()

        audit_event(
            request,
            user,
            "surgical_map_deleted",
            target_type="surgical_map",
            target_id=entry_id,
            extra={
                "day": row.day.isoformat(),
                "time_hhmm": row.time_hhmm,
                "patient_name": row.patient_name,
                "surgeon_id": row.surgeon_id,
                "procedure_type": row.procedure_type,
                "location": row.location,
                "uses_hsr": row.uses_hsr,
                "is_pre_reservation": getattr(row, "is_pre_reservation", None),
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

    return templates.TemplateResponse(
        "relatorio_gustavo.html",
        {
            "request": request,
            "current_user": user,
            "available_dates": available_dates,
            "snapshot": selected,
            "snapshot_date": snapshot_date or "",
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
    edit_id: Optional[str] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    selected_month = safe_selected_month(month)

    y, m = map(int, selected_month.split("-"))
    first_day = date(y, m, 1)
    _, last_day = calendar.monthrange(y, m)
    next_month_first = (date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1))

    days = [date(y, m, d) for d in range(1, last_day + 1)]
    day_index = {d: i for i, d in enumerate(days)}

    units = ["suite_1", "suite_2", "apto"]

    # busca reservas que encostam no mês (por período)
    q = select(LodgingReservation).where(
        LodgingReservation.check_in < next_month_first,
        LodgingReservation.check_out > first_day,
    )
    reservations = session.exec(q).all()

    # barras por unidade (grid com colunas = dias)
    bars_by_unit: dict[str, list[dict]] = {u: [] for u in units}

    for r in reservations:
        u = r.unit or ""
        if u not in bars_by_unit:
            continue

        # clamp dentro do mês visível
        start = max(r.check_in, first_day)
        end = min(r.check_out, next_month_first)

        if start >= end:
            continue

        start_col = day_index[start] + 1
        end_col = day_index[end - timedelta(days=1)] + 2  # fim exclusivo

        bars_by_unit[u].append(
            {
                "id": r.id,
                "patient_name": r.patient_name,
                "check_in": r.check_in.strftime("%d/%m/%Y"),
                "check_out": r.check_out.strftime("%d/%m/%Y"),
                "start_col": start_col,
                "end_col": end_col,
                "is_pre": 1 if r.is_pre_reservation else 0,
            }
        )

    # ordena barras na linha
    for u in bars_by_unit:
        bars_by_unit[u].sort(key=lambda b: (b["start_col"], b["end_col"]))

    return templates.TemplateResponse(
        "hospedagem.html",
        {
            "request": request,
            "current_user": user,
            "selected_month": selected_month,
            "days": days,
            "units": units,
            "bars_by_unit": bars_by_unit,
            "human_unit": human_unit,
            "err": err or "",
            "open": open or "",
            "unit_prefill": unit or "",
            "check_in_prefill": check_in or "",
            "check_out_prefill": check_out or "",
            "patient_prefill": patient_name or "",
            "pre_prefill": 1 if (is_pre_reservation == "1") else 0,
            "edit_id": edit_id or "",
        },
    )


@app.post("/hospedagem/create")
def hospedagem_create(
    request: Request,
    unit: str = Form(...),
    patient_name: str = Form(...),
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

    selected_month = safe_selected_month(None)

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
        return redirect(
            f"/hospedagem?err={quote(e)}&open=1"
            f"&unit={quote(unit)}&check_in={quote(check_in)}&check_out={quote(check_out)}"
            f"&patient_name={quote(patient_name)}&is_pre_reservation={(1 if is_pre_reservation else 0)}"
        )

    row = LodgingReservation(
        unit=unit,
        patient_name=patient_name.strip().upper(),
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

    audit_event(
        request,
        user,
        action="lodging_create",
        success=True,
        message=None,
        target_type="lodging",
        target_id=row.id,
    )

    month_param = f"{ci.year:04d}-{ci.month:02d}"
    return redirect(f"/hospedagem?month={month_param}")


@app.post("/hospedagem/update/{res_id}")
def hospedagem_update(
    request: Request,
    res_id: int,
    unit: str = Form(...),
    patient_name: str = Form(...),
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

    row.unit = unit
    row.patient_name = patient_name.strip().upper()
    row.check_in = ci
    row.check_out = co
    row.is_pre_reservation = bool(is_pre_reservation)
    row.note = (note or None)
    row.updated_by_id = user.id
    row.updated_at = datetime.utcnow()

    session.add(row)
    session.commit()

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

    session.delete(row)
    session.commit()

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

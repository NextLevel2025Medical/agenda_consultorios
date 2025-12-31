from __future__ import annotations

from datetime import datetime, date, time, timedelta, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, select
from sqlalchemy import or_

from db import create_db_and_tables, get_session, engine
from models import User, Room, Reservation, ReservationRequest, AuditLog, SurgicalMapEntry, AgendaBlock
from auth import hash_password, verify_password, require

from pathlib import Path

import calendar
import os
import json
import logging
from logging.handlers import RotatingFileHandler

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
    exclude_entry_id: int | None = None,  # usado na edição pra não contar o próprio registro
) -> str | None:
    """
    Regras do Mapa Cirúrgico

    1) Dr. Gustavo Aquino: máximo 2 agendamentos no mesmo dia (independente de tipo).
    2) Não pode existir CIRURGIA para Dra. Alice e Dr. Ricardo Vilela no mesmo dia.
    """

    gustavo = session.exec(select(User).where(User.full_name == "Dr. Gustavo Aquino")).first()
    alice = session.exec(select(User).where(User.full_name == "Dra. Alice Osório")).first()
    ricardo = session.exec(select(User).where(User.full_name == "Dr. Ricardo Vilela")).first()

    # Helper: aplica "excluir este registro" quando estamos editando
    def _apply_exclude(q):
        if exclude_entry_id is not None:
            return q.where(SurgicalMapEntry.id != exclude_entry_id)
        return q

    # (1) Gustavo: conta TODOS os agendamentos no dia (cirurgia/refino/simples, pré-reserva etc.)
    if gustavo and surgeon_id == gustavo.id:
        q = select(SurgicalMapEntry.id).where(
            SurgicalMapEntry.day == day,
            SurgicalMapEntry.surgeon_id == gustavo.id,
        )
        q = _apply_exclude(q)
        already = session.exec(q).all()

        if len(already) >= 2:
            return (
                "Regra: Dr. Gustavo Aquino não pode ter mais de 2 agendamentos no mesmo dia "
                "(independente se é cirurgia, refinamento ou procedimento simples)."
            )

    # (2) Alice x Ricardo: a restrição é somente para CIRURGIA
    if procedure_type == "Cirurgia" and alice and ricardo:
        if surgeon_id == alice.id:
            q = select(SurgicalMapEntry.id).where(
                SurgicalMapEntry.day == day,
                SurgicalMapEntry.surgeon_id == ricardo.id,
                SurgicalMapEntry.procedure_type == "Cirurgia",
            )
            q = _apply_exclude(q)
            if session.exec(q).first():
                return "Regra: Não pode haver CIRURGIA para Dra. Alice e Dr. Ricardo Vilela no mesmo dia."

        if surgeon_id == ricardo.id:
            q = select(SurgicalMapEntry.id).where(
                SurgicalMapEntry.day == day,
                SurgicalMapEntry.surgeon_id == alice.id,
                SurgicalMapEntry.procedure_type == "Cirurgia",
            )
            q = _apply_exclude(q)
            if session.exec(q).first():
                return "Regra: Não pode haver CIRURGIA para Dra. Alice e Dr. Ricardo Vilela no mesmo dia."

    return None

def _weekday_pt(idx: int) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[idx]

def validate_mapa_block_rules(session: Session, day: date, surgeon_id: int) -> str | None:
    block = session.exec(
        select(AgendaBlock).where(
            AgendaBlock.day == day,
            or_(AgendaBlock.applies_to_all == True, AgendaBlock.surgeon_id == surgeon_id),
        )
    ).first()

    if not block:
        return None

    if block.applies_to_all:
        who = "Todos"
    else:
        u = session.get(User, block.surgeon_id) if block.surgeon_id else None
        who = u.full_name if u else "Profissional"

    return f"Agenda bloqueada: {day.strftime('%d/%m/%Y')} - {block.reason} - {who}"

def compute_priority_card(session: Session) -> dict:
    today = datetime.now(TZ).date()
    end = today + timedelta(days=90)  # janela “hoje até +90”

    gustavo = session.exec(select(User).where(User.full_name == "Dr. Gustavo Aquino")).first()
    if not gustavo:
        return {"mode": "red", "items": []}

    blocked_days = set(
        session.exec(
            select(AgendaBlock.day).where(
                AgendaBlock.day >= today,
                AgendaBlock.day <= end,
                or_(AgendaBlock.applies_to_all == True, AgendaBlock.surgeon_id == gustavo.id),
            )
        ).all()
    )

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

@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    with Session(engine) as session:
        seed_if_empty(session)


@app.get("/", response_class=HTMLResponse)
def home(request: Request, session: Session = Depends(get_session)):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")

    if user.role == "admin":
        return redirect("/admin")
    if user.role == "doctor":
        return redirect("/doctor")
    if user.role == "surgery":   # NOVO
        return redirect("/mapa")

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
    edit: Optional[int] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    blocks = session.exec(select(AgendaBlock).order_by(AgendaBlock.day.desc())).all()
    surgeons = session.exec(select(User).where(User.role == "surgery")).all()

    edit_block = None
    if edit:
        edit_block = session.get(AgendaBlock, edit)

    return templates.TemplateResponse(
        "bloqueios.html",
        {
            "request": request,
            "current_user": user,
            "blocks": blocks,
            "surgeons": surgeons,
            "edit_block": edit_block,  # <<< ESSENCIAL
        },
    )

@app.post("/bloqueios")
def registrar_bloqueio(
    request: Request,
    data: str = Form(...),
    motivo: str = Form(...),
    profissional: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    # converte "YYYY-MM-DD" para date
    day = date.fromisoformat(data)

    applies_to_all = (profissional == "todos")
    surgeon_id = None if applies_to_all else int(profissional)

    block = AgendaBlock(
        day=day,
        reason=motivo.strip(),
        applies_to_all=applies_to_all,
        surgeon_id=surgeon_id,
        created_by_id=user.id,
    )

    session.add(block)
    session.commit()

    print(f"🔴 Bloqueio cadastrado (DB): {data} | {motivo} | {profissional}")

    # volta pra página de bloqueios (UX correta)
    return RedirectResponse(url="/bloqueios", status_code=303)

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

@app.get("/bloqueios", response_class=HTMLResponse)
def bloqueios_page(
    request: Request,
    edit: Optional[int] = None,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    surgeons = session.exec(select(User).where(User.role == "surgery")).all()
    blocks = session.exec(select(AgendaBlock).order_by(AgendaBlock.day.asc())).all()

    edit_block = None
    if edit:
        edit_block = session.get(AgendaBlock, edit)

    return templates.TemplateResponse(
        "bloqueios.html",
        {
            "request": request,
            "current_user": user,
            "surgeons": surgeons,
            "blocks": blocks,
            "edit_block": edit_block,
        },
    )

@app.post("/bloqueios/{block_id}/update")
def atualizar_bloqueio(
    request: Request,
    block_id: int,
    data: str = Form(...),
    motivo: str = Form(...),
    profissional: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    block = session.get(AgendaBlock, block_id)
    if not block:
        return RedirectResponse("/bloqueios", status_code=303)

    block.day = date.fromisoformat(data)
    block.reason = (motivo or "").strip()
    block.applies_to_all = (profissional == "todos")
    block.surgeon_id = None if block.applies_to_all else int(profissional)

    session.add(block)
    session.commit()

    return RedirectResponse("/bloqueios", status_code=303)

@app.post("/bloqueios/{block_id}/delete")
def excluir_bloqueio(
    request: Request,
    block_id: int,
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    block = session.get(AgendaBlock, block_id)
    if block:
        session.delete(block)
        session.commit()

    return RedirectResponse("/bloqueios", status_code=303)

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

    blocks = session.exec(
        select(AgendaBlock)
        .where(AgendaBlock.day >= first_day, AgendaBlock.day < next_first)
        .order_by(AgendaBlock.day, AgendaBlock.created_at)
    ).all()

    blocks_by_day: dict[str, list[AgendaBlock]] = {}
    blocked_all_days: set[str] = set()
    blocked_surgeons_by_day: dict[str, list[int]] = {}

    for b in blocks:
        k = b.day.isoformat()
        blocks_by_day.setdefault(k, []).append(b)
        if b.applies_to_all:
            blocked_all_days.add(k)
        elif b.surgeon_id is not None:
            blocked_surgeons_by_day.setdefault(k, []).append(b.surgeon_id)

    priority = compute_priority_card(session)

    weekday_map = ["segunda-feira","terça-feira","quarta-feira","quinta-feira","sexta-feira","sábado","domingo"]

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
            "blocked_all_days": blocked_all_days,
            "blocked_surgeons_by_day": blocked_surgeons_by_day,
            "priority_mode": priority["mode"],
            "priority_items": priority["items"],

        },
    )


@app.post("/mapa/create")
def mapa_create(
    request: Request,
    day_iso: str = Form(...),
    mode: str = Form("book"),
    time_hhmm: str = Form(...),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    day = date.fromisoformat(day_iso)
    
    is_pre = (mode == "reserve")

    block_err = validate_mapa_block_rules(session, day, surgeon_id)
    if block_err:
        month = day.strftime("%Y-%m")
        from urllib.parse import quote
        audit_event(request, user, "surgical_map_blocked_by_agenda_block", success=False, message=block_err)
        return redirect(f"/mapa?month={month}&err={quote(block_err)}")

    err = validate_mapa_rules(session, day, surgeon_id, procedure_type)
    if err:
        month = day.strftime("%Y-%m")
        audit_event(
            request,
            user,
            "surgical_map_blocked",
            success=False,
            message=err,
            target_type="surgical_map",
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
        return redirect(f"/mapa?month={month}&err={quote(err)}")
    
    row = SurgicalMapEntry(
        day=day,
        time_hhmm=time_hhmm,
        patient_name=patient_name.strip(),
        surgeon_id=surgeon_id,
        procedure_type=procedure_type,
        location=location,
        uses_hsr=bool(uses_hsr),
        is_pre_reservation=is_pre,
        created_by_id=user.id,
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
    time_hhmm: str = Form(...),
    patient_name: str = Form(...),
    surgeon_id: int = Form(...),
    procedure_type: str = Form(...),
    location: str = Form(...),
    uses_hsr: Optional[str] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    row = session.get(SurgicalMapEntry, entry_id)
    if not row:
        return redirect("/mapa")

    day = date.fromisoformat(day_iso)
    is_pre = (mode == "reserve")

    # valida regras EXCLUINDO o próprio item (pra não bloquear edição à toa)
    err = validate_mapa_rules(session, day, surgeon_id, procedure_type, exclude_entry_id=entry_id)
    if err:
        month = day.strftime("%Y-%m")
        from urllib.parse import quote
        return redirect(f"/mapa?month={month}&err={quote(err)}")

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

    # aplica alterações
    row.day = day
    row.time_hhmm = time_hhmm
    row.patient_name = patient_name.strip()
    row.surgeon_id = surgeon_id
    row.procedure_type = procedure_type
    row.location = location
    row.uses_hsr = bool(uses_hsr)
    row.is_pre_reservation = is_pre

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

@app.post("/mapa/block/create")
def mapa_block_create(
    request: Request,
    month: str = Form(...),              # YYYY-MM pra voltar pro mês certo
    day_iso: str = Form(...),            # 2025-12-25
    reason: str = Form(...),
    scope: str = Form("all"),            # "all" | "surgeon"
    surgeon_id: Optional[int] = Form(None),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    day = date.fromisoformat(day_iso)
    applies_all = (scope == "all")
    sid = None if applies_all else surgeon_id

    if not applies_all and not sid:
        from urllib.parse import quote
        return redirect(f"/mapa?month={month}&err={quote('Selecione o profissional do bloqueio.')}")

    # evita duplicidade exata (mesma data + mesmo escopo)
    exists = session.exec(
        select(AgendaBlock).where(
            AgendaBlock.day == day,
            AgendaBlock.applies_to_all == applies_all,
            AgendaBlock.surgeon_id == sid,
        )
    ).first()
    if exists:
        from urllib.parse import quote
        return redirect(f"/mapa?month={month}&err={quote('Já existe um bloqueio igual para essa data.')}")

    row = AgendaBlock(
        day=day,
        reason=reason.strip(),
        applies_to_all=applies_all,
        surgeon_id=sid,
        created_by_id=user.id,
    )
    session.add(row)
    session.commit()

    audit_event(request, user, "agenda_block_created", target_type="agenda_block", target_id=row.id,
                extra={"day": day_iso, "reason": reason, "scope": scope, "surgeon_id": sid})

    return redirect(f"/mapa?month={month}")

@app.post("/mapa/block/delete/{block_id}")
def mapa_block_delete(
    request: Request,
    block_id: int,
    month: str = Form(...),
    session: Session = Depends(get_session),
):
    user = get_current_user(request, session)
    if not user:
        return redirect("/login")
    require(user.role in ("admin", "surgery"))

    row = session.get(AgendaBlock, block_id)
    if row:
        session.delete(row)
        session.commit()
        audit_event(request, user, "agenda_block_deleted", target_type="agenda_block", target_id=block_id,
                    extra={"day": row.day.isoformat(), "reason": row.reason, "applies_to_all": row.applies_to_all, "surgeon_id": row.surgeon_id})

    return RedirectResponse("/bloqueios", status_code=303)


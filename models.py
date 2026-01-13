from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from sqlmodel import SQLModel, Field, UniqueConstraint
from sqlalchemy import Column
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON

class User(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("username"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str
    full_name: str
    password_hash: str
    role: str  # "admin" | "doctor" | "surgery"
    is_active: bool = True
    
    
class SurgicalMapEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    day: date = Field(index=True)  # 2025-12-01

    # ✅ NOVO: horário (HH:MM) - string é o mais simples e ordena bem
    time_hhmm: Optional[str] = Field(default=None, index=True)

    patient_name: str
    surgeon_id: int = Field(foreign_key="user.id", index=True)
    procedure_type: str
    location: str
    uses_hsr: bool = False

    is_pre_reservation: bool = Field(default=False, index=True)

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    
class AgendaBlock(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    # compatibilidade com DB antigo (coluna NOT NULL no SQLite atual)
    day: date = Field(index=True)

    start_date: date = Field(index=True)
    end_date: date = Field(index=True)

    reason: str
    applies_to_all: bool = Field(default=False, index=True)
    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class AgendaBlockSurgeon(SQLModel, table=True):
    block_id: int = Field(foreign_key="agendablock.id", primary_key=True)
    surgeon_id: int = Field(foreign_key="user.id", primary_key=True)

class GustavoAgendaSnapshot(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("snapshot_date"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    # Data do fechamento (referente ao horário de SP às 19h)
    snapshot_date: date = Field(index=True)

    # Quando o snapshot foi gerado (UTC)
    generated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    period_start: date
    period_end: date

    # Mensagens prontas (WhatsApp/site)
    message_1: str
    message_2: str

    # Estrutura opcional para renderização no site (JSON)
    payload: Optional[dict] = Field(default=None, sa_column=Column(SQLiteJSON))

class Room(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("name"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: Optional[str] = None


class Reservation(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("room_id", "start_time"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    room_id: int = Field(foreign_key="room.id")
    doctor_id: int = Field(foreign_key="user.id")
    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id")

    start_time: datetime
    end_time: datetime
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ReservationRequest(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("room_id", "requested_start"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    room_id: int = Field(foreign_key="room.id")
    doctor_id: int = Field(foreign_key="user.id")

    requested_start: datetime
    requested_end: datetime

    status: str = "pending"  # pending | approved | denied | cancelled
    message: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    decided_by_id: Optional[int] = Field(default=None, foreign_key="user.id")
    decided_at: Optional[datetime] = None


class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    actor_user_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    actor_username: Optional[str] = Field(default=None, index=True)
    actor_role: Optional[str] = Field(default=None, index=True)

    action: str = Field(index=True)  # ex: "login_success", "request_created"
    success: bool = True
    message: Optional[str] = None

    room_id: Optional[int] = Field(default=None, foreign_key="room.id", index=True)
    target_type: Optional[str] = Field(default=None, index=True)  # "reservation" | "request"
    target_id: Optional[int] = Field(default=None, index=True)

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    ip: Optional[str] = None
    user_agent: Optional[str] = None
    path: Optional[str] = None
    method: Optional[str] = None

    extra_json: Optional[str] = None  # json.dumps(extra)

class LodgingReservation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    # suite_1 | suite_2 | apto
    unit: str = Field(index=True)

    # pré-reserva bloqueia igual, mas fica marcado
    is_pre_reservation: bool = Field(default=False, index=True)

    patient_name: str = Field(index=True)
    check_in: date = Field(index=True)
    check_out: date = Field(index=True)  # NÃO inclusivo (data de saída)

    note: Optional[str] = None

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    # opcional: vincular à cirurgia (SurgicalMapEntry)
    surgery_entry_id: Optional[int] = Field(default=None, foreign_key="surgicalmapentry.id", index=True)


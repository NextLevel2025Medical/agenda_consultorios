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
    
    status: str = Field(default="approved", index=True)  # "approved" | "pending"

    decide_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    decided_at: Optional[datetime] = Field(default=None, index=True)

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class ProcedureCatalog(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("name"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    nucleus: str = Field(index=True)  # núcleo principal
    allowed_nuclei_json: Optional[dict] = Field(default=None, sa_column=Column(SQLiteJSON))
    is_active: bool = Field(default=True, index=True)

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class SurgeryProcedureItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    surgery_entry_id: int = Field(foreign_key="surgicalmapentry.id", index=True)
    procedure_id: int = Field(foreign_key="procedurecatalog.id", index=True)

    procedure_name_snapshot: str
    nucleus_snapshot: str = Field(index=True)

    amount: float = Field(default=0)

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
    decide_by_id: Optional[int] = Field(default=None, foreign_key="user.id")
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
    patient_cpf: Optional[str] = Field(default=None, index=True)
    patient_phone: Optional[str] = Field(default=None)

    check_in: date = Field(index=True)
    check_out: date = Field(index=True)  # NÃO inclusivo (data de saída)

    note: Optional[str] = None

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    # opcional: vincular à cirurgia (SurgicalMapEntry)
    surgery_entry_id: Optional[int] = Field(default=None, foreign_key="surgicalmapentry.id", index=True)

class PushSubscription(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("endpoint"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    user_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)

    endpoint: str = Field(index=True)
    p256dh: str
    auth: str

    is_active: bool = Field(default=True, index=True)
    user_agent: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class PushNotificationLog(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("event_key"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    event_key: str = Field(index=True)
    event_type: str = Field(index=True)

    reservation_id: Optional[int] = Field(
        default=None,
        foreign_key="lodgingreservation.id",
        index=True,
    )

    scheduled_for: Optional[date] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    
class FeegowProfessionalMap(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("surgeon_user_id"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    surgeon_user_id: int = Field(foreign_key="user.id", index=True)
    feegow_professional_id: int = Field(index=True)

    surgeon_name_snapshot: Optional[str] = None
    feegow_professional_name: Optional[str] = None

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class FeegowValidationRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    period_start: date = Field(index=True)
    period_end: date = Field(index=True)

    status: str = Field(default="completed", index=True)  # completed | failed

    total_entries: int = Field(default=0)
    total_ok: int = Field(default=0)
    total_alert: int = Field(default=0)
    total_unmapped: int = Field(default=0)
    total_api_error: int = Field(default=0)

    created_by_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

    notes_json: Optional[dict] = Field(default=None, sa_column=Column(SQLiteJSON))


class FeegowValidationResult(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    run_id: int = Field(foreign_key="feegowvalidationrun.id", index=True)
    surgical_entry_id: Optional[int] = Field(default=None, foreign_key="surgicalmapentry.id", index=True)

    map_day: date = Field(index=True)
    map_patient_name: str = Field(index=True)
    map_surgeon_id: Optional[int] = Field(default=None, foreign_key="user.id", index=True)
    map_surgeon_name: Optional[str] = Field(default=None, index=True)

    validation_status: str = Field(index=True)  # ok | alert | surgeon_not_mapped | api_error
    detail_message: Optional[str] = None

    matched_feegow_professional_id: Optional[int] = Field(default=None, index=True)
    matched_feegow_agendamento_id: Optional[int] = Field(default=None, index=True)
    matched_feegow_patient_id: Optional[int] = Field(default=None, index=True)
    matched_feegow_patient_name: Optional[str] = None
    matched_feegow_date: Optional[str] = None

    raw_match_json: Optional[dict] = Field(default=None, sa_column=Column(SQLiteJSON))

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)

class FeegowValidationAcknowledgement(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("validation_result_id", "ack_user_id"),)

    id: Optional[int] = Field(default=None, primary_key=True)

    validation_result_id: int = Field(
        foreign_key="feegowvalidationresult.id",
        index=True
    )

    ack_user_id: int = Field(
        foreign_key="user.id",
        index=True
    )

    ack_message: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
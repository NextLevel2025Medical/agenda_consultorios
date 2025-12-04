# db.py
import os
from pathlib import Path
from sqlmodel import SQLModel, Session, create_engine

def _database_url() -> str:
    # 1) Se você setar DATABASE_URL no Render (Postgres / SQLite), ele ganha
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    # 2) Caso contrário, usa DB_PATH (recomendado pro SQLite no Render)
    db_path = os.getenv("DB_PATH", "./agenda.db")

    # Se for caminho absoluto Linux, garante que a pasta existe
    if db_path.startswith("/"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        return "sqlite:////" + db_path.lstrip("/")  # absoluto

    # Caso local (Windows/dev), mantém relativo
    return "sqlite:///" + db_path

DATABASE_URL = _database_url()

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, echo=False, connect_args=connect_args)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

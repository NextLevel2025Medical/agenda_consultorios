from passlib.context import CryptContext
from fastapi import HTTPException

# Troca bcrypt -> pbkdf2 (não depende de bcrypt)
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

def require(condition: bool, msg: str = "Não autorizado"):
    if not condition:
        raise HTTPException(status_code=403, detail=msg)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional
from passlib.context import CryptContext
from . import models
from .models import Session as UserSession
import os
from sqlalchemy.orm import Session

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db(db: Session):
    if not db.query(models.User).filter_by(user_login='a').first():
        customer = models.User(
            user_login="a",
            user_role="customer",
            user_password=pwd_context.hash("a")
        )
        seller = models.User(
            user_login="b",
            user_role="seller",
            user_password=pwd_context.hash("b")
        )
        db.add(customer)
        db.add(seller)
        db.commit()

def get_user_by_login(db: Session, user_login: str):
    return db.query(models.User).filter(models.User.user_login == user_login).first()

def get_session_by_token(db: Session, session_token: str) -> Optional[object]:
    from .models import Session as UserSession
    return db.query(UserSession).filter(UserSession.session_token == session_token).first()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def verify_session(session_token: str, db: Session) -> int:
    session = db.query(UserSession).filter(UserSession.session_token == session_token).first()
    return session.user_id
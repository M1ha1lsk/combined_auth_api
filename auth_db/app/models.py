from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import uuid
import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    user_login = Column(String(255), unique=True, nullable=False)
    user_password = Column(String(255), nullable=False)
    user_role = Column(String(255), nullable=False)

    sessions = relationship("Session", back_populates="user")

class Session(Base):
    __tablename__ = "sessions"
    session_token = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    session_start = Column(TIMESTAMP, default=datetime.datetime.utcnow)
    session_end = Column(TIMESTAMP, nullable=True)

    user = relationship("User", back_populates="sessions")

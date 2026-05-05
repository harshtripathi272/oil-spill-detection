import secrets
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.models.users import User
from app.schemas.dashboard import UserCreate, UserResponse, AuthResponse
from sqlalchemy import desc

router = APIRouter()


def hash_password(password: str) -> str:
    import hashlib
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


@router.get("/", response_model=List[UserResponse])
async def list_users(db: Session = Depends(get_db)):
    users = db.query(User).order_by(desc(User.id)).all()
    return users

@router.post("/", response_model=UserResponse)
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.username == user.username).first()
    if existing:
        raise HTTPException(status_code=400, detail="User already exists")

    db_user = User(
        username=user.username,
        full_name=user.full_name,
        email=user.email,
        role=user.role,
        hashed_password=hash_password(user.password),
        enabled=user.enabled
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

@router.post("/login", response_model=AuthResponse)
async def login(username: str, password: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == username).first()
    if not user or user.hashed_password != hash_password(password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = secrets.token_urlsafe(24)
    return AuthResponse(access_token=token, token_type="bearer")

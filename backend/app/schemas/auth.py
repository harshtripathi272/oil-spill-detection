from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# Alert schemas
class AlertBase(BaseModel):
    incident_id: Optional[str] = None
    level: str
    message: str
    extra_metadata: Optional[Dict[str, Any]] = None

class Alert(AlertBase):
    id: int
    created_at: datetime
    acknowledged: bool

    class Config:
        from_attributes = True

# User schemas
class UserBase(BaseModel):
    username: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: str = "analyst"
    enabled: bool = True

class UserCreate(UserBase):
    password: str

class UserResponse(UserBase):
    id: int

    class Config:
        from_attributes = True

class AuthResponse(BaseModel):
    access_token: str
    token_type: str

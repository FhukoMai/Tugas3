import jwt
import datetime
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.utils.config import settings

security = HTTPBearer()

def create_token(user_id: str, role: str):
    payload = {
        "user_id": user_id,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    }
    return jwt.encode(payload, settings.secret_key, algorithm="HS256")

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        payload = jwt.decode(credentials.credentials, settings.secret_key, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def require_role(role: str):
    def role_checker(payload: dict = Security(verify_token)):
        if payload.get("role") != role and payload.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Not enough permissions")
        return payload
    return role_checker

import os
from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import jwt
from jwt import InvalidTokenError as JWTError
from dotenv import load_dotenv
import bcrypt

# Charger les variables du .env
load_dotenv()

# ⚙️ Variables d'environnement
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH")  # hashé avec bcrypt

SECRET_KEY = os.getenv("SECRET_KEY", "super_secret_key")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

# ✅ Vérification du mot de passe
def verify_password(plain_password: str) -> bool:
    if not ADMIN_PASSWORD_HASH:
        raise HTTPException(status_code=500, detail="Server configuration error")
    return bcrypt.checkpw(plain_password.encode(), ADMIN_PASSWORD_HASH.encode())

# 🔐 Génération du token JWT
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    now = datetime.now()
    expire = now + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({
        "exp": expire,
        "iat": now,  # issued at
        "iss": "eyesight-api"  # issuer
    })
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# ⚡ Authentification de l'utilisateur
def authenticate_user(username: str, password: str) -> bool:
    if username != ADMIN_USERNAME:
        return False
    return verify_password(password)

# 📌 Dépendance pour protéger les routes
def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None or username != ADMIN_USERNAME:
            raise credentials_exception

        # Vérifier l'issuer si ajouté
        if payload.get("iss") != "eyesight-api":
            raise credentials_exception

        return username
    except JWTError:
        raise credentials_exception



# Vérifs de mon environnement
def validate_secret_key():
    """Valide la configuration de la SECRET_KEY au démarrage"""
    if not SECRET_KEY:
        raise ValueError("❌ SECRET_KEY is not set in environment variables")

    if len(SECRET_KEY) < 32:
        raise ValueError(f"❌ SECRET_KEY too short ({len(SECRET_KEY)} chars). Minimum: 32 characters")

    if SECRET_KEY in ["super_secret_key", "your_secret_key", "mysecret"]:
        raise ValueError("❌ SECRET_KEY is using a default/weak value")

    print("✅ SECRET_KEY validation passed")

# Validation des autres variables critiques
def validate_environment():
    """Valide toutes les variables d'environnement critiques"""
    validate_secret_key()

    if not ADMIN_USERNAME:
        raise ValueError("❌ ADMIN_USERNAME is not set")

    if not ADMIN_PASSWORD_HASH:
        raise ValueError("❌ ADMIN_PASSWORD_HASH is not set")

    print("✅ Environment validation completed")

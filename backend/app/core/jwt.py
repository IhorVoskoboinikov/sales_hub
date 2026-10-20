"""Access-токены: JWT с подписью RS256.

Подписывает приватный ключ (файл JWT_PRIVATE_KEY_FILE), проверяет публичный. Публичные ключи
отдаются в /.well-known/jwks.json — по ним токен может проверить любой сервис, не зная секрета.
В заголовке токена kid — отпечаток ключа (RFC 7638): по нему при ротации видно, каким из
ключей JWKS проверять подпись.
"""

import base64
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from app.core.config import Settings
from app.errors import Unauthorized

ALGORITHM = "RS256"
ACCESS_TOKEN_TYPE = "access"
REQUIRED_CLAIMS = ["iss", "aud", "typ", "sub", "sid", "ver", "jti", "iat", "exp"]


@dataclass(frozen=True)
class AccessClaims:
    user_id: uuid.UUID  # sub
    session_id: uuid.UUID  # sid — family_id refresh-токенов этого входа
    token_version: int  # ver — должен совпасть с users.token_version


def generate_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def private_key_to_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def public_jwk(key: rsa.RSAPublicKey) -> dict[str, Any]:
    jwk = RSAAlgorithm.to_jwk(key, as_dict=True)
    return {"kty": "RSA", "n": jwk["n"], "e": jwk["e"]}


def key_id(key: rsa.RSAPublicKey) -> str:
    """Отпечаток ключа по RFC 7638: SHA-256 от его JWK с полями e, kty, n по алфавиту."""
    canonical = json.dumps(public_jwk(key), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


class KeySet:
    """Ключ для подписи и все публичные ключи, по которым токены ещё проверяются."""

    def __init__(
        self,
        private_key: rsa.RSAPrivateKey,
        previous_public_keys: list[rsa.RSAPublicKey],
        *,
        issuer: str,
        audience: str,
        access_ttl: timedelta,
    ) -> None:
        self._private_key = private_key
        self.signing_kid = key_id(private_key.public_key())
        public_keys = [private_key.public_key(), *previous_public_keys]
        self._public_keys = {key_id(k): k for k in public_keys}
        self.issuer = issuer
        self.audience = audience
        self.access_ttl = access_ttl

    @classmethod
    def from_settings(cls, settings: Settings) -> "KeySet":
        return cls(
            _load_private_key(settings.jwt_private_key_file),
            [_load_public_key(path) for path in settings.jwt_previous_public_key_files],
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            access_ttl=timedelta(seconds=settings.jwt_access_ttl),
        )

    def jwks(self) -> dict[str, Any]:
        keys = [
            {**public_jwk(key), "kid": kid, "use": "sig", "alg": ALGORITHM}
            for kid, key in self._public_keys.items()
        ]
        return {"keys": keys}

    def issue_access_token(
        self, user_id: uuid.UUID, session_id: uuid.UUID, token_version: int
    ) -> str:
        now = datetime.now(UTC)
        claims = {
            "iss": self.issuer,
            "aud": self.audience,
            "typ": ACCESS_TOKEN_TYPE,
            "sub": str(user_id),
            "sid": str(session_id),
            "ver": token_version,
            "jti": uuid.uuid4().hex,
            "iat": now,
            "exp": now + self.access_ttl,
        }
        return jwt.encode(
            claims, self._private_key, algorithm=ALGORITHM, headers={"kid": self.signing_kid}
        )

    def decode_access_token(self, token: str) -> AccessClaims:
        """Проверить подпись, iss, aud, exp и typ. Истёк — auth.token_expired (клиенту пора
        делать refresh), всё остальное — auth.invalid_token."""
        try:
            kid = jwt.get_unverified_header(token).get("kid")
            key = self._public_keys.get(kid) if isinstance(kid, str) else None
            if key is None:
                raise Unauthorized()
            claims = jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],  # только RS256: иначе возможна подмена алгоритма
                audience=self.audience,
                issuer=self.issuer,
                options={"require": REQUIRED_CLAIMS},
            )
            if claims["typ"] != ACCESS_TOKEN_TYPE:
                raise Unauthorized()
            return AccessClaims(
                user_id=uuid.UUID(claims["sub"]),
                session_id=uuid.UUID(claims["sid"]),
                token_version=int(claims["ver"]),
            )
        except jwt.ExpiredSignatureError:
            raise Unauthorized("auth.token_expired") from None
        except (jwt.PyJWTError, ValueError, TypeError):
            raise Unauthorized() from None


def _load_private_key(path: Path) -> rsa.RSAPrivateKey:
    if not path.is_file():
        raise RuntimeError(
            f"нет приватного ключа JWT: {path}. В dev создай его командой make keys, "
            "в Kubernetes — смонтируй Secret и укажи путь в JWT_PRIVATE_KEY_FILE"
        )
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise RuntimeError(f"{path}: нужен RSA-ключ для RS256")
    return key


def _load_public_key(path: Path) -> rsa.RSAPublicKey:
    key = serialization.load_pem_public_key(path.read_bytes())
    if not isinstance(key, rsa.RSAPublicKey):
        raise RuntimeError(f"{path}: нужен публичный RSA-ключ")
    return key

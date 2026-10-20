"""Access-токены: выпуск, проверка, ротация ключей, JWKS."""

import base64
import hashlib
import hmac
import json
import uuid
from datetime import timedelta

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from freezegun import freeze_time
from httpx import AsyncClient
from jwt.algorithms import RSAAlgorithm

from app.core.jwt import KeySet, generate_private_key, key_id
from app.errors import Unauthorized

USER_ID = uuid.uuid4()
SESSION_ID = uuid.uuid4()
PRIVATE_KEY = generate_private_key()


def make_keys(
    private_key: rsa.RSAPrivateKey | None = None,
    previous: list[rsa.RSAPublicKey] | None = None,
    audience: str = "saleshub-api",
) -> KeySet:
    return KeySet(
        private_key or generate_private_key(),
        previous or [],
        issuer="https://saleshub.local",
        audience=audience,
        access_ttl=timedelta(hours=1),
    )


@pytest.fixture(scope="module")
def keys() -> KeySet:
    return make_keys(PRIVATE_KEY)


def test_token_roundtrip(keys: KeySet) -> None:
    token = keys.issue_access_token(USER_ID, SESSION_ID, token_version=3)

    claims = keys.decode_access_token(token)

    assert (claims.user_id, claims.session_id, claims.token_version) == (USER_ID, SESSION_ID, 3)
    assert jwt.get_unverified_header(token)["kid"] == keys.signing_kid


def test_expired_token_has_own_code(keys: KeySet) -> None:
    with freeze_time("2026-10-20 10:00:00"):
        token = keys.issue_access_token(USER_ID, SESSION_ID, 0)
    with freeze_time("2026-10-20 11:00:01"), pytest.raises(Unauthorized) as exc:
        keys.decode_access_token(token)

    assert exc.value.code == "auth.token_expired"  # клиент по нему понимает: пора делать refresh


def test_token_signed_by_other_key_is_rejected(keys: KeySet) -> None:
    token = make_keys().issue_access_token(USER_ID, SESSION_ID, 0)

    with pytest.raises(Unauthorized) as exc:
        keys.decode_access_token(token)
    assert exc.value.code == "auth.invalid_token"


def forged(header: dict[str, str], signature: bytes) -> str:
    """JWT, собранный руками: PyJWT сам не подпишет HS256 публичным ключом."""

    def b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    claims = {
        "iss": "https://saleshub.local",
        "aud": "saleshub-api",
        "typ": "access",
        "sub": str(USER_ID),
        "sid": str(SESSION_ID),
        "ver": 0,
        "jti": "x",
        "iat": 1,
        "exp": 9999999999,
    }
    signing_input = f"{b64(json.dumps(header).encode())}.{b64(json.dumps(claims).encode())}"
    return f"{signing_input}.{b64(signature)}" if signature else f"{signing_input}."


def test_garbage_token_is_rejected(keys: KeySet) -> None:
    with pytest.raises(Unauthorized):
        keys.decode_access_token("not-a-jwt")


def test_alg_none_is_rejected(keys: KeySet) -> None:
    # Классическая атака: токен без подписи, но с правильным kid.
    token = forged({"alg": "none", "kid": keys.signing_kid}, b"")

    with pytest.raises(Unauthorized) as exc:
        keys.decode_access_token(token)
    assert exc.value.code == "auth.invalid_token"


def test_hs256_signed_with_public_key_is_rejected(keys: KeySet) -> None:
    # Подмена алгоритма: публичный ключ известен всем (JWKS). Если сервер принял бы HS256,
    # любой подписал бы токен этим ключом как секретом.
    public_pem = PRIVATE_KEY.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    header = {"alg": "HS256", "kid": keys.signing_kid}
    unsigned = forged(header, b"")[:-1]
    signature = hmac.new(public_pem, unsigned.encode(), hashlib.sha256).digest()

    with pytest.raises(Unauthorized) as exc:
        keys.decode_access_token(forged(header, signature))
    assert exc.value.code == "auth.invalid_token"


def test_wrong_audience_is_rejected(keys: KeySet) -> None:
    token = make_keys(PRIVATE_KEY, audience="another-api").issue_access_token(
        USER_ID, SESSION_ID, 0
    )

    with pytest.raises(Unauthorized):
        keys.decode_access_token(token)


def test_old_key_still_verifies_after_rotation() -> None:
    old_private = generate_private_key()
    token = make_keys(old_private).issue_access_token(USER_ID, SESSION_ID, 0)

    rotated = make_keys(previous=[old_private.public_key()])

    assert rotated.decode_access_token(token).user_id == USER_ID
    kids = {k["kid"] for k in rotated.jwks()["keys"]}
    assert kids == {rotated.signing_kid, key_id(old_private.public_key())}


def test_kid_is_rfc7638_thumbprint() -> None:
    # Пример из RFC 7638, раздел 3.1.
    jwk = {
        "kty": "RSA",
        "e": "AQAB",
        "n": (
            "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6"
            "tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5"
            "v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD"
            "08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kE"
            "gU8awapJzKnqDKgw"
        ),
    }
    public_key = RSAAlgorithm.from_jwk(jwk)

    assert key_id(public_key) == "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"  # type: ignore[arg-type]


async def test_jwks_endpoint_publishes_signing_key(client: AsyncClient) -> None:
    resp = await client.get("/.well-known/jwks.json")

    assert resp.status_code == 200
    [key] = resp.json()["keys"]
    assert key["kty"] == "RSA"
    assert key["alg"] == "RS256"
    assert "d" not in key  # приватной части нет

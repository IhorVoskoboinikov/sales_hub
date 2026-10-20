"""Тексты для людей — украинские. Коды (ключи) — стабильные английские, их не переводят."""

from typing import Any

# Заголовок ошибки (поле title) по её коду (поле code).
ERRORS: dict[str, str] = {
    "internal": "Внутрішня помилка сервера",
    "not_found": "Не знайдено",
    "method_not_allowed": "Метод не підтримується",
    "forbidden": "Недостатньо прав",
    "auth.invalid_token": "Недійсний токен",
    "auth.token_expired": "Термін дії токена минув",
    "auth.invalid_credentials": "Невірний email або пароль",
    "auth.user_inactive": "Користувача заблоковано",
    "auth.email_not_verified": "Email не підтверджено",
    "conflict": "Конфлікт даних",
    "validation": "Помилка валідації",
    "rate_limited": "Забагато запитів, спробуйте пізніше",
    "unavailable": "Сервіс тимчасово недоступний",
    "http_error": "Помилка запиту",
    # Пароль
    "password.too_short": "Пароль закороткий",
    "password.too_long": "Пароль задовгий",
    "password.same_as_email": "Пароль не може збігатися з email",
}

# Темы писем по имени шаблона (app/texts/emails/<имя>.txt и .html).
EMAIL_SUBJECTS: dict[str, str] = {
    "invite": "Запрошення до SalesHub",
    "reset_password": "Зміна пароля в SalesHub",
}

# Сообщения валидации по типу ошибки Pydantic. В фигурных скобках — поля из ctx ошибки.
VALIDATION: dict[str, str] = {
    "missing": "Обов'язкове поле",
    "extra_forbidden": "Зайве поле",
    "json_invalid": "Некоректний JSON",
    "string_type": "Має бути рядком",
    "string_too_short": "Мінімальна довжина — {min_length}",
    "string_too_long": "Максимальна довжина — {max_length}",
    "string_pattern_mismatch": "Не відповідає формату",
    "int_type": "Має бути цілим числом",
    "int_parsing": "Має бути цілим числом",
    "float_type": "Має бути числом",
    "float_parsing": "Має бути числом",
    "decimal_parsing": "Має бути числом",
    "bool_type": "Має бути true або false",
    "bool_parsing": "Має бути true або false",
    "greater_than": "Має бути більше за {gt}",
    "greater_than_equal": "Має бути не менше {ge}",
    "less_than": "Має бути менше за {lt}",
    "less_than_equal": "Має бути не більше {le}",
    "enum": "Допустимі значення: {expected}",
    "literal_error": "Допустимі значення: {expected}",
    "uuid_parsing": "Некоректний UUID",
    "date_parsing": "Некоректна дата",
    "datetime_parsing": "Некоректні дата й час",
    "list_type": "Має бути списком",
    "dict_type": "Має бути об'єктом",
    "value_error": "Некоректне значення",
}
VALIDATION_DEFAULT = "Некоректне значення"


def error_title(code: str) -> str:
    # Код без своего текста (например, редкий HTTP-статус Starlette) — общий заголовок.
    return ERRORS.get(code, ERRORS["http_error"])


def validation_message(error_type: str, ctx: dict[str, Any] | None) -> str:
    template = VALIDATION.get(error_type, VALIDATION_DEFAULT)
    ctx = dict(ctx or {})
    if "expected" in ctx:
        # Pydantic пишет список вариантов по-английски: "'a', 'b' or 'c'".
        ctx["expected"] = str(ctx["expected"]).replace(" or ", " або ")
    try:
        return template.format(**ctx)
    except (KeyError, IndexError):
        return template

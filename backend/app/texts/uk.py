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
    "auth.wrong_password": "Невірний поточний пароль",
    "conflict": "Конфлікт даних",
    "validation": "Помилка валідації",
    "rate_limited": "Забагато запитів, спробуйте пізніше",
    "length_required": "Потрібен заголовок Content-Length",
    "payload_too_large": "Завеликий запит",
    "unavailable": "Сервіс тимчасово недоступний",
    "http_error": "Помилка запиту",
    # Приглашения и токены из писем
    "invite.stale": "Запрошення застаріло: email уже зайнятий або відділ видалено",
    "member.cannot_change_self": "Не можна змінювати власні роль, відділ чи статус",
    "member.last_admin": "В організації має залишитися хоча б один активний адміністратор",
    "member.team_required": "Для цієї ролі потрібен відділ",
    "member.team_not_allowed": "Для цієї ролі відділ не вказується",
    "member.team_not_found": "Такого відділу немає в організації",
    "email_token.invalid": "Токен недійсний або вже використаний",
    "email_token.expired": "Термін дії токена минув",
    # Организации и отделы
    "org.slug_taken": "Організація з таким slug уже існує",
    "team.name_taken": "Відділ з такою назвою вже є",
    "team.has_members": "У відділі є люди — спочатку переведіть їх в інший відділ",
    # CRM: компании, контакты, сделки
    "version.conflict": "Запис уже змінив хтось інший — оновіть сторінку й повторіть",
    "owner.required": "Вкажіть відповідального: керівника відділу або менеджера",
    "owner.invalid": "Відповідальним може бути лише активний керівник відділу або менеджер "
    "організації",
    "owner.inactive": "Відповідальний більше не веде записи — спочатку передайте їх іншому",
    "contact.duplicate_email": "Контакт з таким email уже є в організації",
    "member.has_records": "У людини є компанії, контакти, угоди чи відкриті активності — "
    "спочатку передайте їх іншому відповідальному",
    "team.has_records": "У відділу є компанії, контакти, угоди чи відкриті активності — "
    "спочатку передайте їх",
    "pagination.invalid_cursor": "Некоректний курсор сторінки",
    "pipeline.name_taken": "Воронка з такою назвою вже є",
    "pipeline.stage_not_found": "У воронці немає такого етапу",
    "pipeline.stage_duplicate": "Один етап указано двічі",
    "pipeline.stage_has_deals": "На етапі є угоди (зокрема в кошику) — спочатку перенесіть їх",
    # Масові операції
    "bulk.same_owner": "Не можна передати записи тій самій людині",
    "bulk.already_running": "Така масова операція в організації вже виконується — дочекайтеся "
    "її завершення",
    "bulk.idempotency_key_reused": "Цей Idempotency-Key уже використано для іншого запиту",
    "bulk.author_changed": "Автор операції більше не може її виконувати: його відключили, "
    "змінили роль, відділ або права",
    "bulk.target_changed": "Новий відповідальний більше не може вести записи або вийшов за межі "
    "ваших прав",
    "bulk.not_cancellable": "Операцію вже завершено або її не можна скасувати",
    "bulk.stuck": "Операція зависла: завдання кілька разів надсилали повторно без результату",
    # Імпорт контактів з CSV
    "import.length_required": "Потрібен заголовок Content-Length: завантаження частинами "
    "(chunked) не приймається",
    "import.file_too_large": "Файл перевищує дозволений розмір",
    "import.invalid_form": "Очікується форма multipart/form-data з полем file (CSV) і "
    "необов'язковим owner_id",
    "import.invalid_encoding": "Файл має бути в кодуванні UTF-8",
    "import.invalid_header": "Некоректний заголовок CSV: потрібні колонки email і first_name, "
    "без повторів",
    "import.empty_file": "Файл порожній",
    "import.too_many_rows": "У файлі забагато рядків",
    "import.malformed_csv": "Файл CSV пошкоджено: незакрита або зайва лапка, поле довше "
    "10 000 символів чи надто довгий рядок",
    # Вивантаження в CSV
    "export.too_large": "Забагато рядків для вивантаження — звузьте фільтр",
    "export.busy": "Зараз виконується забагато вивантажень — спробуйте трохи пізніше",
    # Задачі, що впали після всіх повторів
    "failed_task.already_replayed": "Це завдання вже перезапущено",
    "failed_task.unknown_task": "Такого завдання більше немає серед зареєстрованих",
    "failed_task.not_replayable": "Це завдання не перезапускається вручну — запустіть нову "
    "операцію",
    # Валюти
    "fx.unknown_currency": "НБУ не встановлює курс для цієї валюти",
    "fx.unavailable": "Курс НБУ тимчасово недоступний",
    # Telegram (раздел 16)
    "telegram.disabled": "Telegram-бот не налаштовано",
    "telegram.access_denied": "Доступ закрито: обліковий запис SalesHub відключено або він не "
    "належить до організації",
    "telegram.link_conflict": "Не вдалося підключити Telegram, спробуйте ще раз",
    "telegram.chat_taken": "Цей чат уже підключено до іншого акаунта SalesHub (надішліть /start, "
    "щоб побачити, до якого). Спершу відключіть його в профілі того акаунта",
    # Права
    "authz.policy_out_of_sync": "Правила доступу не узгоджені, зверніться до адміністратора",
    # Пароль
    "password.too_short": "Пароль закороткий",
    "password.too_long": "Пароль задовгий",
    "password.same_as_email": "Пароль не може збігатися з email",
}

# Названия ролей — в письмах и для людей. Коды ролей (org_admin…) не переводятся.
ROLE_TITLES: dict[str, str] = {
    "org_admin": "Адміністратор організації",
    "team_lead": "Керівник відділу продажів",
    "sales_rep": "Менеджер з продажів",
    "viewer": "Спостерігач",
}

# Темы писем по имени шаблона (app/texts/emails/<имя>.txt и .html).
EMAIL_SUBJECTS: dict[str, str] = {
    "invite": "Запрошення до SalesHub",
    "already_member": "Запрошення до SalesHub",
    "reset_password": "Зміна пароля в SalesHub",
    # Уведомления CRM (раздел 16); тот же шаблон .txt — текст сообщения в Telegram.
    "record_assigned": "Вам призначено запис у SalesHub",
    "deal_closed": "Угоду закрито — SalesHub",
    "bulk_job_completed": "Масову операцію завершено — SalesHub",
    "bulk_reassigned": "Вам передано клієнтів — SalesHub",
}

# Слова для уведомлений notifier (раздел 16): подставляются в шаблоны готовыми строками.
NOTIFY_ENTITIES: dict[str, str] = {"contact": "контакт", "deal": "угоду"}
NOTIFY_UNKNOWN_USER = "користувач SalesHub"  # автора уже удалили — email не показать
NOTIFY_DEAL_RESULTS: dict[str, str] = {"won": "виграно", "lost": "програно"}
NOTIFY_BULK_KINDS: dict[str, str] = {
    "reassign_owner": "Передача клієнтів",
    "move_team": "Перенесення записів у новий відділ",
    "import_contacts": "Імпорт контактів",
    "close_overdue_deals": "Закриття прострочених угод",
    "add_tags": "Додавання тегу",
    "remove_tags": "Зняття тегу",
}
NOTIFY_BULK_STATUSES: dict[str, str] = {
    "done": "виконано",
    "partial_failed": "виконано частково",
    "failed": "не виконано",
    "cancelled": "скасовано",
}
# Строки итога job по сущностям (result.rows); failed — контакты, пропущенные add_tags.
NOTIFY_BULK_ROWS: dict[str, str] = {
    "companies": "компанії",
    "contacts": "контакти",
    "deals": "угоди",
    "activities": "активності",
    "failed": "пропущено",
}

# Telegram-бот (раздел 16). Простой текст без parse_mode: экранировать ничего не нужно.
BOT: dict[str, str] = {
    "linked": "Акаунт SalesHub {email} підключено. Сюди надходитимуть сповіщення.",
    "connected_as": "Ви підключені як {email}.",
    "how_to_link": "Щоб підключити бота, відкрийте профіль у SalesHub, натисніть «Підключити "
    "Telegram» і перейдіть за посиланням — воно діє кілька хвилин.",
    "commands": "Доступні команди:\n{commands}",
    "error": "Сталася помилка, спробуйте пізніше",
    "today_title": "План на {date}:",
    "today_empty": "На сьогодні відкритих завдань і прострочених немає. Гарного дня!",
    "deals_empty": "Відкритих угод немає.",
    "stopped": "Telegram відключено від SalesHub: сповіщення сюди більше не надходитимуть. "
    "Підключити знову можна в профілі SalesHub.",
    "not_linked": "Цей чат не підключено до SalesHub.",
}

# Повестка дня (AgendaService, раздел 16): /today и утренний дайджест (шаг 5.14).
AGENDA: dict[str, str] = {
    "today_none": "На сьогодні завдань немає.",
    "overdue": "Прострочено:",
    "more": "…та інші — усі в SalesHub",
}
AGENDA_KINDS: dict[str, str] = {
    "call": "дзвінок",
    "meeting": "зустріч",
    "task": "завдання",
    "email": "лист",
}

# Задачи beat (шаг 5.14): напоминание за 15 минут и заголовок утреннего дайджеста (дальше —
# та же повестка, что у /today).
REMINDER = "Через {minutes} хв (о {time}) — {kind}: {subject}"
DIGEST_TITLE = "Доброго ранку! План на {date}:"

# /deals: итог по всем открытым сделкам и до 10 новых.
DEALS: dict[str, str] = {
    "total": "Ваші відкриті угоди: {count} на суму {amount}",
    "stale": " (курс на {date})",
    "more": "…та інші — усі в SalesHub",
}

# Команды бота: меню Telegram (set_my_commands) и ответ на любой другой текст.
BOT_COMMANDS: dict[str, str] = {
    "start": "підключити акаунт SalesHub",
    "today": "мої завдання на сьогодні та прострочені",
    "deals": "мої відкриті угоди",
    "stop": "відключити бота і сповіщення в Telegram",
}

# Ответ бота на ошибку по её коду, если он отличается от title ошибки API (ERRORS).
BOT_ERRORS: dict[str, str] = {
    "email_token.invalid": "Посилання недійсне або вже використане — отримайте нове в профілі "
    "SalesHub",
    "email_token.expired": "Термін дії посилання минув — отримайте нове в профілі SalesHub",
    # У роли нет действия (activity:read, deal:read) — команда ей недоступна.
    "forbidden": "Недоступно для вашої ролі",
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


def bot_error_text(code: str, title: str) -> str:
    """Ответ бота на AppError: свой текст для бота, иначе — title ошибки."""
    return BOT_ERRORS.get(code, title)


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

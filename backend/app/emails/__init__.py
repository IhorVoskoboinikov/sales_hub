"""Письма: отрисовка шаблонов и отправка по SMTP (раздел 4).

Сервисы письма не отправляют: они пишут команду email.send в outbox в той же транзакции, что
и данные (OutboxRepository.add_email). После коммита relay ставит задачу
notifications.send_email, и воркер отрисовывает шаблон (render.py) и отправляет его
(sender.py). До этапа 5 отправлял сам API через BackgroundTasks — ошибка SMTP теряла письмо.
"""

"""Воркеры Celery (раздел 15): задачи — тонкие обёртки над теми же сервисами, что у API.

Запуск: celery -A app.workers.celery_app worker -Q default,notifications
"""

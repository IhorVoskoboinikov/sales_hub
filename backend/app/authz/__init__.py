"""Права (раздел 5) в два шага:

1. OPA: может ли роль вызвать этот эндпоинт (opa.py, deps.py).
2. Бэкенд: какие записи видны и какие можно менять — объект Access (access.py), уровень
   own / team / org — из того же policies/saleshub/data.yaml, что читает OPA (roles.py).
"""

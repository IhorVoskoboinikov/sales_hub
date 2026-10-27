# policies/saleshub/authz.rego — можно ли этой роли вызвать этот эндпоинт.
# Данные (свои / отдела / организации) здесь НЕ проверяются — это делает бэкенд.
package saleshub.authz

# 1. Какое действие выполняет эндпоинт: "PATCH /api/v1/deals/{deal_id}" → "deal:write"
action := data.saleshub.routes.actions[concat(" ", [input.method, input.route])]

# 2. Разрешено ли действие. По умолчанию — нет.
default allow := false

allow if {
	data.saleshub.roles[input.user.role].permissions[action] # у роли есть это действие
}

allow if {
	input.user.is_superadmin # супер-админ работает только через /api/v1/admin/*
	startswith(action, "admin:")
}

# 3. Ответ бэкенду: можно ли и какое это действие (по нему бэкенд найдёт уровень в data.yaml).
# METADATA
# description: ответ бэкенду — его запрашивает app/authz/opa.py
# entrypoint: true
default decision := {"allow": false}

decision := {"allow": allow, "action": action} if action

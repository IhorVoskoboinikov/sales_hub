# policies/saleshub/routes.rego — какой эндпоинт какое действие выполняет.
# Новый эндпоинт без строки здесь получит 403 у всех, а pytest-тест test_routes_match_policy упадёт.
# Строки добавляются вместе с эндпоинтами: на этапе 3 — первые два блока, остальные — на этапах 4–9.
package saleshub.routes

actions := {
	# --- этап 3: только супер-админ (действий admin:* нет ни у одной роли в data.yaml)
	"GET /api/v1/admin/orgs": "admin:orgs",
	"POST /api/v1/admin/orgs": "admin:orgs",
	"POST /api/v1/admin/orgs/{org_id}/invites": "admin:orgs",
	# --- этап 3: организация, отделы, люди
	"GET /api/v1/org": "org:read",
	"PATCH /api/v1/org": "org:manage",
	"GET /api/v1/teams": "org:read",
	"POST /api/v1/teams": "org:manage",
	"PATCH /api/v1/teams/{team_id}": "org:manage",
	"DELETE /api/v1/teams/{team_id}": "org:manage",
	"GET /api/v1/users": "member:read",
	"PATCH /api/v1/users/{user_id}": "member:manage",
	"POST /api/v1/invites": "member:manage",
	# --- этап 4: компании и контакты
	"GET /api/v1/companies": "company:read",
	"GET /api/v1/companies/{company_id}": "company:read",
	"POST /api/v1/companies": "company:write",
	"PATCH /api/v1/companies/{company_id}": "company:write",
	"GET /api/v1/contacts": "contact:read",
	"GET /api/v1/contacts/{contact_id}": "contact:read",
	"POST /api/v1/contacts": "contact:write",
	"PATCH /api/v1/contacts/{contact_id}": "contact:write",
	# --- этап 4: воронки и сделки
	"GET /api/v1/pipelines": "org:read",
	"POST /api/v1/pipelines": "org:manage",
	"PATCH /api/v1/pipelines/{pipeline_id}": "org:manage",
	"GET /api/v1/deals": "deal:read",
	"GET /api/v1/deals/kanban": "deal:read",
	"GET /api/v1/deals/{deal_id}": "deal:read",
	"POST /api/v1/deals": "deal:write",
	"PATCH /api/v1/deals/{deal_id}": "deal:write",
}

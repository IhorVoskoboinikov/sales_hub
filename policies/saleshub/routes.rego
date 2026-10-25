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
	# --- этап 4: активности, заметки, журнал
	"GET /api/v1/activities": "activity:read",
	"POST /api/v1/activities": "activity:write",
	"PATCH /api/v1/activities/{activity_id}": "activity:write",
	"DELETE /api/v1/activities/{activity_id}": "activity:write",
	"GET /api/v1/contacts/{contact_id}/notes": "contact:read",
	"POST /api/v1/contacts/{contact_id}/notes": "contact:write",
	"GET /api/v1/audit": "audit:read",
	# --- этап 4: корзина
	"DELETE /api/v1/companies/{company_id}": "company:delete",
	"POST /api/v1/companies/{company_id}/restore": "company:delete",
	"GET /api/v1/contacts/trash": "contact:delete",
	"DELETE /api/v1/contacts/{contact_id}": "contact:delete",
	"POST /api/v1/contacts/{contact_id}/restore": "contact:delete",
	"DELETE /api/v1/deals/{deal_id}": "deal:delete",
	"POST /api/v1/deals/{deal_id}/restore": "deal:delete",
	# --- этап 4: отчёты
	"GET /api/v1/reports/pipeline": "report:read",
	"GET /api/v1/reports/managers": "report:read",
	# --- этап 5: массовые операции и выгрузка (каждый шаг добавляет свои строки)
	"POST /api/v1/bulk/reassign-owner": "bulk:run",
	"GET /api/v1/bulk-jobs/{job_id}": "bulk_job:read",
	"GET /api/v1/bulk-jobs/{job_id}/events": "bulk_job:read",
	"POST /api/v1/bulk-jobs/{job_id}/cancel": "bulk_job:cancel",
	"POST /api/v1/contacts/import": "bulk:run",
	"GET /api/v1/bulk-jobs/{job_id}/errors": "bulk_job:read",
	"GET /api/v1/admin/failed-tasks": "admin:system",
	"GET /api/v1/admin/failed-tasks/{failed_task_id}": "admin:system",
	"POST /api/v1/admin/failed-tasks/{failed_task_id}/replay": "admin:system",
}

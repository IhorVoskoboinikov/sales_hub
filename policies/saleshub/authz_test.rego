# policies/saleshub/authz_test.rego — запуск: make policy-test (opa test -b policies -v)
# Тесты на маршруты этапов 4–9 добавляются вместе с этими маршрутами.
package saleshub.authz_test

import data.saleshub.authz

super_admin := {"id": "u0", "is_superadmin": true, "role": null}

org_admin := {"id": "u1", "is_superadmin": false, "role": "org_admin"}

team_lead := {"id": "u2", "is_superadmin": false, "role": "team_lead"}

sales_rep := {"id": "u3", "is_superadmin": false, "role": "sales_rep"}

viewer := {"id": "u4", "is_superadmin": false, "role": "viewer"}

req(user, method, route) := {"user": user, "method": method, "route": route}

test_decision_has_action if {
	d := authz.decision with input as req(team_lead, "GET", "/api/v1/users")
	d.allow
	d.action == "member:read"
}

test_everyone_in_org_reads_org_and_teams if {
	every user in [org_admin, team_lead, sales_rep, viewer] {
		authz.allow with input as req(user, "GET", "/api/v1/org")
		authz.allow with input as req(user, "GET", "/api/v1/teams")
	}
}

test_only_org_admin_manages_org if {
	authz.allow with input as req(org_admin, "PATCH", "/api/v1/org")
	authz.allow with input as req(org_admin, "POST", "/api/v1/teams")
	every user in [team_lead, sales_rep, viewer] {
		not authz.allow with input as req(user, "PATCH", "/api/v1/org")
		not authz.allow with input as req(user, "DELETE", "/api/v1/teams/{team_id}")
	}
}

test_only_org_admin_manages_members if {
	authz.allow with input as req(org_admin, "POST", "/api/v1/invites")
	authz.allow with input as req(org_admin, "PATCH", "/api/v1/users/{user_id}")
	every user in [team_lead, sales_rep, viewer] {
		not authz.allow with input as req(user, "POST", "/api/v1/invites")
		not authz.allow with input as req(user, "PATCH", "/api/v1/users/{user_id}")
	}
}

test_only_super_admin_creates_orgs if {
	authz.allow with input as req(super_admin, "POST", "/api/v1/admin/orgs")
	authz.allow with input as req(super_admin, "POST", "/api/v1/admin/orgs/{org_id}/invites")
	not authz.allow with input as req(org_admin, "POST", "/api/v1/admin/orgs")
	not authz.allow with input as req(org_admin, "GET", "/api/v1/admin/orgs")
}

test_everyone_reads_companies_viewer_cannot_write if {
	every user in [org_admin, team_lead, sales_rep, viewer] {
		authz.allow with input as req(user, "GET", "/api/v1/companies")
		authz.allow with input as req(user, "GET", "/api/v1/companies/{company_id}")
	}
	every user in [org_admin, team_lead, sales_rep] {
		authz.allow with input as req(user, "POST", "/api/v1/companies")
		authz.allow with input as req(user, "PATCH", "/api/v1/companies/{company_id}")
	}
	not authz.allow with input as req(viewer, "POST", "/api/v1/companies")
	not authz.allow with input as req(viewer, "PATCH", "/api/v1/companies/{company_id}")
}

test_contacts_read_by_all_written_by_sellers if {
	d := authz.decision with input as req(sales_rep, "PATCH", "/api/v1/contacts/{contact_id}")
	d.allow
	d.action == "contact:write"
	every user in [org_admin, team_lead, sales_rep, viewer] {
		authz.allow with input as req(user, "GET", "/api/v1/contacts")
		authz.allow with input as req(user, "GET", "/api/v1/contacts/{contact_id}")
	}
	authz.allow with input as req(team_lead, "POST", "/api/v1/contacts")
	not authz.allow with input as req(viewer, "POST", "/api/v1/contacts")
	not authz.allow with input as req(super_admin, "GET", "/api/v1/contacts")
}

test_pipelines_read_by_all_managed_by_org_admin if {
	every user in [org_admin, team_lead, sales_rep, viewer] {
		authz.allow with input as req(user, "GET", "/api/v1/pipelines")
		authz.allow with input as req(user, "GET", "/api/v1/deals/kanban")
	}
	authz.allow with input as req(org_admin, "PATCH", "/api/v1/pipelines/{pipeline_id}")
	every user in [team_lead, sales_rep, viewer] {
		not authz.allow with input as req(user, "POST", "/api/v1/pipelines")
	}
}

test_sales_rep_can_edit_deal if {
	d := authz.decision with input as req(sales_rep, "PATCH", "/api/v1/deals/{deal_id}")
	d.allow
	d.action == "deal:write"
	authz.allow with input as req(sales_rep, "POST", "/api/v1/deals")
	authz.allow with input as req(viewer, "GET", "/api/v1/deals/{deal_id}")
	not authz.allow with input as req(viewer, "PATCH", "/api/v1/deals/{deal_id}")
}

test_activities_and_notes if {
	every user in [org_admin, team_lead, sales_rep, viewer] {
		authz.allow with input as req(user, "GET", "/api/v1/activities")
		authz.allow with input as req(user, "GET", "/api/v1/contacts/{contact_id}/notes")
	}
	authz.allow with input as req(sales_rep, "DELETE", "/api/v1/activities/{activity_id}")
	authz.allow with input as req(sales_rep, "POST", "/api/v1/contacts/{contact_id}/notes")
	not authz.allow with input as req(viewer, "POST", "/api/v1/activities")
	not authz.allow with input as req(viewer, "POST", "/api/v1/contacts/{contact_id}/notes")
}

test_only_org_admin_reads_audit if {
	authz.allow with input as req(org_admin, "GET", "/api/v1/audit")
	every user in [team_lead, sales_rep, viewer, super_admin] {
		not authz.allow with input as req(user, "GET", "/api/v1/audit")
	}
}

# Менеджер не удаляет даже свои записи — только руководитель и админ (раздел 5).
test_sales_rep_cannot_delete_contact if {
	not authz.allow with input as req(sales_rep, "DELETE", "/api/v1/contacts/{contact_id}")
	not authz.allow with input as req(sales_rep, "GET", "/api/v1/contacts/trash")
	not authz.allow with input as req(viewer, "POST", "/api/v1/deals/{deal_id}/restore")
	every user in [org_admin, team_lead] {
		authz.allow with input as req(user, "DELETE", "/api/v1/companies/{company_id}")
		authz.allow with input as req(user, "POST", "/api/v1/contacts/{contact_id}/restore")
		authz.allow with input as req(user, "DELETE", "/api/v1/deals/{deal_id}")
	}
}

test_everyone_in_org_reads_reports_in_own_level if {
	every user in [org_admin, team_lead, sales_rep, viewer] {
		d := authz.decision with input as req(user, "GET", "/api/v1/reports/managers")
		d.allow
		d.action == "report:read"
	}
	not authz.allow with input as req(super_admin, "GET", "/api/v1/reports/pipeline")
}

# У супер-админа нет организации, поэтому в обычные эндпоинты он не ходит.
test_super_admin_only_admin_routes if {
	authz.allow with input as req(super_admin, "GET", "/api/v1/admin/orgs")
	not authz.allow with input as req(super_admin, "GET", "/api/v1/org")
	not authz.allow with input as req(super_admin, "POST", "/api/v1/invites")
}

test_unknown_route_denied_even_for_super_admin if {
	d := authz.decision with input as req(super_admin, "GET", "/api/v1/secret")
	not d.allow
	not d.action
}

test_wrong_method_denied if {
	not authz.allow with input as req(org_admin, "DELETE", "/api/v1/org")
}

test_user_without_role_denied if {
	not authz.allow with input as req({"id": "u9", "is_superadmin": false, "role": null}, "GET", "/api/v1/org")
}

# Каждое действие из routes.rego должно быть хотя бы у одной роли в data.yaml,
# либо быть админским (admin:*). Ловит опечатку в имени действия.
test_every_action_is_known if {
	known := {a | some r in data.saleshub.roles; some a, _ in r.permissions}
	unknown := {a | some a in data.saleshub.routes.actions; not a in known; not startswith(a, "admin:")}
	count(unknown) == 0
}

# Значения в data.yaml — только own / team / org.
test_scopes_are_valid if {
	bad := {[name, a, s] |
		some name, r in data.saleshub.roles
		some a, s in r.permissions
		not s in {"own", "team", "org"}
	}
	count(bad) == 0
}

rank := {"own": 1, "team": 2, "org": 3}

# Массовые операции и выгрузка не шире, чем права на одну запись:
# иначе через bulk можно было бы обойти ограничения роли.
test_bulk_and_export_not_wider_than_single_record if {
	# Множество пар, а не объект: у bulk:run несколько «базовых» действий — передача клиентов
	# меняет компании, контакты, сделки и активности одним уровнем bulk:run.
	pairs := {
		["bulk:run", "company:write"], ["bulk:run", "contact:write"],
		["bulk:run", "deal:write"], ["bulk:run", "activity:write"],
		["export:run", "contact:read"], ["bulk_job:cancel", "bulk_job:read"],
	}
	bad := {[name, a] |
		some name, r in data.saleshub.roles
		some [a, base] in pairs
		r.permissions[a]
		not rank[r.permissions[a]] <= rank[r.permissions[base]]
	}
	count(bad) == 0
}

test_bulk_run_by_org_admin_and_team_lead if {
	every user in [org_admin, team_lead] {
		d := authz.decision with input as req(user, "POST", "/api/v1/bulk/reassign-owner")
		d.allow
		d.action == "bulk:run"
	}
	not authz.allow with input as req(super_admin, "POST", "/api/v1/bulk/reassign-owner")
}

# Менеджер не запускает массовые операции и не выгружает клиентов.
test_sales_rep_has_no_bulk_and_export if {
	not data.saleshub.roles.sales_rep.permissions["bulk:run"]
	not data.saleshub.roles.sales_rep.permissions["export:run"]
	not data.saleshub.roles.sales_rep.permissions["bulk_job:read"]
	not authz.allow with input as req(sales_rep, "POST", "/api/v1/bulk/reassign-owner")
	not authz.allow with input as req(sales_rep, "GET", "/api/v1/bulk-jobs/{job_id}")
}

# Наблюдатель выгружает (export:run — org), но массовых изменений не запускает.
test_viewer_exports_but_cannot_run_bulk if {
	data.saleshub.roles.viewer.permissions["export:run"] == "org"
	not data.saleshub.roles.viewer.permissions["bulk:run"]
	not authz.allow with input as req(viewer, "POST", "/api/v1/bulk/reassign-owner")
}

# Наблюдатель видит только свои job (выгрузки в S3 — этап 10).
test_viewer_reads_own_jobs if {
	data.saleshub.roles.viewer.permissions["bulk_job:read"] == "own"
	every route in ["/api/v1/bulk-jobs/{job_id}", "/api/v1/bulk-jobs/{job_id}/events"] {
		d := authz.decision with input as req(viewer, "GET", route)
		d.allow
		d.action == "bulk_job:read"
	}
}

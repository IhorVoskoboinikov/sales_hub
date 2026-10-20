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
	pairs := {"bulk:run": "contact:write", "export:run": "contact:read"}
	bad := {[name, a] |
		some name, r in data.saleshub.roles
		some a, base in pairs
		r.permissions[a]
		not rank[r.permissions[a]] <= rank[r.permissions[base]]
	}
	count(bad) == 0
}

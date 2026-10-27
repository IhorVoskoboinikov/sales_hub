"""CI (раздел 12): обязательная проверка ci-ok ждёт все jobs.

Защита main требует только ci-ok. Job, которого нет в его needs, мог бы упасть, а PR всё равно
смержился бы — тест ловит это раньше, как test_every_route_has_a_case ловит маршрут без кейса.
"""

from pathlib import Path

import yaml

CI = Path(__file__).resolve().parents[3] / ".github" / "workflows" / "ci.yml"


def test_ci_ok_needs_every_job() -> None:
    jobs = yaml.safe_load(CI.read_text(encoding="utf-8"))["jobs"]
    gate = jobs.pop("ci-ok")

    assert sorted(gate["needs"]) == sorted(jobs)
    assert gate["if"] == "always()"

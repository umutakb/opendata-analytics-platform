from __future__ import annotations

from typing import Any

from opendata_platform.quality import dq_checks


def run(conn: Any, config: dict[str, Any]) -> list[dict[str, Any]]:
    quality_cfg = config.get("quality", {})
    return dq_checks.check_pk_uniqueness(conn, quality_cfg.get("pk_uniqueness", []))


from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any


def _render_row(row: dict[str, Any]) -> str:
    status = html.escape(str(row.get("status", "fail")))
    css_class = f"status-{status}"
    return (
        f"<tr class='{css_class}'>"
        f"<td>{html.escape(str(row.get('check_type', '')))}</td>"
        f"<td>{html.escape(str(row.get('target', '')))}</td>"
        f"<td>{status}</td>"
        f"<td>{html.escape(str(row.get('observed', '')))}</td>"
        f"<td>{html.escape(str(row.get('warn_threshold', '')))}</td>"
        f"<td>{html.escape(str(row.get('fail_threshold', '')))}</td>"
        f"<td>{html.escape(str(row.get('message', '')))}</td>"
        "</tr>"
    )


def render_quality_html(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    checks = report.get("checks", [])
    rows_html = "\n".join(_render_row(row) for row in checks)

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <title>Data Quality Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1 {{ margin-bottom: 4px; }}
    .summary {{ display: flex; gap: 12px; margin: 16px 0 24px 0; }}
    .card {{ border: 1px solid #d1d5db; padding: 12px 16px; border-radius: 8px; min-width: 120px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 8px; text-align: left; font-size: 13px; }}
    th {{ background: #f9fafb; }}
    .status-pass {{ background: #ecfdf5; }}
    .status-warn {{ background: #fffbeb; }}
    .status-fail {{ background: #fef2f2; }}
  </style>
</head>
<body>
  <h1>Data Quality Report</h1>
  <div>Generated at: {html.escape(str(report.get("generated_at", "")))}</div>

  <div class=\"summary\">
    <div class=\"card\"><strong>PASS</strong><br>{summary.get("pass", 0)}</div>
    <div class=\"card\"><strong>WARN</strong><br>{summary.get("warn", 0)}</div>
    <div class=\"card\"><strong>FAIL</strong><br>{summary.get("fail", 0)}</div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Check Type</th>
        <th>Target</th>
        <th>Status</th>
        <th>Observed</th>
        <th>Warn Threshold</th>
        <th>Fail Threshold</th>
        <th>Message</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</body>
</html>
"""


def write_quality_report(report: dict[str, Any], out_dir: str | Path) -> tuple[Path, Path]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    json_path = out_path / "report.json"
    html_path = out_path / "report.html"

    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    html_path.write_text(render_quality_html(report), encoding="utf-8")

    return json_path, html_path

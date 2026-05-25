#!/usr/bin/env python3
"""
Usage:
  python validate-and-report.py \
    --known-schemas ../schemas \
    --inferred-schemas ../inferred-schemas \
    --output ../reports/report.html \
    --sample-size 100
"""
import argparse
import json
import os
import datetime
import sys
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class Finding:
    level: str    # FAIL | WARNING | INFO
    message: str


@dataclass
class EventResult:
    topic: str
    event_type: str
    status: str   # PASS | WARNING | FAIL
    findings: list = field(default_factory=list)
    known_schema: dict = field(default_factory=dict)
    inferred_schema: dict = field(default_factory=dict)


def compare_schemas(known, inferred):
    findings = []
    known_props = known.get("properties", {})
    inferred_props = inferred.get("properties", {})
    known_required = set(known.get("required", []))
    inferred_required = set(inferred.get("required", []))

    # FAIL: required fields not marked required in inferred
    for f in (known_required - inferred_required):
        findings.append(Finding("FAIL", f"Required field '{f}' not marked required in inferred schema"))

    for prop, schema in known_props.items():
        if prop not in inferred_props:
            findings.append(Finding("WARNING", f"Property '{prop}' from ground truth absent in inferred schema"))
            continue
        inf_prop = inferred_props[prop]
        known_type = schema.get("type")
        inferred_type = inf_prop.get("type")
        if known_type and inferred_type and known_type != inferred_type:
            findings.append(Finding("FAIL", f"Property '{prop}' type mismatch: expected '{known_type}', got '{inferred_type}'"))
        if "enum" in schema:
            missing_enum = set(schema["enum"]) - set(inf_prop.get("enum", []))
            if missing_enum:
                findings.append(Finding("WARNING", f"Property '{prop}' missing enum values: {sorted(missing_enum)}"))

    # WARNING: unexpected properties in inferred
    for prop in inferred_props:
        if prop not in known_props:
            findings.append(Finding("WARNING", f"Inferred schema has unexpected property '{prop}'"))

    return findings


def validate_all(known_root, inferred_root):
    results = []
    for topic_dir in sorted(known_root.iterdir()):
        if not topic_dir.is_dir():
            continue
        topic = topic_dir.name
        for schema_file in sorted(topic_dir.glob("*.json")):
            event_type = schema_file.stem
            known = json.loads(schema_file.read_text())
            inferred_path = inferred_root / topic / schema_file.name
            if not inferred_path.exists():
                results.append(EventResult(
                    topic=topic, event_type=event_type, status="FAIL",
                    findings=[Finding("FAIL", "kafka-spy produced no schema file for this event type")],
                    known_schema=known
                ))
                continue
            inferred = json.loads(inferred_path.read_text())
            findings = compare_schemas(known, inferred)
            if any(f.level == "FAIL" for f in findings):
                status = "FAIL"
            elif any(f.level == "WARNING" for f in findings):
                status = "WARNING"
            else:
                status = "PASS"
            results.append(EventResult(
                topic=topic, event_type=event_type, status=status,
                findings=findings, known_schema=known, inferred_schema=inferred
            ))
    return results


def render_html(results: list, sample_size: int, output: Path):
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    pass_count    = sum(1 for r in results if r.status == "PASS")
    warning_count = sum(1 for r in results if r.status == "WARNING")
    fail_count    = sum(1 for r in results if r.status == "FAIL")

    status_badge = {
        "PASS":    '<span class="badge pass">PASS</span>',
        "WARNING": '<span class="badge warn">WARN</span>',
        "FAIL":    '<span class="badge fail">FAIL</span>',
    }

    rows = "\n".join(
        f'<tr><td>{r.topic}</td><td>{r.event_type}</td>'
        f'<td>{status_badge[r.status]}</td>'
        f'<td>{len(r.findings)}</td></tr>'
        for r in results
    )

    details = ""
    for r in results:
        finding_items = "".join(
            f'<li class="{f.level.lower()}">[{f.level}] {f.message}</li>'
            for f in r.findings
        ) or "<li class='pass'>No issues found.</li>"

        details += f"""
        <section class="event-section" id="{r.event_type}">
          <h3>{r.event_type} <small>({r.topic})</small> {status_badge[r.status]}</h3>
          <ul class="findings">{finding_items}</ul>
          <div class="schema-grid">
            <div>
              <h4>Ground Truth Schema</h4>
              <pre>{json.dumps(r.known_schema, indent=2)}</pre>
            </div>
            <div>
              <h4>Inferred by kafka-spy</h4>
              <pre>{json.dumps(r.inferred_schema, indent=2) if r.inferred_schema else "— file not found —"}</pre>
            </div>
          </div>
        </section>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Kafka Spy Validation Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #222; }}
    h1 {{ border-bottom: 3px solid #2563eb; padding-bottom: .5rem; }}
    .meta {{ color: #555; margin-bottom: 2rem; }}
    .summary-stats {{ display: flex; gap: 1.5rem; margin-bottom: 2rem; }}
    .stat-card {{ padding: 1rem 2rem; border-radius: 8px; text-align: center; }}
    .stat-card.pass   {{ background: #dcfce7; }}
    .stat-card.warn   {{ background: #fef9c3; }}
    .stat-card.fail   {{ background: #fee2e2; }}
    .stat-card .num {{ font-size: 2rem; font-weight: bold; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
    th, td {{ text-align: left; padding: .5rem .75rem; border-bottom: 1px solid #e5e7eb; }}
    th {{ background: #f3f4f6; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: .8rem; font-weight: bold; }}
    .badge.pass {{ background: #16a34a; color: white; }}
    .badge.warn {{ background: #d97706; color: white; }}
    .badge.fail {{ background: #dc2626; color: white; }}
    .event-section {{ margin-bottom: 3rem; border: 1px solid #e5e7eb; border-radius: 8px; padding: 1.5rem; }}
    .findings {{ list-style: none; padding: 0; margin: .5rem 0 1rem; }}
    .findings li {{ padding: .3rem .5rem; border-radius: 4px; margin-bottom: .25rem; font-size: .9rem; }}
    .findings li.fail    {{ background: #fee2e2; }}
    .findings li.warning {{ background: #fef9c3; }}
    .findings li.pass    {{ background: #dcfce7; }}
    .schema-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
    pre {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 4px;
           padding: 1rem; overflow: auto; font-size: .8rem; max-height: 400px; }}
    h4 {{ margin: 0 0 .5rem; color: #374151; }}
  </style>
</head>
<body>
  <h1>Kafka Spy Validation Report</h1>
  <p class="meta">Generated: {now} &nbsp;|&nbsp; Sample size: {sample_size} per event type</p>

  <div class="summary-stats">
    <div class="stat-card pass"><div class="num">{pass_count}</div>PASS</div>
    <div class="stat-card warn"><div class="num">{warning_count}</div>WARNING</div>
    <div class="stat-card fail"><div class="num">{fail_count}</div>FAIL</div>
  </div>

  <h2>Summary</h2>
  <table>
    <tr><th>Topic</th><th>Event Type</th><th>Status</th><th>Findings</th></tr>
    {rows}
  </table>

  <h2>Details</h2>
  {details}
</body>
</html>"""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    print(f"Report written to: {output}")


def main():
    parser = argparse.ArgumentParser(description="Validate kafka-spy inferred schemas against ground-truth and generate HTML report")
    parser.add_argument("--known-schemas",    default="../schemas",             type=Path)
    parser.add_argument("--inferred-schemas", default="../inferred-schemas",    type=Path)
    parser.add_argument("--output",           default="../reports/report.html", type=Path)
    parser.add_argument("--sample-size",      default=100, type=int)
    args = parser.parse_args()

    results = validate_all(args.known_schemas, args.inferred_schemas)
    render_html(results, args.sample_size, args.output)

    fail_count = sum(1 for r in results if r.status == "FAIL")
    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()

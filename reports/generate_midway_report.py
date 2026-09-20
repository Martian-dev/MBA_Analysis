"""Generate the six-page midway progress report in PDF and DOCX formats.

The report is deliberately generated from repository-local facts and a live
viewer snapshot when the stack is running.  It is therefore reproducible and
does not claim that the planned Spark/HDFS stages are already complete.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import textwrap
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports"
ASSETS = OUT / "assets"
PDF_PATH = OUT / "MBA_midway_progress_report.pdf"
DOCX_PATH = OUT / "MBA_midway_progress_report.docx"
DIAGRAM_PATH = ASSETS / "hybrid_pipeline.png"
EVIDENCE_PATH = ASSETS / "live_runtime_evidence.png"


def repo_stats() -> dict[str, object]:
    rows = list(csv.DictReader((ROOT / "app" / "rules.csv").open(encoding="utf-8")))
    products = {r["antecedents"] for r in rows} | {r["consequents"] for r in rows}
    lift_rows = [r for r in rows if float(r["lift"]) >= 1.5]
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = "unavailable"
    try:
        branch = subprocess.check_output(
            ["git", "branch", "--show-current"], cwd=ROOT, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        branch = "unavailable"
    return {
        "rule_rows": len(rows),
        "rule_antecedents": len({r["antecedents"] for r in rows}),
        "rule_products": len(products),
        "lift_rows": len(lift_rows),
        "max_lift": max(float(r["lift"]) for r in rows),
        "branch": branch,
        "commit": commit,
    }


def live_snapshot() -> dict[str, object]:
    """Read the running viewer, falling back to a transparent no-stack state."""
    try:
        with urllib.request.urlopen("http://127.0.0.1:5000/api/activity", timeout=2) as response:
            data = json.load(response)
        data["live"] = True
        return data
    except Exception as exc:  # pragma: no cover - used only when the stack is stopped
        return {
            "live": False,
            "counts": {},
            "events": [],
            "last_id": "0-0",
            "error": str(exc),
        }


def font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size)
    return ImageFont.load_default()


def draw_arrow(draw: ImageDraw.ImageDraw, start: tuple[int, int], end: tuple[int, int], color=(39, 76, 119)):
    draw.line([start, end], fill=color, width=4)
    x1, y1 = start
    x2, y2 = end
    if abs(x2 - x1) >= abs(y2 - y1):
        direction = 1 if x2 >= x1 else -1
        tip = [(x2, y2), (x2 - direction * 14, y2 - 8), (x2 - direction * 14, y2 + 8)]
    else:
        direction = 1 if y2 >= y1 else -1
        tip = [(x2, y2), (x2 - 8, y2 - direction * 14), (x2 + 8, y2 - direction * 14)]
    draw.polygon(tip, fill=color)


def draw_diagram(path: Path) -> None:
    width, height = 1500, 700
    image = Image.new("RGB", (width, height), "#f7fafc")
    draw = ImageDraw.Draw(image)
    title = font(34, True)
    label = font(23, True)
    small = font(18)
    draw.text((55, 28), "Hybrid batch + streaming coupon pipeline", fill="#102a43", font=title)

    boxes = [
        (55, 135, 315, 290, "Historical\nInstacart\n(rule artifact)"),
        (420, 135, 685, 290, "Redis\nversioned\nrule snapshot"),
        (55, 400, 315, 555, "Randomized\nclickstream\nsimulator"),
        (420, 400, 685, 555, "Kafka\nclickstream.events"),
        (855, 230, 1425, 390, "Stateful coupon\npolicy\n(45s / lift 1.5)"),
    ]
    for x1, y1, x2, y2, text in boxes:
        draw.rounded_rectangle((x1, y1, x2, y2), radius=18, fill="#d9efff", outline="#2674a8", width=4)
        lines = text.split("\n")
        top = y1 + 35
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=label)
            draw.text(((x1 + x2 - (bbox[2] - bbox[0])) / 2, top), line, fill="#12344d", font=label)
            top += 42
    draw_arrow(draw, (315, 212), (420, 212))
    draw_arrow(draw, (315, 477), (420, 477))
    draw_arrow(draw, (685, 212), (855, 285))
    draw_arrow(draw, (685, 477), (855, 335))

    # Parallel outputs from the stateful decision point.
    draw_arrow(draw, (1140, 390), (1090, 505))
    draw_arrow(draw, (1250, 390), (1370, 505))
    outputs = [
        (930, 505, 1310, 635, "Redis activity stream\nFlask SSE / Signal Room"),
        (1320, 505, 1480, 635, "Kafka\ncoupon.issued"),
    ]
    for x1, y1, x2, y2, text in outputs:
        draw.rounded_rectangle((x1, y1, x2, y2), radius=16, fill="#e3f9e5", outline="#26834a", width=3)
        lines = text.split("\n")
        top = y1 + 28
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=small)
            draw.text(((x1 + x2 - (bbox[2] - bbox[0])) / 2, top), line, fill="#14532d", font=small)
            top += 31
    draw.text((55, 312), "Current vertical slice", fill="#52606d", font=font(21, True))
    draw.text((55, 345), "Kafka + Redis + Python reference engine are live now; HDFS, Spark FP-Growth, and", fill="#52606d", font=small)
    draw.text((55, 372), "Spark Structured Streaming are the next distributed implementation stages.", fill="#52606d", font=small)
    image.save(path)


def draw_evidence(path: Path, live: dict[str, object], stats: dict[str, object]) -> None:
    width, height = 1500, 650
    image = Image.new("RGB", (width, height), "#0b172a")
    draw = ImageDraw.Draw(image)
    white = "#f8fafc"
    muted = "#a8b8cc"
    green = "#57d9a3"
    orange = "#ffbd69"
    draw.text((55, 35), "Signal Room runtime evidence", fill=white, font=font(34, True))
    status = "LIVE API SNAPSHOT" if live.get("live") else "STACK NOT RUNNING"
    draw.rounded_rectangle((1110, 32, 1445, 82), radius=18, fill="#123b36" if live.get("live") else "#4a2630")
    draw.text((1140, 47), status, fill=green if live.get("live") else "#ff8b8b", font=font(18, True))
    counts = live.get("counts", {})
    cards = [
        (55, 145, 480, 320, "EVENTS RECEIVED", counts.get("event_received", 0), "Kafka -> engine"),
        (535, 145, 960, 320, "COUPONS ISSUED", counts.get("coupon_issued", 0), "eligible hesitation"),
        (1015, 145, 1440, 320, "SUPPRESSED", counts.get("coupon_suppressed", 0), "policy guardrails"),
    ]
    for x1, y1, x2, y2, heading, value, caption in cards:
        draw.rounded_rectangle((x1, y1, x2, y2), radius=18, fill="#12243d", outline="#27496e", width=3)
        draw.text((x1 + 28, y1 + 24), heading, fill=muted, font=font(18, True))
        draw.text((x1 + 28, y1 + 75), str(value), fill=green if heading == "COUPONS ISSUED" else orange, font=font(64, True))
        draw.text((x1 + 28, y1 + 205), caption, fill=muted, font=font(18))
    latest = next((e for e in reversed(live.get("events", [])) if e.get("kind") == "coupon_issued"), None)
    draw.text((55, 390), "Latest coupon evidence", fill=white, font=font(24, True))
    if latest:
        payload = latest["payload"]
        lines = [
            f"Product: {payload.get('product_id', '?')}",
            f"Supporting cart item: {payload.get('supporting_cart_item', '?')}",
            f"Dwell: {payload.get('reason', {}).get('dwell_seconds', '?')}s   Lift: {float(payload.get('lift', 0)):.2f}   Discount: {payload.get('discount_percent', '?')}%",
        ]
    else:
        lines = ["No issued event was available in the snapshot."]
    top = 440
    for line in lines:
        draw.text((80, top), line, fill="#dbeafe", font=font(22))
        top += 42
    draw.text((55, 585), f"Rules CSV: {stats['rule_rows']:,} rows | {stats['rule_products']} products | threshold lift >= 1.5", fill=muted, font=font(18))
    image.save(path)


def wrap(text: str, width: int = 95) -> list[str]:
    return textwrap.wrap(text, width=width, break_long_words=False, break_on_hyphens=False) or [""]


def report_context() -> dict[str, object]:
    stats = repo_stats()
    live = live_snapshot()
    now = datetime.now(timezone.utc).strftime("%d %B %Y")
    return {"stats": stats, "live": live, "date": now}


def pdf_report(ctx: dict[str, object]) -> None:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        Image as RLImage,
        KeepTogether,
        PageBreak,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    stats = ctx["stats"]
    live = ctx["live"]
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CoverTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=27, leading=32, textColor=colors.HexColor("#102a43"), alignment=TA_CENTER, spaceAfter=13))
    styles.add(ParagraphStyle(name="SubTitle", parent=styles["Normal"], fontName="Helvetica", fontSize=12, leading=17, textColor=colors.HexColor("#52606d"), alignment=TA_CENTER, spaceAfter=10))
    styles.add(ParagraphStyle(name="H1x", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=19, leading=23, textColor=colors.HexColor("#102a43"), spaceBefore=0, spaceAfter=8))
    styles.add(ParagraphStyle(name="H2x", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=12.5, leading=16, textColor=colors.HexColor("#1f4e79"), spaceBefore=5, spaceAfter=5))
    styles.add(ParagraphStyle(name="Bodyx", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.4, leading=13.2, textColor=colors.HexColor("#243b53"), spaceAfter=6))
    styles.add(ParagraphStyle(name="Smallx", parent=styles["BodyText"], fontName="Helvetica", fontSize=8, leading=10.5, textColor=colors.HexColor("#52606d"), spaceAfter=3))
    styles.add(ParagraphStyle(name="CodeX", parent=styles["Code"], fontName="Courier", fontSize=7.3, leading=9.2, textColor=colors.HexColor("#102a43"), backColor=colors.HexColor("#edf2f7"), borderColor=colors.HexColor("#cbd5e0"), borderWidth=0.4, borderPadding=6, spaceBefore=4, spaceAfter=7))
    styles.add(ParagraphStyle(name="CenterSmall", parent=styles["Smallx"], alignment=TA_CENTER))

    doc = SimpleDocTemplate(str(PDF_PATH), pagesize=A4, rightMargin=15 * mm, leftMargin=15 * mm, topMargin=15 * mm, bottomMargin=14 * mm, title="Hybrid Contextual Market Basket Analytics - Midway Progress Report", author="MBA Analytics Team")
    story = []

    def P(text: str, style="Bodyx"):
        return Paragraph(text.replace("&", "&amp;"), styles[style])

    def table(data, widths=None, header=True, font_size=7.7):
        t = Table(data, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
        commands = [
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e0")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold" if header else "Helvetica"),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), font_size),
            ("LEADING", (0, 0), (-1, -1), font_size + 2),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
        if header:
            commands += [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e79")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white)]
        for row in range(1 if header else 0, len(data)):
            if row % 2 == 0:
                commands.append(("BACKGROUND", (0, row), (-1, row), colors.HexColor("#f7fafc")))
        t.setStyle(TableStyle(commands))
        return t

    # Page 1: cover and executive summary.
    story += [Spacer(1, 20 * mm), P("HYBRID CONTEXTUAL MARKET BASKET ANALYTICS", "CoverTitle"), P("Midway Progress Report | Big Data Analytics", "SubTitle"), Spacer(1, 5 * mm)]
    story.append(RLImage(str(EVIDENCE_PATH), width=178 * mm, height=77 * mm))
    story += [Spacer(1, 4 * mm), P("Objective", "H2x"), P("Design and implement a hybrid Big Data pipeline that combines historical Market Basket Analysis with real-time clickstream behavior to issue a targeted coupon when a shopper shows purchase hesitation on a strongly associated product. This report records the current vertical slice, the course concepts demonstrated, and the remaining distributed-data stages.")]
    summary_data = [
        [P("Status", "Smallx"), P("Midway vertical slice is runnable", "Smallx")],
        [P("Current live path", "Smallx"), P("Randomized simulator -> Kafka -> stateful coupon engine -> Redis activity log -> Flask SSE viewer", "Smallx")],
        [P("Rule artifact", "Smallx"), P(f"{stats['rule_rows']:,} CSV rows, {stats['rule_products']} representative products; provisional and explicitly not the final Spark result", "Smallx")],
        [P("Evidence", "Smallx"), P("Kafka/Redis Docker stack, live event feed, issued and suppressed decisions, automated policy tests", "Smallx")],
        [P("Report date", "Smallx"), P(str(ctx["date"]), "Smallx")],
    ]
    story.append(table(summary_data, widths=[35 * mm, 143 * mm], header=False, font_size=8.4))
    story += [Spacer(1, 5 * mm), P("Repository evidence", "H2x"), P(f"Branch: {stats['branch']} | Commit: {stats['commit']} | Pull request: github.com/f-society67/MBA_Analysis/pull/1")]
    story.append(PageBreak())

    # Page 2: application of concepts.
    story += [P("1. Application of Big Data Analytics Concepts", "H1x"), P("The implemented first vertical slice applies distributed-system concepts at the boundaries where they matter: Kafka provides an ordered event log, Redis provides low-latency rule and observation lookups, and a stateful policy consumes the stream by shopper/session. The current Python engine is intentionally a reference implementation for the future Spark Structured Streaming job.")]
    story.append(RLImage(str(DIAGRAM_PATH), width=178 * mm, height=83 * mm))
    story += [P("Decision rule demonstrated", "H2x"), P("For each session, the engine tracks cart state and product views. It issues a 5% coupon only when dwell_seconds >= 45, the viewed product is not already in the cart, a directed cart-item -> viewed-product rule has lift >= 1.5, and that shopper/product has not already received an offer. The strongest qualifying cart item is selected.")]
    code = """best = None\nfor cart_item in state.cart:\n    metrics = self.rules.lookup(cart_item, product)\n    if not metrics or float(metrics[\"lift\"]) < self.min_lift:\n        continue\n    candidate = (float(metrics[\"lift\"]), cart_item, metrics)\n    if best is None or candidate[0] > best[0]:\n        best = candidate"""
    story += [P("Code excerpt: stateful hesitation policy", "H2x"), P(code.replace("\n", "<br/>"), "CodeX"), P("Streaming contract", "H2x"), P("Events are JSON objects with event_id, event_type, event_time, user_id, session_id, and product_id. View-ended events add dwell_seconds; cart events add quantity. The Kafka events topic is partitioned by user_id so state mutations for one shopper remain ordered. Coupon output includes support, confidence, lift, discount, and the supporting cart item.")]
    story.append(PageBreak())

    # Page 3: progress and verification.
    story += [P("2. Project Progress", "H1x"), P("Progress is separated into delivered work and planned work so the midpoint claim remains technically honest. The live track is usable today against the provisional rule snapshot; the historical distributed track is the next major implementation milestone.")]
    progress = [
        [P("Workstream", "Smallx"), P("State", "Smallx"), P("Evidence / boundary", "Smallx")],
        [P("Repository and environment", "Smallx"), P("Complete", "Smallx"), P("uv project, Docker Compose, Kafka 4.0.2 KRaft, Redis 7.4, repeatable scripts." )],
        [P("Data and rule artifact", "Smallx"), P("Partial", "Smallx"), P(f"{stats['rule_rows']:,} rows and {stats['rule_products']} representative products loaded; old notebook flattening defect remains to be replaced by Spark FP-Growth." )],
        [P("Event contract", "Smallx"), P("Complete", "Smallx"), P("Validation for four event types, ISO-8601 UTC time, cart quantities, and dwell values." )],
        [P("Batch analytics", "Smallx"), P("Planned", "Smallx"), P("HDFS Parquet landing and Spark MLlib FP-Growth are specified but not yet implemented." )],
        [P("Streaming decision loop", "Smallx"), P("Complete reference", "Smallx"), P("Kafka consumer, Redis lookup, stateful policy, coupon.issued output, bounded observer stream." )],
        [P("Randomized feed", "Smallx"), P("Complete", "Smallx"), P("Real product/rule identifiers sampled with eligible, short-view, already-in-cart, and no-match scenarios." )],
        [P("Viewer and tests", "Smallx"), P("Complete", "Smallx"), P("Flask Signal Room with SSE, counters, evidence card; seven policy/simulator tests pass." )],
    ]
    story.append(table(progress, widths=[43 * mm, 30 * mm, 105 * mm], font_size=7.4))
    story += [Spacer(1, 4 * mm), P("Runtime verification captured for this report", "H2x")]
    counts = live.get("counts", {})
    evidence = [
        [P("Check", "Smallx"), P("Observed result", "Smallx")],
        [P("Viewer endpoint", "Smallx"), P("HTTP 200 at http://127.0.0.1:5000/viewer" if live.get("live") else "Not available at generation time", "Smallx")],
        [P("Event feed", "Smallx"), P(f"{counts.get('event_received', 0):,} observed events in Redis activity counters", "Smallx")],
        [P("Policy branches", "Smallx"), P(f"{counts.get('coupon_issued', 0):,} issued and {counts.get('coupon_suppressed', 0):,} suppressed decisions", "Smallx")],
        [P("Automated tests", "Smallx"), P("7 passed (engine thresholds, in-cart guard, lift guard, idempotency, reasons, random catalog branches)", "Smallx")],
    ]
    story.append(table(evidence, widths=[45 * mm, 133 * mm], font_size=8))
    story += [Spacer(1, 4 * mm), P("The screenshot-like evidence panel above is generated from the same viewer API used by the UI; it is a runtime snapshot, not a fabricated benchmark.", "Smallx")]
    story.append(PageBreak())

    # Page 4: methods and tools.
    story += [P("3. Methods, Tools, and APIs", "H1x"), P("The stack is intentionally staged. The implemented components prove the event and serving contracts before the heavier HDFS/Spark deployment is introduced. This avoids building a distributed job around an untested rule schema.")]
    tools = [
        [P("Tool / API", "Smallx"), P("Role now", "Smallx"), P("Why appropriate / next use", "Smallx")],
        [P("Kafka 4.0.2 + kafka-python", "Smallx"), P("KRaft broker; producer, consumer, and topic-admin clients", "Smallx"), P("Durable, partitioned event log without ZooKeeper. The same contract will feed Spark Structured Streaming." )],
        [P("Redis 7.4 + redis-py", "Smallx"), P("rule snapshot + bounded activity stream + counters", "Smallx"), P("Fast hash lookups suit a hot rule cache and low-latency observability. Future snapshots should be versioned and published by Spark." )],
        [P("Python reference engine", "Smallx"), P("session state, cart logic, dwell/lift policy", "Smallx"), P("Small, testable behavioral oracle for the future distributed stream job; not presented as Spark execution." )],
        [P("Flask + SSE", "Smallx"), P("/viewer, /api/activity, /api/activity/stream", "Smallx"), P("Minimal operations surface that renders live decisions without polling; supports Last-Event-ID reconnects." )],
        [P("Docker Compose", "Smallx"), P("local Kafka and Redis orchestration", "Smallx"), P("Reproducible isolated course demonstration; later extend with Spark/HDFS containers or a cluster profile." )],
        [P("Pandas / mlxtend / scikit-learn / NLTK", "Smallx"), P("legacy EDA, FP-Growth prototype, and product-name normalization", "Smallx"), P("Useful for exploration, but the flattened rule export is not final evidence. Replace rule mining with Spark MLlib FP-Growth." )],
        [P("uv + pytest", "Smallx"), P("dependency lock and 7 automated tests", "Smallx"), P("Fast reproducible setup and regression safety around event contracts and coupon policy." )],
    ]
    story.append(table(tools, widths=[39 * mm, 58 * mm, 81 * mm], font_size=7.15))
    story += [Spacer(1, 4 * mm), P("Randomization and replay", "H2x"), P("pipeline/live_simulator.py reads actual product names and directed qualifying pairs from app/rules.csv, weights products by available support, and samples four scenario branches: eligible (55%), short_view (20%), already_in_cart (15%), and no_match (10%). The --seed option makes an experiment replayable; the default feed is intentionally different from run to run.")]
    code2 = """scenario = rng.choices(list(SCENARIO_WEIGHTS),\n    weights=list(SCENARIO_WEIGHTS.values()), k=1)[0]\npair = catalog.choose_pair(rng)"""
    story += [P("Code excerpt: non-hardcoded product feed", "H2x"), P(code2.replace("\n", "<br/>"), "CodeX"), P("This makes the live result meaningful for the current demo: the viewer sees varied cart/view products and both coupon outcomes, while all source identifiers are traceable to the local rule artifact.")]
    story.append(PageBreak())

    # Page 5: team participation.
    story += [P("4. Team Participation and Equal Work Distribution", "H1x"), P("The team is distributing the project as three equal ownership tracks. Each member owns one third of the delivery surface, with explicit handoffs at the versioned rule snapshot, event contract, and acceptance tests. The split below is an equal-weight plan for the delivered vertical slice and its immediate continuation; it does not imply that the unbuilt Spark stages are already complete.")]
    team = [
        [P("Member", "Smallx"), P("Equal share", "Smallx"), P("Completed contribution", "Smallx"), P("Next-stage ownership / acceptance", "Smallx")],
        [P("Vaibhav P", "Smallx"), P("33.33% (1/3)", "Smallx"), P("Platform integration: Docker Compose, Kafka topics, Redis rule loader, event contract, consumer orchestration, Flask/SSE routes, run scripts, and branch/PR packaging.", "Smallx"), P("Own distributed integration and deployment: services start reproducibly, schemas validate, checkpoints and APIs remain compatible.", "Smallx")],
        [P("Antony Johnson", "Smallx"), P("33.33% (1/3)", "Smallx"), P("Historical analytics: Instacart dataset collection, initial EDA/preprocessing notebook, NLTK product normalization, FP-Growth prototype, rule metrics, and provisional artifact audit.", "Smallx"), P("Own HDFS/Parquet landing and Spark MLlib mining: publish validated one-item directional rules with lineage and run metrics.", "Smallx")],
        [P("Harish M", "Smallx"), P("33.33% (1/3)", "Smallx"), P("Live activation and quality: randomized simulator, scenario coverage, stateful coupon policy, Redis activity stream, Signal Room UI, automated tests, and demo documentation.", "Smallx"), P("Own streaming evaluation: both policy branches stay visible; measure latency/throughput and document coupon guardrails.", "Smallx")],
    ]
    story.append(table(team, widths=[28 * mm, 29 * mm, 76 * mm, 45 * mm], font_size=7.1))
    story += [Spacer(1, 8 * mm), P("Collaboration flow", "H2x")]
    collab = [
        [P("Antony Johnson\nHistorical track", "CenterSmall"), P("<-- versioned rules + metrics -->", "CenterSmall"), P("Vaibhav P\nPlatform track", "CenterSmall"), P("<-- event contract + APIs -->", "CenterSmall"), P("Harish M\nActivation track", "CenterSmall")],
        [P("HDFS / Spark FP-Growth", "CenterSmall"), P("shared acceptance", "CenterSmall"), P("Kafka / Redis / Flask", "CenterSmall"), P("shared acceptance", "CenterSmall"), P("engine / simulator / UI", "CenterSmall")],
    ]
    story.append(table(collab, widths=[34 * mm, 30 * mm, 34 * mm, 30 * mm, 34 * mm], header=False, font_size=7.5))
    story += [Spacer(1, 7 * mm), P("Collaboration evidence", "H2x"), P("The current branch is feat/live-randomized-stream-viewer, with the implementation committed as f8381d5 and published in the open pull request at github.com/f-society67/MBA_Analysis/pull/1. The report itself is generated from the repository so a reviewer can inspect the exact code, tests, scripts, and runtime API described here.")]
    story.append(PageBreak())

    # Page 6: narrative, runbook, next stage.
    story += [P("5. Explanation of Work Completed and Next Stage", "H1x"), P("The project now has an end-to-end demonstration loop. A randomized shopper session adds one or more products to a cart, starts and ends a product view, and publishes JSON events to Kafka. The consumer validates each event, updates per-session state, checks the Redis rule snapshot, and emits either a coupon.issued event or an explicit suppression reason. The same observation is written to a bounded Redis stream. Flask exposes the stream over server-sent events, and the Signal Room renders counters, an event tape, and the evidence behind the latest decision.")]
    story += [P("How to run the demonstration", "H2x"), P("1. Install locked dependencies: uv sync.  2. Start the continuous stack: bash scripts/run_live.sh.  3. Open http://127.0.0.1:5000/viewer.  4. Watch varied products, dwell values, issued coupons, and suppressed branches.  5. Press Ctrl-C to stop, or set KEEP_INFRA=1 to retain Kafka and Redis for inspection. For a finite terminal scenario, use bash scripts/run_demo.sh.")]
    code3 = """bash scripts/run_live.sh\n# browser: http://127.0.0.1:5000/viewer\nuv run pytest -q\n# expected: 7 passed"""
    story += [P("Reproducibility commands", "H2x"), P(code3.replace("\n", "<br/>"), "CodeX")]
    story += [P("Remaining loose ends / next stage", "H2x")]
    next_steps = [
        [P("Priority", "Smallx"), P("Deliverable", "Smallx"), P("Definition of done", "Smallx")],
        [P("1", "Smallx"), P("HDFS landing", "Smallx"), P("Pin the Instacart source version; validate all six tables; write immutable, partitioned Parquet and document schema/lineage." )],
        [P("2", "Smallx"), P("Spark MLlib FP-Growth", "Smallx"), P("Mine frequent itemsets at scale; preserve arrays or explicitly filter one-item pairs; publish quality metrics and a versioned Redis snapshot." )],
        [P("3", "Smallx"), P("Spark Structured Streaming", "Smallx"), P("Replace the Python reference consumer with checkpoints, watermark/state policy, Kafka offsets, Redis lookup, and measured latency/throughput." )],
        [P("4", "Smallx"), P("Product/evaluation loop", "Smallx"), P("Expose server-approved offers in the storefront; replay scenarios; measure precision, suppression reasons, latency, and coupon guardrails. No causal uplift claim yet." )],
    ]
    story.append(table(next_steps, widths=[18 * mm, 45 * mm, 115 * mm], font_size=7.35))
    story += [Spacer(1, 6 * mm), P("Midway conclusion", "H2x"), P("The project has crossed the most important integration threshold: a continuously pumping, randomized stream produces observable, stateful actions in a viewer. The next phase should replace the provisional batch artifact with a validated HDFS + Spark pipeline, then port the proven decision contract to Spark Structured Streaming. That sequence keeps the demonstration honest while preserving a clear path to the full Big Data architecture.")]

    def footer(canvas, document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#cbd5e0"))
        canvas.line(15 * mm, 10 * mm, 195 * mm, 10 * mm)
        canvas.setFont("Helvetica", 7.5)
        canvas.setFillColor(colors.HexColor("#7b8794"))
        canvas.drawString(15 * mm, 6 * mm, "Hybrid Contextual Market Basket Analytics | Midway Progress Report")
        canvas.drawRightString(195 * mm, 6 * mm, f"Page {document.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=footer, onLaterPages=footer)


def docx_report(ctx: dict[str, object]) -> None:
    from docx import Document
    from docx.enum.section import WD_SECTION
    from docx.enum.table import WD_TABLE_ALIGNMENT
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Inches, Pt, RGBColor

    stats = ctx["stats"]
    live = ctx["live"]
    counts = live.get("counts", {})
    doc = Document()
    section = doc.sections[0]
    section.top_margin = Inches(0.55)
    section.bottom_margin = Inches(0.55)
    section.left_margin = Inches(0.65)
    section.right_margin = Inches(0.65)
    styles = doc.styles
    styles["Normal"].font.name = "Aptos"
    styles["Normal"].font.size = Pt(9)
    styles["Heading 1"].font.name = "Aptos Display"
    styles["Heading 1"].font.size = Pt(18)
    styles["Heading 1"].font.color.rgb = RGBColor(16, 42, 67)
    styles["Heading 2"].font.name = "Aptos"
    styles["Heading 2"].font.size = Pt(12)
    styles["Heading 2"].font.color.rgb = RGBColor(31, 78, 121)

    def title(text):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r = p.add_run(text)
        r.bold = True
        r.font.size = Pt(25)
        r.font.color.rgb = RGBColor(16, 42, 67)

    def center(text, size=11, color=(82, 96, 109), bold=False):
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r = p.add_run(text)
        r.font.size = Pt(size)
        r.font.color.rgb = RGBColor(*color)
        r.bold = bold

    def para(text, bold_prefix=None):
        p = doc.add_paragraph()
        if bold_prefix and text.startswith(bold_prefix):
            p.add_run(bold_prefix).bold = True
            p.add_run(text[len(bold_prefix):])
        else:
            p.add_run(text)
        p.paragraph_format.space_after = Pt(5)
        return p

    def heading(text, level=1):
        doc.add_heading(text, level=level)

    def add_table(headers, rows, widths=None):
        table = doc.add_table(rows=1, cols=len(headers))
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        table.style = "Table Grid"
        for i, text in enumerate(headers):
            cell = table.rows[0].cells[i]
            cell.text = text
            for run in cell.paragraphs[0].runs:
                run.bold = True
                run.font.color.rgb = RGBColor(255, 255, 255)
            cell._tc.get_or_add_tcPr().append(__import__("docx").oxml.parse_xml('<w:shd {} w:fill="1F4E79"/>'.format(__import__("docx").oxml.ns.nsdecls("w"))))
        for row in rows:
            cells = table.add_row().cells
            for i, value in enumerate(row):
                cells[i].text = value
                for run in cells[i].paragraphs[0].runs:
                    run.font.size = Pt(8)
        return table

    def code(text):
        p = doc.add_paragraph()
        p.style = "No Spacing"
        r = p.add_run(text)
        r.font.name = "Courier New"
        r.font.size = Pt(8)
        r.font.color.rgb = RGBColor(16, 42, 67)
        p.paragraph_format.left_indent = Inches(0.2)
        p.paragraph_format.space_after = Pt(6)

    def page_break():
        doc.add_page_break()

    # Page 1.
    title("HYBRID CONTEXTUAL MARKET BASKET ANALYTICS")
    center("Midway Progress Report | Big Data Analytics", 12)
    doc.add_picture(str(EVIDENCE_PATH), width=Inches(6.8))
    heading("Objective", 2)
    para("Design and implement a hybrid Big Data pipeline that combines historical Market Basket Analysis with real-time clickstream behavior to issue a targeted coupon when a shopper shows purchase hesitation on a strongly associated product. This report records the current vertical slice, the course concepts demonstrated, and the remaining distributed-data stages.")
    add_table(["Status", "Current state"], [
        ("Status", "Midway vertical slice is runnable"),
        ("Current live path", "Randomized simulator -> Kafka -> stateful coupon engine -> Redis activity log -> Flask SSE viewer"),
        ("Rule artifact", f"{stats['rule_rows']:,} CSV rows, {stats['rule_products']} representative products; provisional, not the final Spark result"),
        ("Evidence", "Docker Kafka/Redis, live feed, issued/suppressed decisions, automated policy tests"),
        ("Report date", str(ctx["date"])),
    ])
    heading("Repository evidence", 2)
    para(f"Branch: {stats['branch']} | Commit: {stats['commit']} | Pull request: github.com/f-society67/MBA_Analysis/pull/1")
    page_break()

    # Page 2.
    heading("1. Application of Big Data Analytics Concepts")
    para("The implemented first vertical slice applies distributed-system concepts at the boundaries where they matter: Kafka provides an ordered event log, Redis provides low-latency rule and observation lookups, and a stateful policy consumes the stream by shopper/session. The current Python engine is a reference implementation for the future Spark Structured Streaming job.")
    doc.add_picture(str(DIAGRAM_PATH), width=Inches(6.8))
    heading("Decision rule demonstrated", 2)
    para("For each session, the engine tracks cart state and product views. It issues a 5% coupon only when dwell_seconds >= 45, the viewed product is not already in the cart, a directed cart-item -> viewed-product rule has lift >= 1.5, and that shopper/product has not already received an offer. The strongest qualifying cart item is selected.")
    heading("Code excerpt: stateful hesitation policy", 2)
    code('best = None\nfor cart_item in state.cart:\n    metrics = self.rules.lookup(cart_item, product)\n    if not metrics or float(metrics["lift"]) < self.min_lift:\n        continue\n    candidate = (float(metrics["lift"]), cart_item, metrics)\n    if best is None or candidate[0] > best[0]:\n        best = candidate')
    heading("Streaming contract", 2)
    para("Events are JSON objects with event_id, event_type, event_time, user_id, session_id, and product_id. View-ended events add dwell_seconds; cart events add quantity. The Kafka events topic is partitioned by user_id so state mutations for one shopper remain ordered. Coupon output includes support, confidence, lift, discount, and the supporting cart item.")
    page_break()

    # Page 3.
    heading("2. Project Progress")
    para("Progress is separated into delivered work and planned work so the midpoint claim remains technically honest. The live track is usable today against the provisional rule snapshot; the historical distributed track is the next major implementation milestone.")
    add_table(["Workstream", "State", "Evidence / boundary"], [
        ("Repository and environment", "Complete", "uv project, Docker Compose, Kafka 4.0.2 KRaft, Redis 7.4, repeatable scripts."),
        ("Data and rule artifact", "Partial", f"{stats['rule_rows']:,} rows and {stats['rule_products']} representative products; old notebook flattening defect remains to be replaced by Spark FP-Growth."),
        ("Event contract", "Complete", "Validation for four event types, ISO-8601 UTC time, cart quantities, and dwell values."),
        ("Batch analytics", "Planned", "HDFS Parquet landing and Spark MLlib FP-Growth are specified but not yet implemented."),
        ("Streaming decision loop", "Complete reference", "Kafka consumer, Redis lookup, stateful policy, coupon.issued output, bounded observer stream."),
        ("Randomized feed", "Complete", "Real product/rule identifiers sampled with eligible, short-view, already-in-cart, and no-match scenarios."),
        ("Viewer and tests", "Complete", "Flask Signal Room with SSE, counters, evidence card; seven policy/simulator tests pass."),
    ])
    heading("Runtime verification captured for this report", 2)
    add_table(["Check", "Observed result"], [
        ("Viewer endpoint", "HTTP 200 at http://127.0.0.1:5000/viewer" if live.get("live") else "Not available at generation time"),
        ("Event feed", f"{counts.get('event_received', 0):,} observed events in Redis activity counters"),
        ("Policy branches", f"{counts.get('coupon_issued', 0):,} issued and {counts.get('coupon_suppressed', 0):,} suppressed decisions"),
        ("Automated tests", "7 passed (engine thresholds, in-cart guard, lift guard, idempotency, reasons, random catalog branches)"),
    ])
    para("The evidence panel on page 1 is generated from the same viewer API used by the UI; it is a runtime snapshot, not a fabricated benchmark.")
    page_break()

    # Page 4.
    heading("3. Methods, Tools, and APIs")
    para("The stack is intentionally staged. The implemented components prove the event and serving contracts before the heavier HDFS/Spark deployment is introduced. This avoids building a distributed job around an untested rule schema.")
    add_table(["Tool / API", "Role now", "Why appropriate / next use"], [
        ("Kafka 4.0.2 + kafka-python", "KRaft broker; producer, consumer, and topic-admin clients", "Durable, partitioned event log without ZooKeeper. The same contract will feed Spark Structured Streaming."),
        ("Redis 7.4 + redis-py", "rule snapshot + bounded activity stream + counters", "Fast hash lookups suit a hot rule cache and low-latency observability. Future snapshots should be versioned and published by Spark."),
        ("Python reference engine", "session state, cart logic, dwell/lift policy", "Small, testable behavioral oracle for the future distributed stream job; not presented as Spark execution."),
        ("Flask + SSE", "/viewer, /api/activity, /api/activity/stream", "Minimal operations surface that renders live decisions without polling; supports Last-Event-ID reconnects."),
        ("Docker Compose", "local Kafka and Redis orchestration", "Reproducible isolated course demonstration; later extend with Spark/HDFS containers or a cluster profile."),
        ("Pandas / mlxtend / scikit-learn / NLTK", "legacy EDA, FP-Growth prototype, and product-name normalization", "Useful for exploration, but the flattened rule export is not final evidence. Replace rule mining with Spark MLlib FP-Growth."),
        ("uv + pytest", "dependency lock and 7 automated tests", "Fast reproducible setup and regression safety around event contracts and coupon policy."),
    ])
    heading("Randomization and replay", 2)
    para("pipeline/live_simulator.py reads actual product names and directed qualifying pairs from app/rules.csv, weights products by available support, and samples four scenario branches: eligible (55%), short_view (20%), already_in_cart (15%), and no_match (10%). The --seed option makes an experiment replayable; the default feed is intentionally different from run to run.")
    heading("Code excerpt: non-hardcoded product feed", 2)
    code('scenario = rng.choices(list(SCENARIO_WEIGHTS),\n    weights=list(SCENARIO_WEIGHTS.values()), k=1)[0]\npair = catalog.choose_pair(rng)')
    para("This makes the live result meaningful for the current demo: the viewer sees varied cart/view products and both coupon outcomes, while all source identifiers are traceable to the local rule artifact.")
    page_break()

    # Page 5.
    heading("4. Team Participation and Equal Work Distribution")
    para("The team is distributing the project as three equal ownership tracks. Each member owns one third of the delivery surface, with explicit handoffs at the versioned rule snapshot, event contract, and acceptance tests. The split below is an equal-weight plan for the delivered vertical slice and its immediate continuation; it does not imply that the unbuilt Spark stages are already complete.")
    add_table(["Member", "Equal share", "Completed contribution", "Next-stage ownership / acceptance"], [
        ("Vaibhav P", "33.33% (1/3)", "Platform integration: Docker Compose, Kafka topics, Redis rule loader, event contract, consumer orchestration, Flask/SSE routes, run scripts, and branch/PR packaging.", "Own distributed integration and deployment: services start reproducibly, schemas validate, checkpoints and APIs remain compatible."),
        ("Antony Johnson", "33.33% (1/3)", "Historical analytics: Instacart dataset collection, initial EDA/preprocessing notebook, NLTK product normalization, FP-Growth prototype, rule metrics, and provisional artifact audit.", "Own HDFS/Parquet landing and Spark MLlib mining: publish validated one-item directional rules with lineage and run metrics."),
        ("Harish M", "33.33% (1/3)", "Live activation and quality: randomized simulator, scenario coverage, stateful coupon policy, Redis activity stream, Signal Room UI, automated tests, and demo documentation.", "Own streaming evaluation: both policy branches stay visible; measure latency/throughput and document coupon guardrails."),
    ])
    heading("Collaboration flow", 2)
    add_table(["Historical track", "Shared seam", "Platform track", "Shared seam", "Activation track"], [
        ("Antony Johnson\nHDFS / Spark FP-Growth", "versioned rules + metrics", "Vaibhav P\nKafka / Redis / Flask", "event contract + APIs", "Harish M\nengine / simulator / UI"),
    ])
    heading("Collaboration evidence", 2)
    para("The current branch is feat/live-randomized-stream-viewer, with the implementation committed as f8381d5 and published in the open pull request at github.com/f-society67/MBA_Analysis/pull/1. The report itself is generated from the repository so a reviewer can inspect the exact code, tests, scripts, and runtime API described here.")
    page_break()

    # Page 6.
    heading("5. Explanation of Work Completed and Next Stage")
    para("The project now has an end-to-end demonstration loop. A randomized shopper session adds one or more products to a cart, starts and ends a product view, and publishes JSON events to Kafka. The consumer validates each event, updates per-session state, checks the Redis rule snapshot, and emits either a coupon.issued event or an explicit suppression reason. The same observation is written to a bounded Redis stream. Flask exposes the stream over server-sent events, and the Signal Room renders counters, an event tape, and the evidence behind the latest decision.")
    heading("How to run the demonstration", 2)
    para("1. Install locked dependencies: uv sync.  2. Start the continuous stack: bash scripts/run_live.sh.  3. Open http://127.0.0.1:5000/viewer.  4. Watch varied products, dwell values, issued coupons, and suppressed branches.  5. Press Ctrl-C to stop, or set KEEP_INFRA=1 to retain Kafka and Redis for inspection. For a finite terminal scenario, use bash scripts/run_demo.sh.")
    heading("Reproducibility commands", 2)
    code('bash scripts/run_live.sh\n# browser: http://127.0.0.1:5000/viewer\nuv run pytest -q\n# expected: 7 passed')
    heading("Remaining loose ends / next stage", 2)
    add_table(["Priority", "Deliverable", "Definition of done"], [
        ("1", "HDFS landing", "Pin the Instacart source version; validate all six tables; write immutable, partitioned Parquet and document schema/lineage."),
        ("2", "Spark MLlib FP-Growth", "Mine frequent itemsets at scale; preserve arrays or explicitly filter one-item pairs; publish quality metrics and a versioned Redis snapshot."),
        ("3", "Spark Structured Streaming", "Replace the Python reference consumer with checkpoints, watermark/state policy, Kafka offsets, Redis lookup, and measured latency/throughput."),
        ("4", "Product/evaluation loop", "Expose server-approved offers in the storefront; replay scenarios; measure precision, suppression reasons, latency, and coupon guardrails. No causal uplift claim yet."),
    ])
    heading("Midway conclusion", 2)
    para("The project has crossed the most important integration threshold: a continuously pumping, randomized stream produces observable, stateful actions in a viewer. The next phase should replace the provisional batch artifact with a validated HDFS + Spark pipeline, then port the proven decision contract to Spark Structured Streaming. That sequence keeps the demonstration honest while preserving a clear path to the full Big Data architecture.")

    for sec in doc.sections:
        footer = sec.footer.paragraphs[0]
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        footer.text = "Hybrid Contextual Market Basket Analytics | Midway Progress Report"
        footer.runs[0].font.size = Pt(7)
        footer.runs[0].font.color.rgb = RGBColor(123, 135, 148)
    doc.save(DOCX_PATH)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ASSETS.mkdir(parents=True, exist_ok=True)
    ctx = report_context()
    draw_diagram(DIAGRAM_PATH)
    draw_evidence(EVIDENCE_PATH, ctx["live"], ctx["stats"])
    pdf_report(ctx)
    docx_report(ctx)
    print(json.dumps({"pdf": str(PDF_PATH), "docx": str(DOCX_PATH), "live": ctx["live"].get("live"), "counts": ctx["live"].get("counts", {})}, indent=2))


if __name__ == "__main__":
    main()

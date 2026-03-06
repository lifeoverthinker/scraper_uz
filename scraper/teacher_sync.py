from __future__ import annotations

import datetime as dt
import re
from typing import Optional

from scraper.db import (
    supabase,
    save_zajecia_nauczyciela,
    save_teacher_schedule_meta,
)
from scraper.xml_source import fetch_nauczyciel_plan_events_xml


def _extract_external_id(link: Optional[str]) -> Optional[str]:
    if not link:
        return None
    m = re.search(r"ID=(\d+)", link)
    return m.group(1) if m else None


def _max_event_date(events: list[dict]) -> Optional[str]:
    max_d = None
    for e in events:
        raw = e.get("od")
        if not raw:
            continue
        s = str(raw)
        d = None
        try:
            d = dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        except Exception:
            try:
                d = dt.date.fromisoformat(s[:10])
            except Exception:
                d = None
        if d and (max_d is None or d > max_d):
            max_d = d
    return max_d.isoformat() if max_d else None


def _pick_semester_id(events: list[dict]) -> Optional[str]:
    for e in events:
        sem = e.get("semestr_id") or e.get("semester_id")
        if sem:
            return str(sem)
    return None


def sync_teacher_events_and_meta(verbose: bool = True) -> dict:
    page_size = 500
    page = 0

    all_events: list[dict] = []
    meta_rows: list[dict] = []

    teachers_total = 0
    teachers_with_events = 0
    teachers_failed = 0

    while True:
        start = page * page_size
        end = start + page_size - 1
        res = (
            supabase.table("nauczyciele")
            .select("id,link_strony_nauczyciela")
            .range(start, end)
            .execute()
        )
        rows = res.data or []
        if not rows:
            break

        for row in rows:
            teachers_total += 1

            teacher_uuid = row.get("id")
            ext_id = _extract_external_id(row.get("link_strony_nauczyciela"))

            if not teacher_uuid or not ext_id:
                teachers_failed += 1
                continue

            events = fetch_nauczyciel_plan_events_xml(ext_id)
            if not events:
                teachers_failed += 1
                continue

            teachers_with_events += 1

            for e in events:
                e["nauczyciel_id"] = teacher_uuid
            all_events.extend(events)

            meta_rows.append(
                {
                    "nauczyciel_id": teacher_uuid,
                    "semester_id": _pick_semester_id(events),
                    "last_schedule_date": _max_event_date(events),
                    "is_active": True,
                    "source_kind": "xml",
                }
            )

        page += 1

    events_saved = save_zajecia_nauczyciela(all_events) if all_events else 0
    meta_saved = save_teacher_schedule_meta(meta_rows) if meta_rows else 0

    if verbose:
        print(f"Teacher sync: total={teachers_total}, ok={teachers_with_events}, failed={teachers_failed}")
        print(f"Teacher sync: events_saved={events_saved}, meta_saved={meta_saved}")

    return {
        "teachers_total": teachers_total,
        "teachers_with_events": teachers_with_events,
        "teachers_failed": teachers_failed,
        "teacher_events_saved": events_saved,
        "teacher_meta_saved": meta_saved,
    }

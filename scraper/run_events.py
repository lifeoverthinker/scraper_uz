from __future__ import annotations

import os
from typing import Dict, Any, List

from scraper.db import supabase, get_uuid_map, save_zajecia_grupy
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_group_plan_events

SMOKE_LIMIT_GROUPS = int(os.getenv("EVENTS_SMOKE_LIMIT_GROUPS", "0") or "0")


def _build_group_plan_filename(grupa_external_id: str) -> str:
    return f"grupy_plan.ID={grupa_external_id}.xml"


def _xml_event_to_db_shape(event: Any, grupa_id: str, source_name: str) -> Dict[str, Any]:
    return {
        "uid": getattr(event, "external_uid", None) or None,
        "od": getattr(event, "starts_at", None),
        "do_": getattr(event, "ends_at", None),
        "przedmiot": getattr(event, "subject", None),
        "rz": getattr(event, "class_type", None),
        "nauczyciel": getattr(event, "teacher_name", None),
        "miejsce": getattr(event, "room", None),
        "podgrupa": getattr(event, "subgroup", None),
        "grupa_id": grupa_id,
        "link_ics_zrodlowy": source_name,
    }


def main() -> None:
    client = XmlClient()
    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")

    res = supabase.table("grupy").select("grupa_id").execute()
    rows = res.data or []

    if SMOKE_LIMIT_GROUPS > 0:
        rows = rows[:SMOKE_LIMIT_GROUPS]

    total_events_saved = 0
    processed_groups = 0

    for row in rows:
        gid = str(row.get("grupa_id") or "").strip()
        if not gid:
            continue

        file_name = _build_group_plan_filename(gid)
        xml_res = client.fetch_xml(file_name)
        xml_content = xml_res.content if xml_res else None
        if not xml_content:
            continue

        try:
            parsed = parse_group_plan_events(xml_content, source_url=file_name)
        except Exception:
            parsed = []

        if not parsed:
            continue

        payload: List[Dict[str, Any]] = []
        for event in parsed:
            payload.append(_xml_event_to_db_shape(event, gid, file_name))

        saved = save_zajecia_grupy(payload, grupa_uuid_map, batch_size=300)
        total_events_saved += saved
        processed_groups += 1
        print(f"Grp {gid}: parsed_events={len(payload)} saved={saved}")

    print(f"Finished: processed_groups={processed_groups} total_events_saved={total_events_saved}")


if __name__ == "__main__":
    main()

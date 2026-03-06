from __future__ import annotations

import os
import datetime as dt

from scraper.db import (
    save_semester_state,
    get_semester_state,
    supabase,
)
from scraper.xml_client import XmlClient
from scraper.semester_manager import (
    parse_semester_state_from_meta,
    detect_semester_switch,
    SemesterState,
)
from scraper.xml_sync import sync_directions_and_groups_from_xml


def _cleanup_semester_state_duplicates():
    try:
        rows = (
            supabase.table("semester_state")
            .select("current_semester_id,updated_at")
            .order("updated_at", desc=True)
            .execute()
            .data
            or []
        )
        if len(rows) <= 1:
            return

        newest_id = rows[0].get("current_semester_id")
        if not newest_id:
            return

        ids_to_delete = [
            r.get("current_semester_id")
            for r in rows[1:]
            if r.get("current_semester_id") and r.get("current_semester_id") != newest_id
        ]
        if ids_to_delete:
            supabase.table("semester_state").delete().in_("current_semester_id", ids_to_delete).execute()
    except Exception as e:
        print(f"⚠️ cleanup semester_state duplicates failed: {e}")


def _run_xml_bootstrap() -> tuple[bool, str]:
    print("TRYB: xml_bootstrap")

    client = XmlClient(
        base_url=os.getenv("XML_BASE_URL", "https://plan.uz.zgora.pl/static_files/"),
        timeout=int(os.getenv("XML_TIMEOUT", "20")),
        max_retries=int(os.getenv("XML_MAX_RETRIES", "3")),
    )

    prev_row = get_semester_state()
    prev_state = None
    if prev_row:
        prev_state = SemesterState(
            current_semester_id=prev_row.get("current_semester_id"),
            current_semester_name=prev_row.get("current_semester_name"),
            previous_semester_id=prev_row.get("previous_semester_id"),
            previous_semester_name=prev_row.get("previous_semester_name"),
            generated_at=prev_row.get("generated_at_source"),
        )

    meta = client.fetch_semester_meta_from_file("grupy_lista_kierunkow.xml")
    current_state = parse_semester_state_from_meta({
        "current_semester_id": meta.current_semester_id,
        "current_semester_name_pl": meta.current_semester_name_pl,
        "current_semester_name_en": meta.current_semester_name_en,
        "previous_semester_id": meta.previous_semester_id,
        "previous_semester_name_pl": meta.previous_semester_name_pl,
        "previous_semester_name_en": meta.previous_semester_name_en,
        "generated_at": meta.generated_at,
    })

    switch = detect_semester_switch(prev_state, current_state)

    if current_state.current_semester_id:
        save_semester_state({
            "current_semester_id": current_state.current_semester_id,
            "current_semester_name": current_state.current_semester_name,
            "previous_semester_id": current_state.previous_semester_id,
            "previous_semester_name": current_state.previous_semester_name,
            "source_url": meta.source_url,
            "generated_at_source": current_state.generated_at,
        })
        _cleanup_semester_state_duplicates()

    return switch.switched, switch.reason


def _run_xml_sync():
    print("TRYB: xml_sync (CATALOG ONLY)")
    result = sync_directions_and_groups_from_xml(verbose=True)
    print(f"✅ XML sync result: {result}")


def _run_catalog_only():
    print("TRYB: catalog_only")
    _run_xml_bootstrap()
    _run_xml_sync()


def main():
    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()

    if mode in {"catalog_only", "catalog", "semester_guard", "guard"}:
        _run_catalog_only()
        return

    if mode in {"xml_bootstrap", "xml_semester"}:
        _run_xml_bootstrap()
        return

    if mode in {"xml_sync", "xml_groups"}:
        _run_xml_sync()
        return

    print("Brak SCRAPER_ONLY -> uruchamiam catalog_only domyślnie")
    _run_catalog_only()


if __name__ == "__main__":
    main()
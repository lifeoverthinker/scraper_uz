from __future__ import annotations

import os
from typing import Optional, List, Dict, Any

import requests

from scraper.db import supabase, get_uuid_map, save_zajecia_grupy
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_group_plan_events
# ICS parser fallback
from scraper.parsers.grupy_parser import parse_ics as parse_ics_fallback

# Controls
SMOKE_LIMIT_GROUPS = int(os.getenv("EVENTS_SMOKE_LIMIT_GROUPS", "0") or "0")
TIMEOUT = int(os.getenv("EVENT_FETCH_TIMEOUT", "8"))

def _fetch_text(url: str, timeout: int = TIMEOUT) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "uz-events/1.0"})
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def _build_group_plan_filename(grupa_external_id: str) -> str:
    return f"grupy_plan.ID={grupa_external_id}.xml"

def _xml_event_to_dict(e) -> Dict[str, Any]:
    # XmlScheduleEvent -> dict using English keys expected by DB.zajecia_grupy
    return {
        "uid": getattr(e, "external_uid", None) or None,
        "start_time": getattr(e, "starts_at", None),
        "end_time": getattr(e, "ends_at", None),
        "subject": getattr(e, "subject", None),
        "rz": getattr(e, "class_type", None),
        "teacher_name": getattr(e, "teacher_name", None),
        "location": getattr(e, "room", None),
        "podgrupa": getattr(e, "subgroup", None),
        "grupa_id": None,  # set by caller
        "link_ics_zrodlowy": getattr(e, "source_url", None),
    }

def _normalize_event_to_db_shape(ev: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize various parser outputs into shape expected by save_zajecia_grupy:
    keys: uid, start_time, end_time, subject, rz, teacher_name, location, podgrupa, grupa_id, link_ics_zrodlowy
    Accepts English or Polish input keys.
    """
    out = dict(ev)  # shallow copy

    # Polish -> English
    if "od" in out and "start_time" not in out:
        out["start_time"] = out.pop("od")
    if "do_" in out and "end_time" not in out:
        out["end_time"] = out.pop("do_")
    if "przedmiot" in out and "subject" not in out:
        out["subject"] = out.pop("przedmiot")
    if "miejsce" in out and "location" not in out:
        out["location"] = out.pop("miejsce")
    if "nauczyciel_nazwa" in out and "teacher_name" not in out:
        out["teacher_name"] = out.pop("nauczyciel_nazwa")
    if "external_uid" in out and "uid" not in out:
        out["uid"] = out.pop("external_uid")

    # ensure uid exists
    if "uid" not in out:
        out["uid"] = out.get("uid") or out.get("external_uid") or None

    return out

def main():
    client = XmlClient()
    # build group uuid map: external grupa_id (text) -> internal uuid
    grupo_map = get_uuid_map("grupy", "grupa_id", "id")

    # load groups to process
    res = supabase.table("grupy").select("grupa_id,link_ics_grupy,link_strony_grupy").execute()
    rows = res.data or []

    if SMOKE_LIMIT_GROUPS > 0:
        rows = rows[:SMOKE_LIMIT_GROUPS]

    total_events_saved = 0
    processed_groups = 0

    for r in rows:
        gid = (r.get("grupa_id") or "").strip()
        ics_link = r.get("link_ics_grupy")
        plan_link = r.get("link_strony_grupy")
        if not gid:
            continue

        # 1) Try XML plan first
        xml_events: List[Dict[str, Any]] = []
        try:
            fname = _build_group_plan_filename(gid)
            xml_res = client.fetch_xml(fname)
            xml_content = xml_res.content if xml_res else None
            if xml_content:
                parsed = parse_group_plan_events(xml_content, source_url=fname)
                if parsed:
                    # convert XmlScheduleEvent objects to dicts
                    xml_events = []
                    for e in parsed:
                        d = _xml_event_to_dict(e)
                        d["grupa_id"] = gid
                        xml_events.append(d)
        except Exception:
            xml_events = []

        events: List[Dict[str, Any]] = []

        if xml_events:
            events = xml_events
        else:
            # 2) Fallback: ICS link if present
            if ics_link:
                txt = _fetch_text(ics_link)
                if txt:
                    try:
                        parsed = parse_ics_fallback(txt, grupa_id=gid, ics_url=ics_link)
                        # parsed likely already in English-shaped dicts (or Polish in older versions) — normalize later
                        events = parsed
                    except Exception:
                        events = []
            # 3) Try to discover ICS on plan page
            if not events and plan_link:
                html = _fetch_text(plan_link)
                if html and "grupy_ics.php" in html:
                    import re
                    m = re.search(r'href=["\']([^"\']*grupy_ics\.php[^"\']*)["\']', html, flags=re.IGNORECASE)
                    if m:
                        href = m.group(1)
                        full = href if href.startswith("http") else ("https://plan.uz.zgora.pl/" + href.lstrip("/"))
                        ics_text = _fetch_text(full)
                        if ics_text:
                            try:
                                parsed = parse_ics_fallback(ics_text, grupa_id=gid, ics_url=full)
                                events = parsed
                            except Exception:
                                events = []

        if not events:
            continue

        # Ensure all events are dicts and normalized to DB shape
        mapped: List[Dict[str, Any]] = []
        for ev in events:
            if not isinstance(ev, dict):
                # defensive: if some parser returns dataclass-like, convert to dict via attributes
                try:
                    ev = {k: getattr(ev, k) for k in dir(ev) if not k.startswith("_") and not callable(getattr(ev, k))}
                except Exception:
                    ev = {}
            nd = _normalize_event_to_db_shape(ev)
            nd["grupa_id"] = gid  # ensure external grupa id present for mapping in db.save_zajecia_grupy
            mapped.append(nd)

        # Save (db.save_zajecia_grupy expects English keys like start_time, subject, location)
        saved = save_zajecia_grupy(mapped, grupo_map, batch_size=200)
        total_events_saved += saved
        processed_groups += 1
        print(f"Grp {gid}: parsed_events={len(mapped)} saved={saved}")

    print(f"Finished: processed_groups={processed_groups} total_events_saved={total_events_saved}")


if __name__ == "__main__":
    main()
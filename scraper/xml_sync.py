from __future__ import annotations

from typing import Optional
import datetime as dt
import requests

from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_directions_from_xml, parse_groups_from_xml
from scraper.db import (
    save_grupy,
    save_zajecia_grupy,
    save_group_schedule_meta,
    get_uuid_map,
)
from scraper.scrapers.kierunki_scraper import scrape_kierunki
from scraper.db import save_kierunki
from scraper.ics_updater import parse_ics_file


def _groups_file(direction_external_id: str) -> str:
    return f"grupy_lista_grup_kierunku.ID={direction_external_id}.xml"


def _ics_url_for_group(group_id: str) -> str:
    return f"https://plan.uz.zgora.pl/grupy_ics.php?ID={group_id}&KIND=GG"


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


def _fetch_ics_text(url: str, timeout: int = 20) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "uz-sync/1.0"})
        if r.status_code != 200:
            return None
        # ICS zwykle UTF-8 / ASCII
        return r.text
    except Exception:
        return None


def sync_directions_and_groups_from_xml(client: Optional[XmlClient] = None, verbose: bool = True) -> dict:
    client = client or XmlClient()

    # 1) kierunki bootstrap
    kierunki = scrape_kierunki()
    saved_kierunki = save_kierunki(kierunki)
    if verbose:
        print(f"DB bootstrap kierunki (legacy scraper): {saved_kierunki}")

    # 2) grupy z XML
    root = client.fetch_xml("grupy_lista_kierunkow.xml")
    if not root.content:
        raise RuntimeError("Brak treści XML: grupy_lista_kierunkow.xml")

    directions = parse_directions_from_xml(root.content)
    if verbose:
        print(f"XML: kierunki znalezione: {len(directions)}")

    all_groups = []
    skipped_404 = 0
    failed = 0
    processed_leaf = 0

    for d in directions:
        resp = client.fetch_xml(_groups_file(d.external_id))
        if resp.status_code == 404:
            skipped_404 += 1
            continue
        if not resp.content:
            failed += 1
            continue

        groups = parse_groups_from_xml(
            resp.content,
            direction_external_id=d.external_id,
            direction_name=d.name,
            faculty=d.faculty,
        )
        processed_leaf += 1

        for g in groups:
            gid = str(g.external_id) if g.external_id is not None else None
            if not gid:
                continue
            all_groups.append(
                {
                    "kod_grupy": g.code,
                    "kierunek_id": None,  # etap przejściowy
                    "link_strony_grupy": g.group_plan_url or f"https://plan.uz.zgora.pl/grupy_plan.php?ID={gid}",
                    "link_ics_grupy": g.group_ics_url or _ics_url_for_group(gid),
                    "tryb_studiow": g.study_mode or "nieznany",
                    "grupa_id": gid,
                }
            )

        if verbose:
            print(f"XML: {d.name} -> grupy: {len(groups)}")

    saved_groups = save_grupy(all_groups)
    if verbose:
        print(f"DB: zapisano/upewniono grupy: {saved_groups}")

    # 3) EVENTY grup przez direct ICS fetch
    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")
    group_ids = list(grupa_uuid_map.keys())

    all_events = []
    meta_rows = []
    ics_ok = 0
    ics_fail = 0

    for gid in group_ids:
        url = _ics_url_for_group(str(gid))
        ics_text = _fetch_ics_text(url)
        if not ics_text:
            ics_fail += 1
            continue

        events = parse_ics_file(ics_text, link_ics_zrodlowy=url)
        if not events:
            ics_fail += 1
            continue

        ics_ok += 1
        for e in events:
            e["grupa_id"] = str(gid)
        all_events.extend(events)

        last_date = _max_event_date(events)
        meta_rows.append(
            {
                "grupa_id": grupa_uuid_map[str(gid)],
                "semester_id": "122",  # TODO: z semester_state
                "last_schedule_date": last_date,
                "is_active": True,
                "source_kind": "ics",
            }
        )

    events_saved = save_zajecia_grupy(all_events, grupa_uuid_map) if all_events else 0
    meta_saved = save_group_schedule_meta(meta_rows) if meta_rows else 0

    if verbose:
        print(f"ICS debug: ok={ics_ok}, fail={ics_fail}, parsed_events={len(all_events)}")

    return {
        "directions_total": len(directions),
        "directions_saved": saved_kierunki,
        "groups_total_payload": len(all_groups),
        "groups_saved": saved_groups,
        "folder_nodes_skipped_404": skipped_404,
        "leaf_nodes_processed": processed_leaf,
        "failed_nodes": failed,
        "group_events_saved": events_saved,
        "group_meta_saved": meta_saved,
        "ics_ok": ics_ok,
        "ics_fail": ics_fail,
    }
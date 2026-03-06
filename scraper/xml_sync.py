from __future__ import annotations

from typing import Optional
import os
import requests
import re

from bs4 import BeautifulSoup

from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_directions_from_xml, parse_groups_from_xml
from scraper.parsers.grupy_parser import parse_grupa_details
from scraper.db import (
    supabase,
    save_kierunki,
    save_grupy,
    save_nauczyciele,
)

BASE_URL = "https://plan.uz.zgora.pl/"


def _groups_file(direction_external_id: str) -> str:
    return f"grupy_lista_grup_kierunku.ID={direction_external_id}.xml"


def _teacher_ics_url(teacher_ext_id: str) -> str:
    return f"{BASE_URL}nauczyciel_ics.php?ID={teacher_ext_id}&KIND=GG"


def _normalize_tryb(v: Optional[str]) -> str:
    if not v:
        return "nieznany"
    low = v.strip().lower()
    if any(x in low for x in ["niestac", "zaocz", "part-time", "part time", "np", "ns"]):
        return "niestacjonarne"
    if any(x in low for x in ["stac", "dzien", "full-time", "full time", "sp", "sd"]):
        return "stacjonarne"
    return "nieznany"


def _fetch_text(url: str, timeout: int = 8) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "uz-sync/2.1"})
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def _safe_soup_html(html: Optional[str]) -> Optional[BeautifulSoup]:
    if not html:
        return None
    txt = str(html)
    if len(txt) > 2_000_000:
        return None
    try:
        return BeautifulSoup(txt, "html.parser")
    except Exception:
        return None


def _extract_group_code_from_group_html(html: str) -> Optional[str]:
    soup = _safe_soup_html(html)
    if not soup:
        return None
    h2s = soup.find_all("h2")
    if len(h2s) >= 2:
        return h2s[1].get_text(strip=True) or None
    return None


def _extract_group_tryb_from_group_html(html: str) -> Optional[str]:
    soup = _safe_soup_html(html)
    if not soup:
        return None
    h3 = soup.find("h3")
    if not h3:
        return None
    txt = h3.get_text(" ", strip=True).lower()
    if "niestacjonarne" in txt or "zaoczne" in txt:
        return "niestacjonarne"
    if "stacjonarne" in txt or "dzienne" in txt:
        return "stacjonarne"
    return None


def _extract_teacher_refs_from_group_html(html: str) -> list[dict]:
    if not html:
        return []
    out = []
    seen = set()
    pattern = re.compile(
        r'<a[^>]+href="([^"]*nauczyciel_plan\.php\?ID=(\d+)[^"]*)"[^>]*>([^<]+)</a>',
        flags=re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        href = m.group(1)
        ext_id = m.group(2)
        anchor_name = (m.group(3) or "").strip()
        full = href if href.startswith("http") else BASE_URL + href.lstrip("/")
        key = full.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "name_from_group": anchor_name if anchor_name else f"Nauczyciel {ext_id}",
                "teacher_ext_id": ext_id,
                "link_strony_nauczyciela": full,
            }
        )
    return out


def _extract_teacher_details_from_profile_html(html: str) -> dict:
    soup = _safe_soup_html(html)
    if not soup:
        return {"nazwa": None, "instytut": None, "email": None}

    nazwa = None
    for h2 in soup.find_all("h2"):
        t = h2.get_text(strip=True)
        if t and "Plan zajęć" not in t:
            nazwa = t
            break

    instytut = None
    h3s = [h.get_text(" ", strip=True) for h in soup.find_all("h3")]
    if h3s:
        instytut = " | ".join([x for x in h3s if x])

    email = None
    a = soup.find("a", href=lambda v: v and "mailto:" in v)
    if a:
        email = a.get_text(strip=True)

    return {"nazwa": nazwa, "instytut": instytut, "email": email}


def _build_teacher_payload(group_teacher_refs: list[dict], verbose: bool = True) -> list[dict]:
    by_link: dict[str, dict] = {}
    for ref in group_teacher_refs:
        link = ref.get("link_strony_nauczyciela")
        ext_id = ref.get("teacher_ext_id")
        if not link or not ext_id:
            continue
        if link in by_link:
            continue

        profile_html = _fetch_text(link, timeout=6)
        details = _extract_teacher_details_from_profile_html(profile_html) if profile_html else {}

        by_link[link] = {
            "nazwa": details.get("nazwa") or ref.get("name_from_group"),
            "instytut": details.get("instytut"),
            "email": details.get("email"),
            "link_strony_nauczyciela": link,
            "link_ics_nauczyciela": _teacher_ics_url(ext_id),
        }

    if verbose:
        print(f"Teacher enrichment: refs={len(group_teacher_refs)}, unique_profiles={len(by_link)}")
    return list(by_link.values())


def _get_unassigned_kierunek_id() -> Optional[str]:
    row = (
        supabase.table("kierunki")
        .select("id")
        .eq("nazwa", "Nieprzypisane")
        .eq("wydzial", "Nieprzypisane")
        .limit(1)
        .execute()
        .data
        or []
    )
    if row:
        return row[0]["id"]

    supabase.table("kierunki").upsert(
        [{"nazwa": "Nieprzypisane", "wydzial": "Nieprzypisane"}],
        on_conflict="nazwa,wydzial",
    ).execute()

    row2 = (
        supabase.table("kierunki")
        .select("id")
        .eq("nazwa", "Nieprzypisane")
        .eq("wydzial", "Nieprzypisane")
        .limit(1)
        .execute()
        .data
        or []
    )
    return row2[0]["id"] if row2 else None


def _dedupe_groups_by_grupa_id(groups: list[dict]) -> list[dict]:
    out: dict[str, dict] = {}
    for g in groups:
        gid = str(g.get("grupa_id") or "").strip()
        if not gid:
            continue
        out[gid] = g
    return list(out.values())


def _upsert_directions_with_external_id(directions) -> None:
    def _norm(v):
        return (v or "").strip()

    def _read(item, field: str):
        if isinstance(item, dict):
            return item.get(field)
        return getattr(item, field, None)

    dedup = {}

    for d in directions:
        nazwa = _norm(_read(d, "nazwa"))
        wydzial = _norm(_read(d, "wydzial"))
        if not nazwa or not wydzial:
            continue

        ext = _read(d, "external_id") or _read(d, "xml_kierunek_id")
        key = (nazwa.casefold(), wydzial.casefold())

        if key not in dedup:
            dedup[key] = {
                "nazwa": nazwa,
                "wydzial": wydzial,
                "external_id": ext,
            }
        else:
            if not dedup[key].get("external_id") and ext:
                dedup[key]["external_id"] = ext

    rows = list(dedup.values())

    for i in range(0, len(rows), 200):
        batch = rows[i:i + 200]
        try:
            supabase.table("kierunki").upsert(batch, on_conflict="nazwa,wydzial").execute()
        except Exception:
            for row in batch:
                supabase.table("kierunki").upsert([row], on_conflict="nazwa,wydzial").execute()


def _direction_external_to_uuid_map() -> dict[str, str]:
    rows = supabase.table("kierunki").select("id,external_id").execute().data or []
    return {
        str(r["external_id"]).strip(): r["id"]
        for r in rows
        if r.get("external_id") is not None and r.get("id")
    }


def sync_directions_and_groups_from_xml(client: Optional[XmlClient] = None, verbose: bool = True) -> dict:
    client = client or XmlClient()

    # SMOKE FLAGS (szybkie testy)
    smoke_limit_directions = int(os.getenv("SMOKE_LIMIT_DIRECTIONS", "0") or "0")
    smoke_limit_groups_per_direction = int(os.getenv("SMOKE_LIMIT_GROUPS_PER_DIRECTION", "0") or "0")
    skip_teacher_enrichment = os.getenv("SKIP_TEACHER_ENRICHMENT", "0") == "1"

    unassigned_kierunek_id = _get_unassigned_kierunek_id()

    root = client.fetch_xml("grupy_lista_kierunkow.xml")
    if not root.content:
        raise RuntimeError("Brak treści XML: grupy_lista_kierunkow.xml")

    directions = parse_directions_from_xml(root.content)
    if smoke_limit_directions > 0:
        directions = directions[:smoke_limit_directions]

    _upsert_directions_with_external_id(directions)
    kierunek_id_by_external = _direction_external_to_uuid_map()

    all_groups = []
    all_teacher_refs = []
    fixed_codes = 0
    fallback_tryb_from_html = 0
    skipped_404 = 0

    for d in directions:
        resp = client.fetch_xml(_groups_file(d.external_id))
        if resp.status_code == 404:
            skipped_404 += 1
            continue
        if not resp.content:
            continue

        groups = parse_groups_from_xml(
            resp.content,
            direction_external_id=d.external_id,
            direction_name=d.name,
            faculty=d.faculty,
        )
        if smoke_limit_groups_per_direction > 0:
            groups = groups[:smoke_limit_groups_per_direction]

        for g in groups:
            gid = str(g.external_id) if g.external_id is not None else None
            if not gid:
                continue

            plan_url = g.group_plan_url or f"{BASE_URL}grupy_plan.php?ID={gid}"

            # Fetch HTML only when we need to fix code/tryb or to enrich teachers
            need_html = False
            xml_code = (g.code or "").strip()
            if not xml_code or xml_code.upper().startswith("GRUPA-") or not g.study_mode:
                need_html = True
            html = _fetch_text(plan_url, timeout=6) if need_html or (not skip_teacher_enrichment) else None

            parsed_details = {}
            if html:
                try:
                    parsed_details = parse_grupa_details(html)
                except Exception:
                    parsed_details = {}

            real_code = parsed_details.get("kod_grupy")
            final_code = xml_code
            if (not final_code or final_code.upper().startswith("GRUPA-")) and real_code:
                final_code = real_code
                fixed_codes += 1
            if not final_code:
                final_code = f"GRUPA-{gid}"

            tryb = parsed_details.get("tryb_studiow") or _normalize_tryb(g.study_mode)
            semestr = parsed_details.get("semestr")

            kierunek_id = kierunek_id_by_external.get(str(d.external_id)) or unassigned_kierunek_id

            all_groups.append(
                {
                    "kod_grupy": final_code,
                    "kierunek_id": kierunek_id,
                    "link_strony_grupy": plan_url,
                    "link_ics_grupy": g.group_ics_url,
                    "tryb_studiow": tryb,
                    "semestr": semestr,
                    "grupa_id": gid,
                }
            )

            if html and not skip_teacher_enrichment:
                all_teacher_refs.extend(_extract_teacher_refs_from_group_html(html))

    all_groups = _dedupe_groups_by_grupa_id(all_groups)
    saved_groups = save_grupy(all_groups)

    if skip_teacher_enrichment:
        teachers_payload = []
        saved_teachers = 0
    else:
        teachers_payload = _build_teacher_payload(all_teacher_refs, verbose=verbose)
        saved_teachers = save_nauczyciele(teachers_payload) if teachers_payload else 0

    if verbose:
        print(f"XML: directions_total={len(directions)}, skipped_404={skipped_404}")
        print(f"DB: zapisano/upewniono grupy: {saved_groups}")
        print(f"DB: zapisano/upewniono nauczycieli: {saved_teachers}")
        print(f"DB: poprawiono kodów grup z HTML: {fixed_codes}")
        print(f"DB: fallback tryb z HTML: {fallback_tryb_from_html}")
        print(f"SMOKE: skip_teacher_enrichment={skip_teacher_enrichment}")

    return {
        "directions_total": len(directions),
        "directions_skipped_404": skipped_404,
        "groups_saved": saved_groups,
        "teachers_saved": saved_teachers,
        "fixed_group_codes_from_html": fixed_codes,
        "fallback_tryb_from_html": fallback_tryb_from_html,
        "smoke_skip_teacher_enrichment": skip_teacher_enrichment,
    }
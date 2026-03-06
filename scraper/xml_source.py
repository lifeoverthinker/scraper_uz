import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import requests

XML_BASE_URL = "https://plan.uz.zgora.pl/static_files"
HTML_BASE_URL = "https://plan.uz.zgora.pl"


def _tag(node: ET.Element) -> str:
    return node.tag.split("}")[-1].upper() if node.tag else ""


def _clean(text: Optional[str]) -> str:
    return (text or "").strip()


def _pick(row: Dict[str, str], *keys: str) -> Optional[str]:
    for key in keys:
        value = row.get(key)
        if value:
            return value
    return None


def _fetch_xml_root(file_name: str, timeout: int = 30) -> Optional[ET.Element]:
    url = f"{XML_BASE_URL}/{file_name}"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return ET.fromstring(response.content)
    except Exception as e:
        print(f"XML fetch error {url}: {e}")
        return None


def _iter_item_nodes(root: ET.Element) -> Iterable[ET.Element]:
    for node in root.iter():
        if _tag(node) == "ITEM":
            yield node


def _item_to_flat_dict(item: ET.Element) -> Dict[str, str]:
    row: Dict[str, str] = {}

    for child in list(item):
        child_tag = _tag(child)
        if child_tag == "ITEMS":
            continue

        text = _clean(child.text)
        if text:
            row[child_tag] = text

    return row


def _extract_semester_hint(value: Optional[str]) -> Optional[str]:
    text = (value or "").lower()
    if not text:
        return None
    if "letni" in text or "summer" in text:
        return "letni"
    if "zimowy" in text or "winter" in text:
        return "zimowy"
    return None


def _extract_mode_hint(value: Optional[str]) -> Optional[str]:
    text = (value or "").lower()
    if not text:
        return None
    if "niestacjon" in text:
        return "niestacjonarne"
    if "stacjon" in text:
        return "stacjonarne"
    return _clean(value) or None


def _minutes_to_hhmm(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        minutes = int(str(value).strip())
        hh = minutes // 60
        mm = minutes % 60
        return f"{hh:02d}:{mm:02d}"
    except Exception:
        return None


def _parse_date_token(token: str) -> Optional[str]:
    token = _clean(token)
    if not token:
        return None

    # Czasem daty maja postfix godziny/teksty - obcinamy do sensownego fragmentu.
    token = token.split(" ")[0]

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(token, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _normalize_time_token(value: Optional[str]) -> Optional[str]:
    value = _clean(value)
    if not value:
        return None

    # Akceptujemy np. 8:00, 08:00, 08:00:00
    match = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2})?$", value)
    if not match:
        return None
    hh = int(match.group(1))
    mm = int(match.group(2))
    return f"{hh:02d}:{mm:02d}"


def _split_dates(value: Optional[str]) -> List[str]:
    raw = _clean(value)
    if not raw:
        return []

    tokens = re.split(r"[;,]", raw)
    out = []
    for token in tokens:
        parsed = _parse_date_token(token)
        if parsed:
            out.append(parsed)
    return out


def _event_uid(prefix: str, owner_id: str, subject: str, date_str: str, time_from: str, time_to: str, row_id: str) -> str:
    raw = f"{prefix}|{owner_id}|{subject}|{date_str}|{time_from}|{time_to}|{row_id}"
    digest = hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()
    return f"xml-{digest}"


def _parse_plan_rows(root: ET.Element, owner_prefix: str, owner_id: str) -> List[dict]:
    rows: List[dict] = []

    for item in _iter_item_nodes(root):
        row = _item_to_flat_dict(item)
        if not row:
            continue

        subject = _pick(row, "PRZEDMIOT", "SUBJECT", "NAME", "NAZWA", "TYTUL")
        if not subject:
            continue

        date_values = _split_dates(_pick(row, "TERMIN_DT", "DATES", "DATY", "DATE", "DATA"))
        if not date_values:
            # Brak daty => najpewniej nie jest to rekord zajec.
            continue

        time_from = (
            _normalize_time_token(_pick(row, "G_OD", "START", "OD"))
            or _minutes_to_hhmm(_pick(row, "OD_GODZ", "START_MIN", "ODMIN"))
        )
        time_to = (
            _normalize_time_token(_pick(row, "G_DO", "END", "DO"))
            or _minutes_to_hhmm(_pick(row, "DO_GODZ", "END_MIN", "DOMIN"))
        )

        if not time_from or not time_to:
            continue

        teacher = _pick(row, "NAUCZYCIEL", "TEACHER", "PROWADZACY", "PROWADZACY_NAZWA")
        groups = _pick(row, "GRUPY", "GROUPS", "GRUPA", "KOD_GRUPY")
        location = _pick(row, "SALA", "MIEJSCE", "ROOM", "LOKALIZACJA")
        podgrupa = _pick(row, "PODGRUPA", "PG", "PGR")
        rz = _pick(row, "RZ", "RODZAJ", "FORMA", "TYPE")
        semester_id = _pick(row, "SEMESTER_ID", "SEMESTR_ID", "SEM_ID")
        row_id = _pick(row, "UID", "ID", "EVENT_ID") or "no-id"

        for date_str in date_values:
            uid = _event_uid(owner_prefix, owner_id, subject, date_str, time_from, time_to, row_id)
            rows.append(
                {
                    "uid": uid,
                    "przedmiot": subject,
                    "od": f"{date_str}T{time_from}:00",
                    "do_": f"{date_str}T{time_to}:00",
                    "miejsce": location,
                    "rz": rz,
                    "nauczyciel": teacher,
                    "grupy": groups,
                    "podgrupa": podgrupa,
                    "semester_id": semester_id,
                    "link_ics_zrodlowy": f"{XML_BASE_URL}/{owner_prefix}_plan.ID={owner_id}.xml",
                }
            )

    return rows


def scrape_kierunki_xml() -> List[dict]:
    root = _fetch_xml_root("grupy_lista_kierunkow.xml")
    if root is None:
        return []

    kierunki = []
    seen = set()

    for item in _iter_item_nodes(root):
        row = _item_to_flat_dict(item)
        if not row:
            continue

        kierunek_id = _pick(row, "ID", "KIERUNEK_ID", "COURSE_ID")
        nazwa = _pick(row, "NAZWA", "NAME", "KIERUNEK", "KIERUNEK_NAZWA")
        wydzial = _pick(
            row,
            "WYDZIAL",
            "WYDZIAL_NAZWA",
            "NAZWA_WYDZIALU",
            "JEDNOSTKA",
            "UNIT",
            "FACULTY",
            "DEPARTMENT",
        )

        if not nazwa or not kierunek_id:
            continue

        key = str(kierunek_id)
        if key in seen:
            continue
        seen.add(key)

        kierunki.append(
            {
                "nazwa": _clean(nazwa),
                "wydzial": _clean(wydzial),
                "xml_kierunek_id": str(kierunek_id),
                "link_strony_kierunku": f"{HTML_BASE_URL}/grupy_lista_grup_kierunku.php?ID={kierunek_id}",
            }
        )

    return kierunki


def _parse_grupy_for_single_kierunek(kierunek: dict) -> List[dict]:
    xml_kierunek_id = kierunek.get("xml_kierunek_id") or kierunek.get("id")
    if not xml_kierunek_id:
        return []

    root = _fetch_xml_root(f"grupy_lista_grup_kierunku.ID={xml_kierunek_id}.xml")
    if root is None:
        return []

    kierunek_nazwa = kierunek.get("nazwa")
    wydzial = kierunek.get("wydzial")

    grupy = []
    seen = set()

    for item in _iter_item_nodes(root):
        row = _item_to_flat_dict(item)
        if not row:
            continue

        grupa_id = _pick(row, "ID", "GRUPA_ID", "GROUP_ID", "GID")
        kod_grupy = _pick(row, "KOD_GRUPY", "KOD", "SKROT", "NAZWA", "NAME")
        tryb_raw = _pick(row, "TRYB_STUDIOW", "TRYB", "MODE")
        semestr_raw = _pick(row, "SEMESTR", "SEMESTER", "TERM", "SEM")

        if not tryb_raw and kod_grupy:
            tryb_raw = kod_grupy
        if not semestr_raw and kod_grupy:
            semestr_raw = kod_grupy

        if not grupa_id:
            continue

        key = str(grupa_id)
        if key in seen:
            continue
        seen.add(key)

        grupy.append(
            {
                "kod_grupy": _clean(kod_grupy) or f"GRUPA-{grupa_id}",
                "kierunek_id": kierunek.get("id"),
                "kierunek_nazwa": kierunek_nazwa,
                "wydzial": wydzial,
                "link_strony_grupy": f"{HTML_BASE_URL}/grupy_plan.php?ID={grupa_id}",
                "link_ics_grupy": f"{HTML_BASE_URL}/grupy_ics.php?ID={grupa_id}&KIND=GG",
                "tryb_studiow": _extract_mode_hint(tryb_raw),
                "semestr": _extract_semester_hint(semestr_raw),
                "grupa_id": str(grupa_id),
            }
        )

    return grupy


def scrape_grupy_for_kierunki_xml(kierunki: List[dict]) -> List[dict]:
    all_grupy: List[dict] = []
    for kierunek in kierunki:
        grupy = _parse_grupy_for_single_kierunek(kierunek)
        if grupy:
            all_grupy.extend(grupy)

    return all_grupy


def fetch_grupy_plan_events_xml(grupa_id: str) -> List[dict]:
    root = _fetch_xml_root(f"grupy_plan.ID={grupa_id}.xml")
    if root is None:
        return []

    events = _parse_plan_rows(root, "grupy", str(grupa_id))
    for event in events:
        event["grupa_id"] = str(grupa_id)
    return events


def fetch_nauczyciel_plan_events_xml(nauczyciel_id: str) -> List[dict]:
    root = _fetch_xml_root(f"nauczyciel_plan.ID={nauczyciel_id}.xml")
    if root is None:
        return []

    events = _parse_plan_rows(root, "nauczyciel", str(nauczyciel_id))
    return events


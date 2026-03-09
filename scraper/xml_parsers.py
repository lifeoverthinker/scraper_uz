from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Optional
import re
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class XmlDirection:
    external_id: str
    name: str
    faculty: str


@dataclass(frozen=True)
class XmlGroup:
    external_id: str
    code: str
    direction_external_id: Optional[str] = None
    study_mode: Optional[str] = None
    semester_name: Optional[str] = None


@dataclass(frozen=True)
class XmlScheduleEvent:
    external_uid: str
    subject: str
    starts_at: Optional[str]
    ends_at: Optional[str]
    room: Optional[str]
    class_type: Optional[str]
    teacher_name: Optional[str]
    groups_label: Optional[str]
    subgroup: Optional[str]
    id_semestru: Optional[str]
    raw_dates: list[date]


def _format_teacher_name(raw_name: Optional[str]) -> Optional[str]:
    """
    Zamienia format 'Nazwisko Imię, tytuły' (np. z tagu SORT)
    na 'tytuły Imię Nazwisko' używany w tabeli nauczyciele.
    """
    if not raw_name or raw_name.lower() == "brak":
        return None

    # Dzielimy tekst po PIERWSZYM przecinku
    # Dla "Sztyber Radosław, dr hab., prof. UZ"
    # parts[0] = "Sztyber Radosław", parts[1] = " dr hab., prof. UZ"
    parts = raw_name.split(',', 1)

    imie_nazwisko_part = parts[0].strip()
    tytuly_part = parts[1].strip() if len(parts) > 1 else ""

    # Odwracanie "Nazwisko Imię" -> "Imię Nazwisko"
    name_tokens = imie_nazwisko_part.split()
    if len(name_tokens) > 1:
        # name_tokens[0] to nazwisko, reszta (np. name_tokens[1:]) to imiona
        imie_nazwisko = " ".join(name_tokens[1:]) + " " + name_tokens[0]
    else:
        imie_nazwisko = imie_nazwisko_part

    # Składamy wszystko w jedną całość (Tytuły + Imię + Nazwisko)
    if tytuly_part:
        return f"{tytuly_part} {imie_nazwisko}"
    return imie_nazwisko

def parse_directions_from_xml(xml_content: str) -> list[XmlDirection]:
    soup = BeautifulSoup(xml_content, "xml")
    results = []

    # ROOT > ITEMS > ITEM (to są Wydziały)
    root_items = soup.find("ITEMS")
    if not root_items: return []

    for faculty_item in root_items.find_all("ITEM", recursive=False):
        faculty_name = faculty_item.find("NAME").get_text(strip=True) if faculty_item.find("NAME") else ""

        # Wydział > ITEMS > ITEM (to są Kierunki)
        dir_items_container = faculty_item.find("ITEMS")
        if dir_items_container:
            for dir_item in dir_items_container.find_all("ITEM", recursive=False):
                ext_id = dir_item.find("ID")
                dir_name = dir_item.find("NAME")
                if ext_id and dir_name:
                    results.append(XmlDirection(
                        external_id=ext_id.get_text(strip=True),
                        name=dir_name.get_text(strip=True),
                        faculty=faculty_name  # Tu wstawiamy nazwę wydziału-rodzica
                    ))
    return results


def parse_groups_from_xml(xml_content: str, direction_external_id: Optional[str] = None) -> list[XmlGroup]:
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all("ITEM")
    results = []
    for it in items:
        ext_id_tag = it.find("ID")
        # Log pokazał, że KOD to często NAME w nagłówku, ale w liście to ID lub KOD
        code_tag = it.find("KOD") or it.find("CODE") or it.find("NAME")

        if ext_id_tag:
            results.append(XmlGroup(
                external_id=ext_id_tag.get_text(strip=True),
                code=code_tag.get_text(strip=True) if code_tag else f"GRUPA-{ext_id_tag.text}",
                direction_external_id=direction_external_id
            ))
    return results


def parse_group_plan_events(xml_content: str, source_url: Optional[str] = None) -> list[XmlScheduleEvent]:
    return _parse_plan_events(xml_content, source_url)


def parse_teacher_plan_events(xml_content: str, source_url: Optional[str] = None) -> list[XmlScheduleEvent]:
    return _parse_plan_events(xml_content, source_url)


def _parse_plan_events(xml_content: str, source_url: Optional[str] = None) -> list[XmlScheduleEvent]:
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all("ITEM")
    out = []

    # Próba pobrania ID semestru z nagłówka pliku (ROOT) jako fallback
    root_tag = soup.find("ROOT")
    header_semester_id = root_tag.find("SEMESTER_ID").get_text(strip=True) if root_tag and root_tag.find("SEMESTER_ID") else None

    for it in items:
        # Podstawowe dane
        uid_tag = it.find("ID_POZYCJA") or it.find("UID")
        subject_tag = it.find("NAME") or it.find("PRZEDMIOT")
        if not uid_tag or not subject_tag: continue

        def get_txt(tag_name):
            f = it.find(tag_name)
            return f.get_text(strip=True) if f and f.text else None

        # Dane potwierdzone w PowerShell
        teacher = get_txt("SORT")  # W plikach grup SORT zawiera nazwisko nauczyciela
        subgroup = get_txt("PG")  # W plikach grup PG zawiera podgrupę (np. "Praw")
        semester_id = get_txt("ID_SEMESTR") or header_semester_id
        class_type = get_txt("RZ")

        # --- Logika Sali ---
        room = None
        sale_node = it.find("SALE")
        if sale_node:
            room_tag = sale_node.find("NAME")
            if room_tag:
                room = room_tag.get_text(strip=True)

        # Fallback: wyciąganie sali z uwag (R_UWAGI), np. "s. 305 A-41"
        if not room:
            remarks = get_txt("R_UWAGI")
            if remarks and "s." in remarks:
                # Pobieramy tekst po "s. "
                room = remarks.split("s.")[-1].strip().replace("\n", " ")

        # Czas i Daty
        g_od_val = get_txt("G_OD")
        g_do_val = get_txt("G_DO")
        dates_raw = get_txt("TERMIN_DT")

        if dates_raw:
            # KLUCZOWA ZMIANA: Pętla po wszystkich datach połączonych średnikami
            for d_str in [c.strip() for c in dates_raw.split(";") if c.strip()]:
                try:
                    current_date = datetime.strptime(d_str, "%Y-%m-%d").date()
                    starts_at = _compose_datetime_iso(current_date, g_od_val)
                    ends_at = _compose_datetime_iso(current_date, g_do_val)

                    out.append(XmlScheduleEvent(
                        # Tworzymy unikalne ID łącząc oryginalne UID z datą
                        external_uid=f"{uid_tag.get_text(strip=True)}_{d_str}",
                        subject=subject_tag.get_text(strip=True),
                        starts_at=starts_at,
                        ends_at=ends_at,
                        room=room,
                        class_type=class_type,
                        teacher_name=teacher,
                        groups_label=get_txt("SORT"),
                        subgroup=subgroup,
                        id_semestru=semester_id,
                        raw_dates=[current_date]
                    ))
                except Exception:
                    continue

    return out


def _compose_datetime_iso(d: Optional[date], hhmm: Optional[str]) -> Optional[str]:
    if not d or not hhmm or ":" not in hhmm: return None
    try:
        h, m = map(int, hhmm.split(":"))
        return datetime(d.year, d.month, d.day, h, m).isoformat()
    except: return None
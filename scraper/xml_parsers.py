from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Optional
import re

from bs4 import BeautifulSoup


# =========================
# Dataclasses (czytelność)
# =========================

@dataclass(frozen=True)
class XmlDirection:
    external_id: str
    name: str
    faculty: str


@dataclass(frozen=True)
class XmlGroup:
    external_id: str
    code: str
    direction_external_id: Optional[str]
    direction_name: Optional[str]
    faculty: Optional[str]
    group_plan_url: Optional[str]
    group_ics_url: Optional[str]
    study_mode: Optional[str]   # stacjonarne / niestacjonarne
    semester_name: Optional[str]  # letni / zimowy


@dataclass(frozen=True)
class XmlTeacher:
    external_id: str
    name: str
    unit_name: Optional[str]
    email: Optional[str]
    teacher_plan_url: Optional[str]
    teacher_ics_url: Optional[str]


@dataclass(frozen=True)
class XmlScheduleEvent:
    external_uid: str
    subject: str
    starts_at: Optional[str]
    ends_at: Optional[str]
    room: Optional[str]
    class_type: Optional[str]  # rz
    teacher_name: Optional[str]
    groups_label: Optional[str]
    subgroup: Optional[str]
    raw_dates: list[date]      # z TERMIN_DT
    last_schedule_date: Optional[date]
    source_url: Optional[str]


# =========================
# Public parsery list
# =========================

def parse_directions_from_xml(xml_content: str) -> list[XmlDirection]:
    """
    Parser dla grupy kierunków (np. grupy_lista_kierunkow.xml).
    Elastyczny: szuka ITEM i wielu wariantów pól.
    """
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all(lambda t: t.name and t.name.lower() == "item")
    results: list[XmlDirection] = []

    for it in items:
        ext_id = _pick_text(it, ["ID", "KIERUNEK_ID", "DIRECTION_ID"])
        name = _pick_text(it, ["NAME", "NAZWA", "KIERUNEK", "DIRECTION_NAME"])
        faculty = _pick_text(it, ["WYDZIAL", "FACULTY", "JEDNOSTKA", "UNIT_NAME"])

        if not ext_id or not name:
            continue

        results.append(
            XmlDirection(
                external_id=ext_id,
                name=name,
                faculty=faculty or "",
            )
        )
    return results


# PODMIEŃ TYLKO funkcję parse_groups_from_xml i _normalize_study_mode

def parse_groups_from_xml(
    xml_content: str,
    direction_external_id: Optional[str] = None,
    direction_name: Optional[str] = None,
    faculty: Optional[str] = None,
) -> list[XmlGroup]:
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all(lambda t: t.name and t.name.lower() == "item")
    results: list[XmlGroup] = []

    for it in items:
        ext_id = _pick_text(it, ["ID", "GRUPA_ID", "GROUP_ID"])
        code = _pick_text(it, ["KOD", "KOD_GRUPY", "CODE", "GROUP_CODE"])
        plan_url = _pick_text(it, ["URL", "LINK", "PLAN_URL", "LINK_PLANU", "GROUP_PLAN_URL"])
        ics_url = _pick_text(it, ["ICS", "URL_ICS", "LINK_ICS", "GROUP_ICS_URL"])

        mode_raw = _pick_text(
            it,
            [
                "TRYB", "TRYB_STUDIOW", "STUDY_MODE", "MODE",
                "FORMA_STUDIOW", "FORM", "STUDIA_FORMA"
            ],
        )
        mode = _normalize_study_mode(mode_raw)

        sem = _normalize_semester_name(
            _pick_text(it, ["SEMESTR", "SEMESTER_NAME", "SEMESTR_NAZWA", "SEMESTER"])
        )

        if not ext_id:
            continue
        if not code:
            code = f"GRUPA-{ext_id}"

        results.append(
            XmlGroup(
                external_id=ext_id,
                code=code,
                direction_external_id=direction_external_id,
                direction_name=direction_name,
                faculty=faculty,
                group_plan_url=plan_url,
                group_ics_url=ics_url,
                study_mode=mode,
                semester_name=sem,
            )
        )
    return results

def parse_teachers_from_xml(
    xml_content: str,
    unit_name: Optional[str] = None,
) -> list[XmlTeacher]:
    """
    Parser listy nauczycieli dla jednostki
    (np. nauczyciel_lista_wydzialu.ID=xxx.xml).
    """
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all(lambda t: t.name and t.name.lower() == "item")
    results: list[XmlTeacher] = []

    for it in items:
        ext_id = _pick_text(it, ["ID", "NAUCZYCIEL_ID", "TEACHER_ID"])
        name = _pick_text(it, ["NAME", "NAZWA", "NAUCZYCIEL", "TEACHER_NAME"])
        email = _pick_text(it, ["EMAIL", "MAIL"])
        plan_url = _pick_text(it, ["URL", "LINK", "PLAN_URL", "LINK_PLANU"])
        ics_url = _pick_text(it, ["ICS", "URL_ICS", "LINK_ICS"])

        if not ext_id or not name:
            continue

        results.append(
            XmlTeacher(
                external_id=ext_id,
                name=name,
                unit_name=unit_name,
                email=email,
                teacher_plan_url=plan_url,
                teacher_ics_url=ics_url,
            )
        )
    return results


# =========================
# Public parser planów
# =========================

def parse_group_plan_events(xml_content: str, source_url: Optional[str] = None) -> list[XmlScheduleEvent]:
    return _parse_plan_events(xml_content=xml_content, source_url=source_url)


def parse_teacher_plan_events(xml_content: str, source_url: Optional[str] = None) -> list[XmlScheduleEvent]:
    return _parse_plan_events(xml_content=xml_content, source_url=source_url)


# =========================
# Core parser planu
# =========================

def _parse_plan_events(xml_content: str, source_url: Optional[str]) -> list[XmlScheduleEvent]:
    soup = BeautifulSoup(xml_content, "xml")
    items = soup.find_all(lambda t: t.name and t.name.lower() == "item")
    out: list[XmlScheduleEvent] = []

    for it in items:
        # Pola spotykane w planach XML
        uid = _pick_text(it, ["UID", "EVENT_UID", "ID_ZAJEC", "ZAJECIA_ID"]) or _build_fallback_uid(it)
        subject = _pick_text(it, ["PRZEDMIOT", "NAME", "TYTUL", "SUBJECT"]) or "Brak nazwy"
        room = _pick_text(it, ["MIEJSCE", "SALA", "ROOM", "LOCATION"])
        rz = _pick_text(it, ["RZ", "RODZAJ_ZAJEC", "TYPE"])
        teacher = _pick_text(it, ["NAUCZYCIEL", "PROWADZACY", "TEACHER"])
        groups_label = _pick_text(it, ["GRUPY", "GROUPS", "GROUP_LABEL"])
        subgroup = _pick_text(it, ["PODGRUPA", "PG", "SUBGROUP"])

        # Czas: preferuj G_OD/G_DO (hh:mm) + daty z TERMIN_DT,
        # ale zachowaj też OD_GODZ/DO_GODZ gdy brak.
        g_od = _pick_text(it, ["G_OD", "START_HHMM"])
        g_do = _pick_text(it, ["G_DO", "END_HHMM"])
        od_godz = _pick_text(it, ["OD_GODZ", "START_MINUTES"])
        do_godz = _pick_text(it, ["DO_GODZ", "END_MINUTES"])

        dates = _parse_termin_dt_dates(_pick_text(it, ["TERMIN_DT", "DATES", "TERM_DATES"]))
        last_date = max(dates) if dates else None

        starts_at = _compose_datetime_iso(last_date, g_od, od_godz)
        ends_at = _compose_datetime_iso(last_date, g_do, do_godz)

        out.append(
            XmlScheduleEvent(
                external_uid=uid,
                subject=subject,
                starts_at=starts_at,
                ends_at=ends_at,
                room=room,
                class_type=rz,
                teacher_name=teacher,
                groups_label=groups_label,
                subgroup=subgroup,
                raw_dates=dates,
                last_schedule_date=last_date,
                source_url=source_url,
            )
        )

    return out


# =========================
# Helpers
# =========================

def _pick_text(tag: Any, names: list[str]) -> Optional[str]:
    # 1) child tag case-insensitive
    for n in names:
        found = tag.find(lambda t: t.name and t.name.lower() == n.lower())
        if found and found.text is not None:
            val = found.text.strip()
            if val:
                return val

    # 2) attributes
    attrs = getattr(tag, "attrs", {}) or {}
    attrs_lower = {str(k).lower(): v for k, v in attrs.items()}
    for n in names:
        val = attrs_lower.get(n.lower())
        if val is not None:
            sval = str(val).strip()
            if sval:
                return sval

    return None


def _normalize_study_mode(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip().lower()

    # niestacjonarne
    if any(x in v for x in ["niestac", "zaoczne", "part-time", "part time", "np", "ns"]):
        return "niestacjonarne"

    # stacjonarne
    if any(x in v for x in ["stac", "dzienne", "full-time", "full time", "sp", "sd"]):
        return "stacjonarne"

    return value.strip()


def _normalize_semester_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip().lower()
    if "let" in v:
        return "letni"
    if "zim" in v:
        return "zimowy"
    return value.strip()


def _parse_termin_dt_dates(raw: Optional[str]) -> list[date]:
    """
    TERMIN_DT wg opisu CK: daty oddzielone średnikami.
    Obsługa najczęstszych formatów:
    - YYYY-MM-DD
    - DD.MM.YYYY
    - YYYYMMDD
    """
    if not raw:
        return []
    chunks = [c.strip() for c in raw.split(";") if c.strip()]
    parsed: list[date] = []

    for c in chunks:
        d = _try_parse_date(c)
        if d:
            parsed.append(d)

    return parsed


def _try_parse_date(value: str) -> Optional[date]:
    # YYYY-MM-DD
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        pass
    # DD.MM.YYYY
    try:
        return datetime.strptime(value, "%d.%m.%Y").date()
    except ValueError:
        pass
    # YYYYMMDD
    if re.fullmatch(r"\d{8}", value):
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError:
            return None
    return None


def _compose_datetime_iso(last_date: Optional[date], hhmm: Optional[str], minutes: Optional[str]) -> Optional[str]:
    """
    Składa datetime ISO na bazie:
    - data = last_date (jeśli jest)
    - czas = hh:mm albo minuty od północy
    Jeśli brak daty, zwraca None (nie zgadujemy daty).
    """
    if not last_date:
        return None

    hour = 0
    minute = 0

    if hhmm and re.fullmatch(r"\d{1,2}:\d{2}", hhmm.strip()):
        h, m = hhmm.strip().split(":")
        hour, minute = int(h), int(m)
    elif minutes and str(minutes).isdigit():
        total = int(minutes)
        hour, minute = divmod(total, 60)
    else:
        return None

    try:
        dt = datetime(
            year=last_date.year,
            month=last_date.month,
            day=last_date.day,
            hour=hour,
            minute=minute,
        )
        return dt.isoformat()
    except ValueError:
        return None


def _build_fallback_uid(item_tag: Any) -> str:
    """
    Awaryjny UID gdy XML go nie ma.
    """
    base = (
        _pick_text(item_tag, ["PRZEDMIOT", "NAME", "SUBJECT"]) or "NO_SUBJECT",
        _pick_text(item_tag, ["TERMIN_DT", "DATES"]) or "NO_DATE",
        _pick_text(item_tag, ["G_OD", "START_HHMM", "OD_GODZ"]) or "NO_START",
        _pick_text(item_tag, ["G_DO", "END_HHMM", "DO_GODZ"]) or "NO_END",
    )
    return "XML-" + "|".join(base)
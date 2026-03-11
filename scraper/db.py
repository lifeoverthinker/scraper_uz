from __future__ import annotations
import hashlib, os, time
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# Inicjalizacja klienta Supabase
project_root = Path(__file__).resolve().parent.parent
load_dotenv(project_root / ".env")
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))


def _norm(v: Any) -> str: return str(v).strip().casefold()


def _str(v: Any) -> str: return "" if v is None else str(v)


def _normalize_timestamp(v: Any) -> str | None:
    if v is None: return None
    if isinstance(v, datetime): return v.isoformat()
    t = str(v).strip()
    if not t or t.upper() in {"NO_DATE", "NULL"}: return None
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00")).isoformat()
    except:
        return None


def chunks(lst, n):
    for i in range(0, len(lst), n): yield lst[i:i + n]


def get_semester_state() -> Optional[dict]:
    try:
        res = supabase.table("semester_state").select("*").eq("id", 1).execute()
        return res.data[0] if res.data else None
    except:
        return None


def save_semester_state(data: dict):
    payload = {
        "id": 1,
        "id_semestru_aktualny": data.get("current_semester_id"),
        "nazwa_semestru_aktualny": data.get("current_semester_name_pl") or data.get("current_semester_name"),
        "id_semestru_poprzedni": data.get("previous_semester_id"),
        "nazwa_semestru_poprzedni": data.get("previous_semester_name_pl") or data.get("previous_semester_name"),
        "data_aktualizacji": "now()"
    }
    supabase.table("semester_state").upsert(payload).execute()


def save_kierunki(kierunki):
    unique_data = {}
    for k in kierunki:
        unique_data[k.external_id] = {
            "nazwa": k.name,
            "wydzial": k.faculty,
            "external_id": k.external_id
        }
    data = list(unique_data.values())
    if data:
        supabase.table("kierunki").upsert(data, on_conflict="external_id").execute()


def get_uuid_map(table, key_col, val_col):
    res = supabase.table(table).select(f"{key_col}, {val_col}").execute()
    return {str(row[key_col]).strip().lower(): row[val_col] for row in (res.data or [])}


def save_grupy(grupy):
    unique_data = {}
    for g in grupy:
        gid = g.get("external_id") or g.get("grupa_id")
        unique_data[gid] = {
            "nazwa": g.get("kod_grupy") or g.get("nazwa"),
            "kierunek_id": g.get("kierunek_id"),
            "tryb": g.get("study_mode") or g.get("tryb_studiow") or "nieznane",
            "semestr": g.get("semester_name") or g.get("semestr"),
            "grupa_id": gid
        }
    data = list(unique_data.values())
    if data:
        supabase.table("grupy").upsert(data, on_conflict="grupa_id").execute()


def save_nauczyciele(teachers):
    unique_data = {}
    for t in teachers:
        ext_id = t.get("external_id")
        unique_data[ext_id] = {
            "nazwisko_imie": t.get("name") or t.get("nazwa") or t.get("nazwisko_imie"),
            "jednostka": t.get("unit_name") or t.get("instytut") or t.get("jednostka"),
            "email": t.get("email"),
            "external_id": ext_id
        }
    data = list(unique_data.values())
    if data:
        supabase.table("nauczyciele").upsert(data, on_conflict="external_id").execute()


def save_zajecia_grupy(events, grupa_id_target: str):
    """
    Zapisuje zajęcia grupy, aktualizuje zmienione dane (np. sala)
    oraz usuwa zajęcia, które zostały odwołane (zniknęły z XML).
    """
    if not events:
        return 0

    batch_data = []
    seen_uids = set()

    for e in events:
        if is_dataclass(e):
            e = asdict(e)

        # Pobieramy bazowy UID z parsera XML
        base_uid = e.get("external_uid") or e.get("uid")
        if not base_uid:
            continue

        # ==========================================================
        # KLUCZOWA ZMIANA: Prefiksujemy UID identyfikatorem grupy!
        # Zapobiega to nadpisywaniu wykładów przez inne grupy.
        # ==========================================================
        uid = f"{grupa_id_target}_{base_uid}"

        if uid in seen_uids:
            continue

        seen_uids.add(uid)

        # Przygotowanie rekordu zgodnie z Twoim schematem SQL
        batch_data.append({
            "uid": uid,
            "id_semestru": e.get("id_semestru"),
            "poczatek": e.get("starts_at"),
            "koniec": e.get("ends_at"),
            "przedmiot": e.get("subject"),
            "rodzaj_zajec": e.get("class_type"),
            "sala": e.get("room"),
            "nauczyciel": e.get("teacher_name"),
            "podgrupa": e.get("subgroup")[:20] if e.get("subgroup") else None,
            "grupa_id": grupa_id_target
        })

    # 2. Upsert danych (Aktualizacja jeśli UID już istnieje)
    if batch_data:
        for b in chunks(batch_data, 500):
            supabase.table("zajecia_grupy").upsert(b, on_conflict="uid").execute()

    # 3. USUWANIE ODWOŁANYCH ZAJĘĆ
    if seen_uids:
        supabase.table("zajecia_grupy").delete() \
            .eq("grupa_id", grupa_id_target) \
            .gt("poczatek", "now()") \
            .not_.in_("uid", list(seen_uids)) \
            .execute()

    return len(batch_data)


def save_zajecia_nauczyciela(events, nauczyciel_uuid: str):
    if not events: return 0

    seen_uids = set()
    batch_data = []

    for e in events:
        if is_dataclass(e): e = asdict(e)

        # Pobieramy bazowy UID
        base_uid = e.get("external_uid") or e.get("uid")

        poczatek = _normalize_timestamp(e.get("starts_at") or e.get("od"))
        koniec = _normalize_timestamp(e.get("ends_at") or e.get("do_"))

        if not base_uid: continue

        # KLUCZOWA ZMIANA: Prefiksujemy UID unikalnym ID nauczyciela!
        # Dzięki temu współdzielone zajęcia będą miały osobne wpisy dla każdego prowadzącego.
        uid = f"{nauczyciel_uuid}_{base_uid}"

        if uid in seen_uids: continue

        seen_uids.add(uid)
        batch_data.append({
            "uid": uid,
            "id_semestru": e.get("id_semestru"),
            "poczatek": poczatek,
            "koniec": koniec,
            "przedmiot": e.get("subject") or e.get("przedmiot"),
            "rodzaj_zajec": e.get("class_type") or e.get("rz"),
            "sala": e.get("room") or e.get("miejsce"),
            "grupy": e.get("groups_label") or e.get("grupy"),
            "nauczyciel_id": nauczyciel_uuid
        })

    for b in chunks(batch_data, 500):
        supabase.table("zajecia_nauczyciela").upsert(b, on_conflict="uid").execute()

    # Usuwanie znikniętych zajęć przyszłych dla tego nauczyciela
    if seen_uids:
        supabase.table("zajecia_nauczyciela").delete() \
            .eq("nauczyciel_id", nauczyciel_uuid) \
            .gt("poczatek", "now()") \
            .not_.in_("uid", list(seen_uids)) \
            .execute()

    return len(batch_data)
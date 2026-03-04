from __future__ import annotations

from dotenv import load_dotenv
import os
import time
import datetime as dt
from dataclasses import asdict, is_dataclass
from typing import Dict, Any, List, Optional

from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# =========================
# Helpers
# =========================

def chunks(lst: List[Any], n: int):
    """Dzieli listę na części o rozmiarze n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _to_iso_date(value: Any) -> Optional[str]:
    """
    Zamienia date/datetime/string -> YYYY-MM-DD (jeśli możliwe).
    """
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()

    s = str(value).strip()
    if not s:
        return None

    # YYYY-MM-DD...
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]

    # DD.MM.YYYY
    try:
        d = dt.datetime.strptime(s, "%d.%m.%Y").date()
        return d.isoformat()
    except ValueError:
        pass

    # YYYYMMDD
    if s.isdigit() and len(s) == 8:
        try:
            d = dt.datetime.strptime(s, "%Y%m%d").date()
            return d.isoformat()
        except ValueError:
            return None

    return None


def _upsert_with_retry(
    table: str,
    data: List[dict],
    on_conflict: str,
    max_retries: int = 3,
    backoff_start: float = 1.5,
) -> int:
    """
    Uniwersalny upsert z retry dla problemów sieciowych/timeouts.
    Zwraca liczbę rekordów przekazanych do upsert.
    """
    if not data:
        return 0

    attempt = 0
    backoff = backoff_start
    while attempt < max_retries:
        try:
            supabase.table(table).upsert(data, on_conflict=on_conflict).execute()
            return len(data)
        except Exception as e:
            msg = str(e).lower()
            attempt += 1
            retriable = any(tok in msg for tok in ["timed out", "timeout", "ssl", "did not complete", "connection"])
            if retriable and attempt < max_retries:
                print(f"⚠️ Retry {attempt}/{max_retries} dla {table}: {str(e)[:120]}...")
                time.sleep(backoff)
                backoff *= 2
                continue
            print(f"❌ Upsert failed [{table}]: {e}")
            return 0
    return 0


# =========================
# UUID maps
# =========================

def get_uuid_map(table: str, key_col: str, id_col: str) -> Dict:
    """Pobiera mapowanie kluczy do UUID z bazy."""
    if table == "kierunki":
        result = supabase.table(table).select(f"{key_col}, wydzial, {id_col}").execute()
        return {
            (str(row[key_col]).strip().casefold(), str(row["wydzial"]).strip().casefold()): row[id_col]
            for row in result.data
            if row.get(key_col) and row.get("wydzial")
        }
    else:
        result = supabase.table(table).select(f"{key_col}, {id_col}").execute()
        return {
            str(row[key_col]).strip().casefold(): row[id_col]
            for row in result.data
            if row.get(key_col)
        }


# =========================
# Existing saves
# =========================

def save_kierunki(kierunki, batch_size=100):
    if not kierunki:
        return 0

    total = 0
    for batch in chunks(kierunki, batch_size):
        data = []
        for k in batch:
            if is_dataclass(k):
                k = asdict(k)
            if not k.get("nazwa") or not k.get("wydzial"):
                continue
            data.append({
                "nazwa": k["nazwa"],
                "wydzial": k["wydzial"]
            })

        total += _upsert_with_retry("kierunki", data, on_conflict="nazwa,wydzial")
    return total


def save_grupy(grupy, batch_size=500):
    if not grupy:
        return 0

    seen = set()
    unique_grupy = []
    for g in grupy:
        key = (g.get("kod_grupy"), g.get("kierunek_id"))
        if key not in seen:
            seen.add(key)
            unique_grupy.append(g)

    total = 0
    for batch in chunks(unique_grupy, batch_size):
        data = []
        for g in batch:
            if is_dataclass(g):
                g = asdict(g)

            # FIX: tryb_studiow nie może być null (DB NOT NULL)
            tryb = g.get("tryb_studiow")
            if tryb is None or str(tryb).strip() == "":
                tryb = "nieznany"

            data.append({
                "kod_grupy": g.get("kod_grupy"),
                "kierunek_id": g.get("kierunek_id"),
                "link_strony_grupy": g.get("link_strony_grupy"),
                "link_ics_grupy": g.get("link_ics_grupy"),
                "tryb_studiow": tryb,
                "grupa_id": g.get("grupa_id"),
            })

        total += _upsert_with_retry("grupy", data, on_conflict="kod_grupy,kierunek_id")
    return total

def save_nauczyciele(nauczyciele, batch_size=500):
    if not nauczyciele:
        return 0

    nauczyciele_by_link = {}
    for n in nauczyciele:
        if is_dataclass(n):
            n = asdict(n)
        link = n.get("link_strony_nauczyciela")
        if not link:
            continue

        if link in nauczyciele_by_link:
            existing = nauczyciele_by_link[link]
            for key in ["instytut", "email", "link_ics_nauczyciela"]:
                if not existing.get(key) and n.get(key):
                    existing[key] = n.get(key)
        else:
            nauczyciele_by_link[link] = {
                "nazwa": n.get("nazwa"),
                "instytut": n.get("instytut"),
                "email": n.get("email"),
                "link_strony_nauczyciela": link,
                "link_ics_nauczyciela": n.get("link_ics_nauczyciela"),
            }

    print(f"ℹ️ Duplikaty linków nauczycieli: {len(nauczyciele) - len(nauczyciele_by_link)}")
    nauczyciele_list = list(nauczyciele_by_link.values())

    total = 0
    for batch in chunks(nauczyciele_list, batch_size):
        total += _upsert_with_retry("nauczyciele", batch, on_conflict="link_strony_nauczyciela")
    return total


def save_zajecia_grupy(events, grupa_uuid_map, batch_size=500):
    if not events:
        return 0
    if not grupa_uuid_map:
        print("⚠️ grupa_uuid_map puste — pomijam save_zajecia_grupy")
        return 0

    seen = set()
    batch_data = []
    skipped = 0

    for event in events:
        if is_dataclass(event):
            event = asdict(event)
        grupa_id = event.get("grupa_id")
        if not grupa_id:
            skipped += 1
            continue

        grupa_uuid = grupa_uuid_map.get(str(grupa_id))
        if not grupa_uuid:
            skipped += 1
            continue

        key = (event.get("uid"), grupa_uuid)
        if key in seen:
            continue
        seen.add(key)

        batch_data.append({
            "uid": event.get("uid"),
            "podgrupa": (event.get("podgrupa") or "")[:20],
            "od": event.get("od"),
            "do_": event.get("do_"),
            "przedmiot": event.get("przedmiot"),
            "rz": event.get("rz"),
            "nauczyciel": event.get("nauczyciel_nazwa") or event.get("nauczyciel"),
            "miejsce": event.get("miejsce"),
            "grupa_id": grupa_uuid,
            "link_ics_zrodlowy": event.get("link_ics_zrodlowy"),
        })

    print(f"ℹ️ Pominięte zajęcia grup: {skipped}")
    total = 0
    for batch in chunks(batch_data, batch_size):
        # lokalna deduplikacja
        local_seen = set()
        dedup = []
        for r in batch:
            k = (r["uid"], r["grupa_id"])
            if k in local_seen:
                continue
            local_seen.add(k)
            dedup.append(r)

        total += _upsert_with_retry("zajecia_grupy", dedup, on_conflict="uid,grupa_id")
    return total


def save_zajecia_nauczyciela(events, nauczyciel_uuid_map=None, batch_size=1000):
    if not events:
        return 0

    seen = set()
    batch_data = []
    duplicates = 0

    for event in events:
        if is_dataclass(event):
            event = asdict(event)

        uuid = event.get("nauczyciel_id")
        if not uuid:
            continue
        if not (event.get("uid") and event.get("od") and event.get("do_") and event.get("przedmiot")):
            continue

        key = (event.get("uid"), uuid)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)

        batch_data.append({
            "uid": event.get("uid"),
            "od": event.get("od"),
            "do_": event.get("do_"),
            "przedmiot": event.get("przedmiot"),
            "rz": event.get("rz"),
            "grupy": event.get("grupy"),
            "miejsce": event.get("miejsce"),
            "nauczyciel_id": uuid,
            "link_ics_zrodlowy": event.get("link_ics_zrodlowy"),
        })

    if duplicates:
        print(f"ℹ️ Duplikaty nauczyciel events pominięte: {duplicates}")

    total = 0
    for batch in chunks(batch_data, batch_size):
        # ostateczna deduplikacja w batchu
        local_seen = set()
        dedup = []
        for r in batch:
            k = (r["uid"], r["nauczyciel_id"])
            if k in local_seen:
                continue
            local_seen.add(k)
            dedup.append(r)

        total += _upsert_with_retry("zajecia_nauczyciela", dedup, on_conflict="uid,nauczyciel_id")
    return total


# =========================
# NEW: Semester state
# =========================

def get_semester_state() -> Optional[dict]:
    """
    Odczyt ostatniego zapisanego stanu semestru.
    Zakładamy tabelę: semester_state
    kolumny m.in.:
      - current_semester_id
      - current_semester_name
      - previous_semester_id
      - previous_semester_name
      - source_url
      - generated_at_source
      - updated_at
    """
    try:
        res = (
            supabase.table("semester_state")
            .select("*")
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        data = res.data or []
        return data[0] if data else None
    except Exception as e:
        print(f"❌ get_semester_state error: {e}")
        return None


def save_semester_state(
    current_semester_id: Optional[str],
    current_semester_name: Optional[str],
    previous_semester_id: Optional[str] = None,
    previous_semester_name: Optional[str] = None,
    source_url: Optional[str] = None,
    generated_at_source: Optional[str] = None,
) -> int:
    """
    Upsert stanu semestru.
    Wersja prosta: jeden wiersz logiczny po current_semester_id.
    """
    payload = [{
        "current_semester_id": current_semester_id,
        "current_semester_name": current_semester_name,
        "previous_semester_id": previous_semester_id,
        "previous_semester_name": previous_semester_name,
        "source_url": source_url,
        "generated_at_source": generated_at_source,
        "updated_at": dt.datetime.utcnow().isoformat(),
    }]
    return _upsert_with_retry("semester_state", payload, on_conflict="current_semester_id")


# =========================
# NEW: Schedule meta
# =========================

def save_group_schedule_meta(rows: list[dict], batch_size: int = 500) -> int:
    """
    Tabela: group_schedule_meta
    expected keys per row:
      - grupa_id (UUID z tabeli grupy)
      - semester_id
      - last_schedule_date (YYYY-MM-DD)
      - is_active (bool)
      - source_kind ('xml'/'ics')
    unique: (grupa_id, semester_id)
    """
    if not rows:
        return 0

    clean = []
    for r in rows:
        grupa_id = r.get("grupa_id")
        semester_id = r.get("semester_id")
        if not grupa_id or not semester_id:
            continue
        clean.append({
            "grupa_id": grupa_id,
            "semester_id": str(semester_id),
            "last_schedule_date": _to_iso_date(r.get("last_schedule_date")),
            "is_active": bool(r.get("is_active", True)),
            "source_kind": r.get("source_kind") or "xml",
            "updated_at": dt.datetime.utcnow().isoformat(),
        })

    total = 0
    for batch in chunks(clean, batch_size):
        total += _upsert_with_retry("group_schedule_meta", batch, on_conflict="grupa_id,semester_id")
    return total


def save_teacher_schedule_meta(rows: list[dict], batch_size: int = 500) -> int:
    """
    Tabela: teacher_schedule_meta
    expected keys:
      - nauczyciel_id (UUID)
      - semester_id
      - last_schedule_date
      - is_active
      - source_kind
    unique: (nauczyciel_id, semester_id)
    """
    if not rows:
        return 0

    clean = []
    for r in rows:
        nauczyciel_id = r.get("nauczyciel_id")
        semester_id = r.get("semester_id")
        if not nauczyciel_id or not semester_id:
            continue
        clean.append({
            "nauczyciel_id": nauczyciel_id,
            "semester_id": str(semester_id),
            "last_schedule_date": _to_iso_date(r.get("last_schedule_date")),
            "is_active": bool(r.get("is_active", True)),
            "source_kind": r.get("source_kind") or "xml",
            "updated_at": dt.datetime.utcnow().isoformat(),
        })

    total = 0
    for batch in chunks(clean, batch_size):
        total += _upsert_with_retry("teacher_schedule_meta", batch, on_conflict="nauczyciel_id,semester_id")
    return total


# =========================
# NEW: Cleanup
# =========================

def deactivate_expired_records(today: Optional[dt.date] = None) -> dict:
    """
    Oznacza rekordy meta jako nieaktywne, gdy:
      last_schedule_date < today
    Zwraca licznik zmian (best-effort).
    """
    if today is None:
        today = dt.date.today()
    today_iso = today.isoformat()

    changed_groups = 0
    changed_teachers = 0

    # group_schedule_meta
    try:
        # najpierw pobierz kandydatów
        res = (
            supabase.table("group_schedule_meta")
            .select("id,last_schedule_date,is_active")
            .lt("last_schedule_date", today_iso)
            .eq("is_active", True)
            .execute()
        )
        rows = res.data or []
        if rows:
            ids = [r["id"] for r in rows if r.get("id")]
            if ids:
                supabase.table("group_schedule_meta").update(
                    {"is_active": False, "updated_at": dt.datetime.utcnow().isoformat()}
                ).in_("id", ids).execute()
                changed_groups = len(ids)
    except Exception as e:
        print(f"❌ deactivate_expired_records group meta error: {e}")

    # teacher_schedule_meta
    try:
        res = (
            supabase.table("teacher_schedule_meta")
            .select("id,last_schedule_date,is_active")
            .lt("last_schedule_date", today_iso)
            .eq("is_active", True)
            .execute()
        )
        rows = res.data or []
        if rows:
            ids = [r["id"] for r in rows if r.get("id")]
            if ids:
                supabase.table("teacher_schedule_meta").update(
                    {"is_active": False, "updated_at": dt.datetime.utcnow().isoformat()}
                ).in_("id", ids).execute()
                changed_teachers = len(ids)
    except Exception as e:
        print(f"❌ deactivate_expired_records teacher meta error: {e}")

    return {
        "groups_deactivated": changed_groups,
        "teachers_deactivated": changed_teachers,
        "as_of": today_iso,
    }
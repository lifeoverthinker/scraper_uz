from dotenv import load_dotenv
import hashlib
import os
import time
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime

from supabase import create_client

project_root = Path(__file__).resolve().parent.parent
load_dotenv(project_root / ".env")
load_dotenv(Path.cwd() / ".env")
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    missing_str = ", ".join(missing)
    raise RuntimeError(
        f"Brak zmiennych srodowiskowych: {missing_str}. "
        f"Utworz plik .env w {project_root} albo ustaw je w sesji PowerShell."
    )

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def _norm(value: Any) -> str:
    return str(value).strip().casefold()


def _str(value: Any) -> str:
    return "" if value is None else str(value)


def _hash_values(*values: Any) -> str:
    raw = "|".join(_str(v).strip() for v in values)
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()



def _normalize_timestamp(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        return None

    upper = text.upper()
    if upper in {"NO_DATE", "NO_START", "NO_END", "NULL", "NONE"}:
        return None

    candidate = text.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(candidate)
        return text
    except ValueError:
        return None

def get_uuid_map(table: str, key_col: str, id_col: str) -> Dict:
    """Pobiera mapowanie kluczy do UUID z bazy."""
    if table == "kierunki":
        result = supabase.table(table).select(f"{key_col}, wydzial, {id_col}").execute()
        return {
            (_norm(row[key_col]), _norm(row["wydzial"])): row[id_col]
            for row in (result.data or [])
            if row.get(key_col) and row.get("wydzial")
        }

    result = supabase.table(table).select(f"{key_col}, {id_col}").execute()
    return {
        _norm(row[key_col]): row[id_col]
        for row in (result.data or [])
        if row.get(key_col)
    }


def chunks(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def save_kierunki(kierunki, batch_size=100):
    if not kierunki:
        return 0

    total = 0
    for batch in chunks(kierunki, batch_size):
        data = []
        for k in batch:
            if is_dataclass(k):
                k = asdict(k)

            nazwa = k.get("nazwa")
            wydzial = k.get("wydzial")
            if not nazwa or not wydzial:
                continue

            data.append(
                {
                    "nazwa": nazwa,
                    "wydzial": wydzial,
                    "external_id": k.get("external_id") or k.get("xml_kierunek_id"),
                }
            )

        if not data:
            continue

        try:
            supabase.table("kierunki").upsert(data, on_conflict="nazwa,wydzial").execute()
            total += len(data)
        except Exception as e:
            print(f"Blad zapisu kierunkow: {e}")

    return total


def save_grupy(grupy, batch_size=500):
    """Zapisuje grupy; preferuje unikalnosc po grupa_id i zapisuje semestr."""
    if not grupy:
        return 0

    seen = set()
    unique_grupy = []
    skipped_without_kierunek = 0

    for g in grupy:
        if is_dataclass(g):
            g = asdict(g)

        if not g.get("kierunek_id"):
            skipped_without_kierunek += 1
            continue

        grupa_id = g.get("grupa_id")
        if grupa_id:
            key = ("grupa_id", str(grupa_id))
        else:
            key = (
                "fallback",
                g.get("kod_grupy"),
                g.get("kierunek_id"),
                g.get("tryb_studiow"),
                g.get("semestr"),
            )

        if key in seen:
            continue
        seen.add(key)
        unique_grupy.append(g)

    if skipped_without_kierunek:
        print(f"Ominieto {skipped_without_kierunek} grup bez kierunek_id (zabezpieczenie FK)")

    conflict_candidates = [
        os.getenv("GROUP_UPSERT_CONFLICT", "grupa_id"),
        "kod_grupy,kierunek_id",
    ]
    active_conflict = None
    include_semestr = True
    total = 0

    for batch in chunks(unique_grupy, batch_size):
        data = [
            {
                "kod_grupy": g.get("kod_grupy"),
                "kierunek_id": g.get("kierunek_id"),
                "link_strony_grupy": g.get("link_strony_grupy"),
                "link_ics_grupy": g.get("link_ics_grupy"),
                "tryb_studiow": g.get("tryb_studiow") or "nieznane",
                "semestr": g.get("semestr"),
                "grupa_id": g.get("grupa_id"),
            }
            for g in batch
        ]
        if not data:
            continue

        payload = data if include_semestr else [{k: v for k, v in row.items() if k != "semestr"} for row in data]

        if active_conflict:
            try:
                supabase.table("grupy").upsert(payload, on_conflict=active_conflict).execute()
                total += len(payload)
            except Exception as e:
                print(f"Blad zapisu grup (on_conflict={active_conflict}): {e}")
            continue

        saved = False
        last_error = None

        for conflict in conflict_candidates:
            if not conflict:
                continue
            try:
                supabase.table("grupy").upsert(payload, on_conflict=conflict).execute()
                active_conflict = conflict
                total += len(payload)
                saved = True
                print(f"Uzywam konfliktu upsert dla grup: {active_conflict}")
                break
            except Exception as e:
                last_error = e

        if not saved and include_semestr:
            payload_without_semestr = [{k: v for k, v in row.items() if k != "semestr"} for row in data]
            for conflict in conflict_candidates:
                if not conflict:
                    continue
                try:
                    supabase.table("grupy").upsert(payload_without_semestr, on_conflict=conflict).execute()
                    include_semestr = False
                    active_conflict = conflict
                    total += len(payload_without_semestr)
                    saved = True
                    print("Kolumna semestr nie jest jeszcze w bazie - zapis grup bez semestru")
                    print(f"Uzywam konfliktu upsert dla grup: {active_conflict}")
                    break
                except Exception as e:
                    last_error = e

        if not saved:
            print(f"Blad zapisu grup: {last_error}")

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

        external_id = n.get("external_id") or n.get("nauczyciel_id")

        if link in nauczyciele_by_link:
            existing = nauczyciele_by_link[link]
            for key in ["instytut", "email", "link_ics_nauczyciela", "external_id"]:
                if not existing.get(key) and n.get(key):
                    existing[key] = n.get(key)
            if not existing.get("external_id") and external_id:
                existing["external_id"] = external_id
        else:
            nauczyciele_by_link[link] = {
                "nazwa": n.get("nazwa"),
                "instytut": n.get("instytut"),
                "email": n.get("email"),
                "link_strony_nauczyciela": link,
                "link_ics_nauczyciela": n.get("link_ics_nauczyciela"),
                "external_id": external_id,
            }

    print(f"Po deduplikacji nauczycieli: {len(nauczyciele_by_link)}")

    total = 0
    for batch in chunks(list(nauczyciele_by_link.values()), batch_size):
        try:
            supabase.table("nauczyciele").upsert(batch, on_conflict="link_strony_nauczyciela").execute()
            total += len(batch)
        except Exception as e:
            print(f"Blad zapisu batcha nauczycieli: {e}")
            if batch:
                print(f"Przykladowy rekord z bledem: {batch[0]}")

    return total


def save_zajecia_grupy(events, grupa_uuid_map, batch_size=500):
    """Zapis do nowej tabeli public.zajecia_grup."""
    if not events:
        return 0

    if not grupa_uuid_map:
        print("UWAGA: grupa_uuid_map jest puste. Najpierw dodaj grupy do bazy.")
        return 0

    total = 0
    skipped = 0
    seen = set()
    batch_data = []

    for event in events:
        if is_dataclass(event):
            event = asdict(event)

        grupa_id = event.get("grupa_id")
        if not grupa_id:
            skipped += 1
            continue

        grupa_uuid = grupa_uuid_map.get(_norm(grupa_id)) or grupa_uuid_map.get(str(grupa_id))
        if not grupa_uuid:
            skipped += 1
            continue

        uid = event.get("uid")
        start_time = _normalize_timestamp(event.get("od"))
        end_time = _normalize_timestamp(event.get("do_"))
        subject = event.get("przedmiot")
        teacher_name = event.get("nauczyciel_nazwa") or event.get("nauczyciel")
        location = event.get("miejsce")
        rz = event.get("rz")
        podgrupa = (event.get("podgrupa") or "")[:20]

        if not start_time or not end_time:
            skipped += 1
            continue

        row_hash = _hash_values(
            grupa_uuid,
            uid,
            start_time,
            end_time,
            subject,
            teacher_name,
            location,
            rz,
            podgrupa,
        )

        if row_hash in seen:
            continue
        seen.add(row_hash)

        source_link = event.get("link_ics_zrodlowy")
        source_type = "xml" if source_link and "/static_files/" in source_link else "ics"

        batch_data.append(
            {
                "uid": uid,
                "podgrupa": podgrupa or None,
                "od": start_time,
                "do_": end_time,
                "przedmiot": subject,
                "rz": rz,
                "prowadzacy": teacher_name,
                "miejsce": location,
                "grupa_id": grupa_uuid,
                "zrodlo_typ": source_type,
                "zrodlo_link": source_link,
                "semestr_id": event.get("semestr_id") or event.get("semester_id"),
                "hash": row_hash,
            }
        )

    if skipped:
        print(f"Pominieto {skipped} zajec grup (brak mapowania/grupa_id/czasu)")

    for batch in chunks(batch_data, batch_size):
        if not batch:
            continue

        try:
            supabase.table("zajecia_grup").upsert(batch, on_conflict="grupa_id,uid").execute()
            total += len(batch)
        except Exception as e:
            print(f"Blad podczas upsertowania zajec grup: {e}")
            print(f"Przykladowy rekord: {batch[0]}")

    return total
def _insert_teacher_events_without_unique_index(data_batch):
    """
    Fallback gdy brak unique (uid,nauczyciel_id).
    Robimy delikatna deduplikacje przez odczyt istniejacych uid dla nauczyciela.
    """
    inserted = 0

    grouped = {}
    for row in data_batch:
        grouped.setdefault(row["nauczyciel_id"], []).append(row)

    for nauczyciel_id, rows in grouped.items():
        uids = list({r.get("uid") for r in rows if r.get("uid")})
        existing_keys = set()

        for uid_chunk in chunks(uids, 200):
            try:
                res = (
                    supabase.table("zajecia_nauczycieli")
                    .select("uid,nauczyciel_id")
                    .eq("nauczyciel_id", nauczyciel_id)
                    .in_("uid", uid_chunk)
                    .execute()
                )
                for row in (res.data or []):
                    existing_keys.add((row.get("uid"), row.get("nauczyciel_id")))
            except Exception as e:
                print(f"Blad odczytu istniejacych zajec nauczyciela: {e}")

        to_insert = []
        for row in rows:
            key = (row.get("uid"), row.get("nauczyciel_id"))
            if key in existing_keys:
                continue
            existing_keys.add(key)
            to_insert.append(row)

        if not to_insert:
            continue

        try:
            supabase.table("zajecia_nauczycieli").insert(to_insert).execute()
            inserted += len(to_insert)
        except Exception as e:
            print(f"Blad insertowania zajec nauczyciela (fallback): {e}")
            print(f"Przykladowy rekord: {to_insert[0]}")

    return inserted
def save_zajecia_nauczyciela(events, nauczyciel_uuid_map=None, batch_size=1000):
    if not events:
        return 0

    total = 0
    batch_data = []
    seen = set()
    duplicates = 0

    for event in events:
        if is_dataclass(event):
            event = asdict(event)

        uuid = event.get("nauczyciel_id")
        if not uuid:
            continue

        start_time = _normalize_timestamp(event.get("od"))
        end_time = _normalize_timestamp(event.get("do_"))

        if not (event.get("uid") and start_time and end_time and event.get("przedmiot")):
            continue

        key = (event.get("uid"), uuid)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)

        source_link = event.get("link_ics_zrodlowy")
        source_type = "xml" if source_link and "/static_files/" in source_link else "ics"

        batch_data.append(
            {
                "uid": event.get("uid"),
                "od": start_time,
                "do_": end_time,
                "przedmiot": event.get("przedmiot"),
                "rz": event.get("rz"),
                "grupy": event.get("grupy"),
                "miejsce": event.get("miejsce"),
                "nauczyciel_id": uuid,
                "zrodlo_typ": source_type,
                "zrodlo_link": source_link,
                "semestr_id": event.get("semestr_id") or event.get("semester_id"),
            }
        )

    if duplicates:
        print(f"Wykryto i pominieto {duplicates} duplikatow (uid,nauczyciel_id)")

    max_retries = int(os.getenv("TEACHER_EVENTS_MAX_RETRIES", "3"))
    teacher_conflict = os.getenv("TEACHER_EVENTS_CONFLICT", "uid,nauczyciel_id")
    no_unique_mode = False

    def _upsert_with_retry(data_batch):
        nonlocal total, no_unique_mode

        if no_unique_mode:
            total += _insert_teacher_events_without_unique_index(data_batch)
            return

        attempt = 0
        backoff = 2

        while attempt < max_retries:
            try:
                supabase.table("zajecia_nauczycieli").upsert(data_batch, on_conflict=teacher_conflict).execute()
                total += len(data_batch)
                return
            except Exception as e:
                msg = str(e).lower()

                if "there is no unique or exclusion constraint" in msg or "42p10" in msg:
                    no_unique_mode = True
                    print("Brak unikalnego indeksu dla upsert zajec nauczycieli - fallback do insert z deduplikacja")
                    total += _insert_teacher_events_without_unique_index(data_batch)
                    return

                if "cannot affect row a second time" in msg:
                    dedup_seen = set()
                    filtered = []
                    for row in data_batch:
                        key = (row["uid"], row["nauczyciel_id"])
                        if key in dedup_seen:
                            continue
                        dedup_seen.add(key)
                        filtered.append(row)

                    if len(filtered) != len(data_batch):
                        data_batch = filtered
                        continue

                    if len(data_batch) > 1:
                        mid = len(data_batch) // 2
                        _upsert_with_retry(data_batch[:mid])
                        _upsert_with_retry(data_batch[mid:])
                        return

                if any(token in msg for token in ["timed out", "did not complete", "timeout", "ssl"]):
                    attempt += 1
                    if attempt < max_retries:
                        time.sleep(backoff)
                        backoff *= 2
                        continue

                print(f"Blad podczas upsertowania zajec nauczyciela: {e}")
                if data_batch:
                    print(f"Przykladowy rekord: {data_batch[0]}")
                return

    for batch in chunks(batch_data, batch_size):
        local_seen = set()
        final_batch = []
        for row in batch:
            key = (row["uid"], row["nauczyciel_id"])
            if key in local_seen:
                continue
            local_seen.add(key)
            final_batch.append(row)

        if not final_batch:
            continue

        _upsert_with_retry(final_batch)

    return total





def get_semester_state() -> Optional[Dict[str, Any]]:
    """Pobiera najnowszy zapis stanu semestru."""
    try:
        query = supabase.table("semester_state").select("*")
        try:
            query = query.order("updated_at", desc=True)
        except Exception:
            pass
        rows = query.limit(1).execute().data or []
        return rows[0] if rows else None
    except Exception as e:
        print(f"Blad odczytu semester_state: {e}")
        return None


def save_semester_state(state: Dict[str, Any]) -> bool:
    """Zapisuje stan semestru (update po current_semester_id lub insert)."""
    if not state:
        return False

    current_semester_id = _str(state.get("current_semester_id")).strip()
    if not current_semester_id:
        return False

    payload = {
        "current_semester_id": current_semester_id,
        "current_semester_name": state.get("current_semester_name"),
        "previous_semester_id": state.get("previous_semester_id"),
        "previous_semester_name": state.get("previous_semester_name"),
        "source_url": state.get("source_url"),
        "generated_at_source": _normalize_timestamp(state.get("generated_at_source")),
    }

    try:
        existing = (
            supabase.table("semester_state")
            .select("current_semester_id")
            .eq("current_semester_id", current_semester_id)
            .limit(1)
            .execute()
            .data
            or []
        )

        if existing:
            supabase.table("semester_state").update(payload).eq("current_semester_id", current_semester_id).execute()
        else:
            supabase.table("semester_state").insert(payload).execute()
        return True
    except Exception as e:
        print(f"Blad zapisu semester_state: {e}")
        return False


def save_teacher_schedule_meta(meta_rows, batch_size=500):
    """Zapisuje metadane harmonogramu nauczyciela (best effort)."""
    if not meta_rows:
        return 0

    total = 0
    for batch in chunks(meta_rows, batch_size):
        data = []
        for row in batch:
            if is_dataclass(row):
                row = asdict(row)
            if not row.get("nauczyciel_id"):
                continue
            data.append(
                {
                    "nauczyciel_id": row.get("nauczyciel_id"),
                    "semester_id": row.get("semester_id"),
                    "last_schedule_date": row.get("last_schedule_date"),
                    "is_active": row.get("is_active", True),
                    "source_kind": row.get("source_kind") or "ics",
                }
            )

        if not data:
            continue

        try:
            supabase.table("teacher_schedule_meta").upsert(data, on_conflict="nauczyciel_id,semester_id").execute()
            total += len(data)
        except Exception as e:
            print(f"Blad zapisu teacher_schedule_meta: {e}")

    return total

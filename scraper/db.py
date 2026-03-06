import os
import time
import logging
import hashlib
from typing import List, Dict, Any, Optional
from supabase import create_client, Client
from dotenv import load_dotenv

# Setup logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Init Supabase
url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

if not url or not key:
    logger.warning("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set. DB operations will fail.")

supabase: Client = create_client(url, key) if url and key else None


def _upsert_with_retry(table: str, data: List[Dict[str, Any]], on_conflict: str = "id", retries: int = 3) -> int:
    """
    Generic upsert with simple retry logic.
    """
    if not data:
        return 0

    for attempt in range(retries):
        try:
            # Supabase-py upsert
            response = supabase.table(table).upsert(data, on_conflict=on_conflict).execute()
            # W nowszych wersjach supabase-py response.data to lista wstawionych rekordów
            return len(response.data) if response.data else len(data)
        except Exception as e:
            logger.error(f"Upsert failed for {table} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Final failure upserting to {table}")
                return 0
    return 0


def save_kierunki(data: List[Dict]) -> int:
    # on_conflict: nazwa,wydzial (zdefiniowane w SQL jako unique constraint) lub id
    # Tutaj zakładamy upsert po ID jeśli jest, lub po constraintach.
    # Bezpieczniej użyć on_conflict="nazwa,wydzial" jeśli ID nie jest znane,
    # ale logika w xml_sync często najpierw pobiera ID.
    # Użyjmy 'id' jeśli dane mają ID, w przeciwnym razie trzeba polegać na constraintach.
    return _upsert_with_retry("kierunki", data, on_conflict="external_id")


def save_grupy(data: List[Dict]) -> int:
    # Kluczem biznesowym jest często kod_grupy + kierunek_id, ale tutaj używamy upsert
    # xml_sync często generuje dane bez ID (insert) lub z ID (update).
    # Zakładamy constraint na (grupa_id) z XML lub (kod_grupy, kierunek_id).
    # Dla bezpieczeństwa: upsert po 'grupa_id' (external ID z planu) jeśli dostępne.
    return _upsert_with_retry("grupy", data, on_conflict="grupa_id")


def save_nauczyciele(data: List[Dict]) -> int:
    # Unikalność po link_strony_nauczyciela lub imie+nazwisko
    return _upsert_with_retry("nauczyciele", data, on_conflict="link_strony_nauczyciela")


def save_zajecia_grupy(events: List[Dict], grupo_map: Dict[str, str], batch_size: int = 200) -> int:
    """
    Wzbogaca wydarzenia o wewnętrzne UUID grupy, oblicza hash i wykonuje upsert.
    """
    if not events:
        return 0

    payload = []
    for event in events:
        external_grupa_id = str(event.get("grupa_id") or "").strip()
        internal_grupa_uuid = grupo_map.get(external_grupa_id)

        if not internal_grupa_uuid:
            logger.warning(f"Pominięto wydarzenie, brak wew. UUID dla zew. ID grupy {external_grupa_id}")
            continue

        db_event = event.copy()
        db_event["grupa_id"] = internal_grupa_uuid

        # Generowanie stabilnego hasha do upsertu
        hash_str = f"{internal_grupa_uuid}-{db_event.get('start_time')}-{db_event.get('subject')}-{db_event.get('location')}"
        db_event["hash"] = hashlib.md5(hash_str.encode()).hexdigest()

        # Usuń pola, które nie należą do tabeli `zajecia_grupy`
        db_event.pop("kod_grupy", None)
        db_event.pop("kierunek_nazwa", None)

        payload.append(db_event)

    if not payload:
        return 0

    total_saved = 0
    # Przetwarzanie w paczkach
    for i in range(0, len(payload), batch_size):
        batch = payload[i:i + batch_size]
        saved_in_batch = _upsert_with_retry("zajecia_grupy", batch, on_conflict="hash")
        total_saved += saved_in_batch

    return total_saved


def save_zajecia_nauczyciela(data: List[Dict]) -> int:
    return _upsert_with_retry("zajecia_nauczyciela", data, on_conflict="hash")


def get_semester_state() -> Optional[Dict]:
    try:
        res = supabase.table("semester_state").select("*").order("updated_at", desc=True).limit(1).execute()
        if res.data:
            return res.data[0]
    except Exception:
        pass
    return None


def save_semester_state(data: Dict) -> int:
    return _upsert_with_retry("semester_state", [data], on_conflict="current_semester_id")


def get_uuid_map(table: str, key_col: str, val_col: str) -> Dict[str, str]:
    """
    Pobiera wiersze z tabeli i tworzy mapowanie między dwiema kolumnami.
    """
    mapping = {}
    if not supabase:
        return mapping
    try:
        res = supabase.table(table).select(f"{key_col},{val_col}").execute()
        if res.data:
            for row in res.data:
                key = row.get(key_col)
                val = row.get(val_col)
                if key is not None and val is not None:
                    mapping[str(key)] = str(val)
    except Exception as e:
        logger.error(f"Nie udało się pobrać mapy uuid dla tabeli {table}: {e}")
    return mapping
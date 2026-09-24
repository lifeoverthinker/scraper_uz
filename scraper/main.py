from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from scraper.db import (
    save_semester_state,
    get_semester_state,
    supabase,
)
from scraper.xml_client import XmlClient
from scraper.xml_sync import sync_directions_and_groups_from_xml

# Aliasy trybow uruchomienia przez SCRAPER_ONLY.
MODE_FULL = {"full", "all", "pipeline"}
MODE_CATALOG = {"catalog_only", "catalog", "semester_guard", "guard"}
MODE_XML_BOOTSTRAP = {"xml_bootstrap", "xml_semester"}
MODE_XML_SYNC = {"xml_sync", "xml_groups", "kierunki", "grupy"}
MODE_GROUP_EVENTS = {"grupy_zajecia", "groups_events", "events_groups"}
MODE_TEACHER_EVENTS = {"teachers", "teacher_events", "nauczyciele"}


def reset_database():
    """Czyści tabele bazy danych przed synchronizacją za pomocą szybkiej procedury TRUNCATE."""
    print("⚠️ CZYSZCZENIE BAZY DANYCH (Clean Start)...")
    try:
        supabase.rpc("truncate_semester_tables", {}).execute()
        print("  ✅ Baza danych wyczyszczona błyskawicznie (TRUNCATE CASCADE).")
    except Exception as e:
        print(f"  - Błąd rpc truncate: {e}, próba usuwania partiami...")
        for table, pk in [("zajecia_grupy", "uid"), ("zajecia_nauczyciela", "uid"), ("grupy", "grupa_id"), ("nauczyciele", "external_id"), ("kierunki", "external_id")]:
            try:
                supabase.table(table).delete().neq(pk, "" if pk != "id" else 0).execute()
            except Exception as inner_e:
                print(f"    Błąd '{table}': {inner_e}")


def _run_xml_bootstrap() -> tuple[bool, str]:
    print("TRYB: xml_bootstrap (Weryfikacja stanu semestru)")
    client = XmlClient()

    # Pobieramy metadane z nagłówka XML uczelni
    meta = client.fetch_semester_meta_from_file("grupy_lista_kierunkow.xml")
    prev_state = get_semester_state()

    current_remote_id = str(meta.current_semester_id) if meta.current_semester_id else None
    prev_saved_id = str(prev_state.get("id_semestru_aktualny")) if prev_state and prev_state.get("id_semestru_aktualny") else None

    semester_changed = bool(prev_saved_id and prev_saved_id != current_remote_id)

    if semester_changed:
        print(f"🔔 Wykryto zmianę semestru na UZ: nowy ID={current_remote_id}, w bazie był ID={prev_saved_id}")
        
        # Sprawdzamy czy są jeszcze jakiekolwiek trwające/przyszłe zajęcia ze starego planu (np. sesja poprawkowa)
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            res = supabase.table("zajecia_grupy").select("uid").gt("koniec", now_iso).limit(1).execute()
            has_future_classes = bool(res.data)
        except Exception as e:
            print(f"Ostrzeżenie przy sprawdzaniu terminów zajęć: {e}")
            has_future_classes = False

        if not has_future_classes:
            print("✅ Wszystkie zajęcia ze starego planu już się zakończyły.")
            print("🧹 Bezpieczne czyszczenie bazy pod nowy semestr...")
            reset_database()
        else:
            print("⏳ W starym planie trwają jeszcze zajęcia/sesja. Nie czyścimy bazy, dopisujemy nowy plan.")

    # Zapisujemy bieżący stan semestrów do bazy
    save_semester_state({
        "current_semester_id": meta.current_semester_id,
        "current_semester_name": meta.current_semester_name_pl,
        "previous_semester_id": meta.previous_semester_id,
        "previous_semester_name": meta.previous_semester_name_pl
    })

    return semester_changed, "semester_changed" if semester_changed else "no_change"


def _run_xml_sync() -> None:
    print("TRYB: xml_catalog_sync (Synchronizacja katalogów)")
    result = sync_directions_and_groups_from_xml(verbose=True)
    print(f"Wynik synchronizacji katalogów: {result}")


def _run_catalog_only() -> None:
    print("TRYB: catalog_only")
    _run_xml_bootstrap()
    _run_xml_sync()


def _run_group_events() -> None:
    print("TRYB: synchronizacja_planow_grup")
    from scraper.run_events import main as run_group_events
    run_group_events()


def _run_teacher_events() -> None:
    print("TRYB: synchronizacja_planow_nauczycieli")
    from scraper.teacher_sync import sync_teacher_events_and_meta
    result = sync_teacher_events_and_meta(verbose=True)
    print(f"Wynik synchronizacji nauczycieli: {result}")


def _run_full() -> None:
    print("TRYB: pelna_synchronizacja (Full Pipeline)")
    _run_catalog_only()
    _run_group_events()
    _run_teacher_events()


def main() -> None:
    """Główny punkt wejścia: uruchamia wybrany etap synchronizacji."""
    start_time = time.time()

    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()

    if not mode or mode in MODE_FULL:
        _run_full()
    elif mode in MODE_CATALOG:
        _run_catalog_only()
    elif mode in MODE_XML_BOOTSTRAP:
        _run_xml_bootstrap()
    elif mode in MODE_XML_SYNC:
        _run_xml_sync()
    elif mode in MODE_GROUP_EVENTS:
        _run_group_events()
    elif mode in MODE_TEACHER_EVENTS:
        _run_teacher_events()
    else:
        print(f"Nieznany tryb SCRAPER_ONLY='{mode}' -> uruchamiam pełną synchronizację")
        _run_full()

    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"\n--- MODUŁ SYNCHRONIZACJI ZASOBÓW XML ZAKOŃCZYŁ PRACĘ ---")
    print(f"⏱️ Całkowity czas wykonania: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()

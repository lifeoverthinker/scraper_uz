from __future__ import annotations

import os
import time
from scraper.db import (
    save_semester_state,
    get_semester_state,
    supabase,
)
from scraper.xml_client import XmlClient
from scraper.semester_manager import (
    parse_semester_state_from_meta,
    detect_semester_switch,
    SemesterState,
)
from scraper.xml_sync import sync_directions_and_groups_from_xml


def reset_database():
    """
    Czyści tabele bazy danych przed synchronizacją (opcjonalne).
    Używane do weryfikacji poprawności importu na 'czystym' środowisku.
    """
    print("⚠️ CZYSZCZENIE BAZY DANYCH (Clean Start)...")
    tables = [
        "zajecia_grupy",
        "zajecia_nauczyciela",
        "grupy",
        "nauczyciele",
        "kierunki",
        "semester_state"
    ]
    for table in tables:
        try:
            # Usuwamy rekordy, które nie mają ID równego zeru (efektywnie wszystkie)
            supabase.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print(f"  - Tabela '{table}' wyczyszczona.")
        except Exception as e:
            print(f"  - Błąd podczas czyszczenia '{table}': {e}")


def _cleanup_semester_state_duplicates() -> None:
    try:
        rows = (
                supabase.table("semester_state")
                .select("id_semestru_aktualny,data_aktualizacji")
                .order("data_aktualizacji", desc=True)
                .execute()
                .data
                or []
        )
        if len(rows) <= 1:
            return

        newest_id = rows[0].get("id_semestru_aktualny")
        if not newest_id:
            return

        # Usuwanie nadmiarowych wpisów stanu semestru
        supabase.table("semester_state").delete().neq("id", 1).execute()
    except Exception as e:
        print(f"cleanup semester_state duplicates failed: {e}")


def _run_xml_bootstrap() -> tuple[bool, str]:
    print("TRYB: xml_bootstrap (Weryfikacja stanu semestru)")
    client = XmlClient()

    # Pobieramy metadane z nagłówka XML
    meta = client.fetch_semester_meta_from_file("grupy_lista_kierunkow.xml")

    # Zapisujemy bieżący stan semestrów do bazy
    save_semester_state({
        "current_semester_id": meta.current_semester_id,
        "current_semester_name": meta.current_semester_name_pl,
        "previous_semester_id": meta.previous_semester_id,
        "previous_semester_name": meta.previous_semester_name_pl
    })

    # Detekcja zmiany semestru (porównanie z poprzednim zapisem)
    prev_state = get_semester_state()
    if prev_state and prev_state.get("id_semestru_aktualny") != meta.current_semester_id:
        print(f"!!! WYKRYTO ZMIANĘ SEMESTRU: {meta.current_semester_id} !!!")
        return True, "semester_changed"

    return False, "no_change"


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
    start_time = time.time()

    # reset_database()  # Odkoduj tę linię, jeśli chcesz wyczyścić bazę przed startem

    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()

    if mode in {"full", "all", "pipeline"}:
        _run_full()
    elif mode in {"catalog_only", "catalog", "semester_guard", "guard"}:
        _run_catalog_only()
    elif mode in {"xml_bootstrap", "xml_semester"}:
        _run_xml_bootstrap()
    elif mode in {"xml_sync", "xml_groups", "kierunki", "grupy"}:
        _run_xml_sync()
    elif mode in {"grupy_zajecia", "groups_events", "events_groups"}:
        _run_group_events()
    elif mode in {"teachers", "teacher_events", "nauczyciele"}:
        _run_teacher_events()
    else:
        if mode:
            print(f"Nieznany tryb SCRAPER_ONLY='{mode}' -> uruchamiam domyślną synchronizację katalogów")
        else:
            print("Brak zdefiniowanego trybu -> uruchamiam domyślną synchronizację katalogów")
        _run_catalog_only()

    # Statystyki końcowe
    duration = time.time() - start_time
    minutes = int(duration // 60)
    seconds = int(duration % 60)
    print(f"\n--- MODUŁ SYNCHRONIZACJI ZASOBÓW XML ZAKOŃCZYŁ PRACĘ ---")
    print(f"⏱️ Całkowity czas wykonania: {minutes}m {seconds}s")


if __name__ == "__main__":
    main()
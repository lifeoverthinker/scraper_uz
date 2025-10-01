from scraper.scrapers.kierunki_scraper import scrape_kierunki
from scraper.scrapers.grupy_scraper import scrape_grupy_for_kierunki
from scraper.parsers.nauczyciel_parser import (
    parse_nauczyciele_from_group_page,
    parse_nauczyciel_details,
    fetch_page,
)
from scraper.db import (
    save_kierunki,
    save_grupy,
    save_nauczyciele,
    save_zajecia_grupy,
    save_zajecia_nauczyciela,
    get_uuid_map,
    supabase,
)
from scraper.downloader import download_ics_for_groups_async
from scraper.ics_updater import pobierz_plan_ics_nauczyciela, parse_ics_file
import os
import re

# ---------------------- TRYBY SKRÓCONE ----------------------


def _extract_external_id(link: str) -> str | None:
    if not link:
        return None
    m = re.search(r"ID=(\d+)", link)
    return m.group(1) if m else None


def _update_only_kierunki():
    print("TRYB: tylko kierunki")
    kierunki = scrape_kierunki()
    saved = save_kierunki(kierunki)
    print(f"Zapisano/upewniono się o {saved} kierunkach. Koniec.")


def _update_only_groups():
    """Scrapuje kierunki + grupy (bez nauczycieli i zajęć)."""
    print("TRYB: tylko grupy (kierunki + grupy)")
    kierunki = scrape_kierunki()
    save_kierunki(kierunki)
    grupy = scrape_grupy_for_kierunki(kierunki)
    save_grupy(grupy)
    print(f"Kierunków: {len(kierunki)}, grup: {len(grupy)} – koniec.")


def _update_only_groups_events():
    """Aktualizuje wyłącznie zajęcia grup na podstawie istniejących rekordów 'grupy' w bazie."""
    print("TRYB: tylko zajęcia grup (ICS)")
    # Paginate grupy z bazy aby pobrać wszystkie grupa_id
    page = 0
    page_size = 1000
    grupa_ids = []
    while True:
        start = page * page_size
        end = start + page_size - 1
        res = supabase.table("grupy").select("grupa_id").range(start, end).execute()
        data = res.data or []
        if not data:
            break
        for row in data:
            gid = row.get("grupa_id")
            if gid:
                grupa_ids.append(gid)
        if len(data) < page_size:
            break
        page += 1
    print(f"Znaleziono {len(grupa_ids)} ID grup do odświeżenia")
    if not grupa_ids:
        print("Brak grup w bazie – uruchom pełny scrap albo tryb 'grupy'.")
        return
    wyniki = download_ics_for_groups_async(grupa_ids)
    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")
    wszystkie_zajecia_grupy = []
    for w in wyniki:
        if w["status"] == "success":
            zajecia = parse_ics_file(w["ics_content"], link_ics_zrodlowy=w["link_ics_zrodlowy"])
            for z in zajecia:
                z["grupa_id"] = w["grupa_id"]
            wszystkie_zajecia_grupy.extend(zajecia)
        else:
            print(f"❌ Błąd pobierania: {w['link_ics_zrodlowy']}")
    saved = save_zajecia_grupy(wszystkie_zajecia_grupy, grupa_uuid_map)
    print(f"Zapisano (upsertowane) zajęcia grup: {saved}")


def _update_only_teachers():
    print("TRYB: tylko nauczyciele (pełna paginacja + zajęcia)")
    page_size = 500
    page = 0
    all_events = []
    total_teachers = 0
    total_events = 0
    while True:
        start = page * page_size
        end = start + page_size - 1
        res = supabase.table("nauczyciele").select("id, link_strony_nauczyciela").range(start, end).execute()
        data = res.data or []
        if not data:
            break
        print(f"Strona {page+1}: {len(data)} nauczycieli")
        for row in data:
            ext_id = _extract_external_id(row.get("link_strony_nauczyciela"))
            if not ext_id:
                continue
            plan = pobierz_plan_ics_nauczyciela(ext_id)
            if plan["status"] != "success" or not plan["ics_content"]:
                continue
            events = parse_ics_file(plan["ics_content"], plan["link_ics_zrodlowy"])
            if not events:
                continue
            for e in events:
                e["nauczyciel_id"] = row["id"]
            all_events.extend(events)
            total_events += len(events)
            total_teachers += 1
        page += 1
    if all_events:
        saved = save_zajecia_nauczyciela(all_events)
        print(f"Zapisano (przekazanych do upsert): {saved}")
    else:
        print("Brak wydarzeń do zapisu.")
    print(f"Nauczycieli z wydarzeniami: {total_teachers}")
    print(f"Łącznie wydarzeń parsowanych: {total_events}")

# ---------------------- PEŁNY PIPELINE ----------------------


def main():
    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()
    if mode in {"kierunki", "kierunki_only", "directions"}:
        _update_only_kierunki(); return
    if mode in {"grupy", "groups"}:
        _update_only_groups(); return
    if mode in {"grupy_zajecia", "groups_events"}:
        _update_only_groups_events(); return
    if mode in {"teachers", "teachers_events", "nauczyciele"}:
        _update_only_teachers(); return

    # Pełny proces
    print("ETAP 1: Pobieranie kierunków studiów...")
    kierunki = scrape_kierunki()
    save_kierunki(kierunki)
    print(f"Przetworzono {len(kierunki)} kierunków\n")

    print("ETAP 2: Pobieranie grup dla kierunków...")
    wszystkie_grupy = scrape_grupy_for_kierunki(kierunki)

    # Sekcja: Mapowanie UUID kierunków do grup (Figma: Kierunek, Wydział)
    kierunek_uuid_map = get_uuid_map("kierunki", "nazwa", "id")
    for g in wszystkie_grupy:
        # Dodaj brakujące pola kierunek_nazwa i wydzial jeśli nie istnieją
        if not g.get("kierunek_nazwa") or not g.get("wydzial"):
            # Znajdź kierunek na podstawie przekazanych danych
            kierunek = next((k for k in kierunki if k.get("nazwa")), None)
            if kierunek:
                g["kierunek_nazwa"] = kierunek.get("nazwa", "")
                g["wydzial"] = kierunek.get("wydzial", "")

        # Mapowanie UUID z case-insensitive
        key = (
            (g.get("kierunek_nazwa") or "").strip().casefold(),
            (g.get("wydzial") or "").strip().casefold()
        )
        g["kierunek_id"] = kierunek_uuid_map.get(key)

        # Usuń tymczasowe pola
        g.pop("kierunek_nazwa", None)
        g.pop("wydzial", None)

    save_grupy(wszystkie_grupy)
    print(f"Przetworzono {len(wszystkie_grupy)} grup\n")

    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")

    print("ETAP 3: Pobieranie nauczycieli z planów grup...")
    nauczyciele_dict = {}
    for grupa in wszystkie_grupy:
        html = fetch_page(grupa.get("link_strony_grupy"))
        nauczyciele = parse_nauczyciele_from_group_page(html, grupa_id=grupa.get("grupa_id"))
        for n in nauczyciele:
            link = n.get("link")
            if link and link not in nauczyciele_dict:
                html_n = fetch_page(link)
                details = parse_nauczyciel_details(html_n, n.get("nauczyciel_id")) if html_n else {}
                nauczyciele_dict[link] = {
                    "nazwa": n.get("nazwa"),
                    "instytut": details.get("instytut"),
                    "email": details.get("email"),
                    "link_strony_nauczyciela": link,
                    "link_ics_nauczyciela": details.get("link_ics_nauczyciela"),
                    # Zachowujemy zewnętrzne ID z planu (liczbowe) – potrzebne do ICS
                    "nauczyciel_id": n.get("nauczyciel_id"),
                }
    nauczyciele_final = list(nauczyciele_dict.values())
    save_nauczyciele(nauczyciele_final)
    print(f"Przetworzono {len(nauczyciele_final)} nauczycieli\n")

    nauczyciel_uuid_map = get_uuid_map("nauczyciele", "link_strony_nauczyciela", "id")

    print("ETAP 4: Pobieranie i zapisywanie zajęć grup...")
    wszystkie_id_grup = [g["grupa_id"] for g in wszystkie_grupy if g.get("grupa_id")]
    wyniki = download_ics_for_groups_async(wszystkie_id_grup)
    wszystkie_zajecia_grupy = []
    for w in wyniki:
        if w["status"] == "success":
            zajecia = parse_ics_file(w["ics_content"], link_ics_zrodlowy=w["link_ics_zrodlowy"])
            for z in zajecia:
                z["grupa_id"] = w["grupa_id"]
            wszystkie_zajecia_grupy.extend(zajecia)
            print(f"Pobrano {len(zajecia)} zajęć dla grupy {w['grupa_id']}")
        else:
            print(f"❌ Błąd pobierania ICS: {w['link_ics_zrodlowy']}")
    save_zajecia_grupy(wszystkie_zajecia_grupy, grupa_uuid_map)
    print(f"Zapisano {len(wszystkie_zajecia_grupy)} zajęć grup\n")

    print("ETAP 5: Pobieranie i zapisywanie zajęć nauczycieli...")
    wszystkie_zajecia_nauczyciela = []
    missing_uuid = []
    created_on_demand = 0
    allow_ondemand = os.getenv("TEACHER_UUID_ONDEMAND", "1") == "1"

    for n in nauczyciele_final:
        external_id = n.get("nauczyciel_id")  # ID numeryczne z planu (dla ICS)
        nazwa = n.get("nazwa")
        link_strony = n.get("link_strony_nauczyciela")
        if not external_id:
            print(f"⚠️ Brak external_id dla nauczyciela {nazwa} ({link_strony})")
            continue

        uuid_key = (link_strony or "").strip().casefold()
        uuid_row = nauczyciel_uuid_map.get(uuid_key)

        if not uuid_row:
            # Spróbuj on-demand jeśli włączone
            if allow_ondemand:
                try:
                    print(f"ℹ️ Fallback: próbuję dodać nauczyciela on-demand: {nazwa} ({link_strony})")
                    supabase.table('nauczyciele').upsert([
                        {
                            'nazwa': nazwa,
                            'link_strony_nauczyciela': link_strony,
                            'instytut': n.get('instytut'),
                            'email': n.get('email'),
                            'link_ics_nauczyciela': n.get('link_ics_nauczyciela')
                        }
                    ], on_conflict='link_strony_nauczyciela').execute()
                    # Odśwież tylko dla tego linku (select zamiast pełnego mapy)
                    res = supabase.table('nauczyciele').select('id,link_strony_nauczyciela').eq('link_strony_nauczyciela', link_strony).limit(1).execute()
                    if res.data:
                        uuid_row = res.data[0]['id']
                        nauczyciel_uuid_map[uuid_key] = uuid_row
                        created_on_demand += 1
                        print(f"✔ Dodano on-demand (UUID={uuid_row}) dla {nazwa}")
                except Exception as e:
                    print(f"❌ Nie udało się dodać nauczyciela on-demand: {nazwa} -> {e}")
            if not uuid_row:
                missing_uuid.append({
                    'external_id': external_id,
                    'nazwa': nazwa,
                    'link': link_strony
                })
                print(f"⚠️ Brak UUID w bazie dla nauczyciela {nazwa} (ext_id={external_id}) {uuid_key}")
                continue

        plan = pobierz_plan_ics_nauczyciela(external_id)
        if plan["status"] == "success" and plan["ics_content"]:
            zajecia = parse_ics_file(plan["ics_content"], link_ics_zrodlowy=plan["link_ics_zrodlowy"])
            if not zajecia:
                continue
            for z in zajecia:
                z["nauczyciel_id"] = uuid_row  # UUID z bazy
            wszystkie_zajecia_nauczyciela.extend(zajecia)
            print(f"Pobrano {len(zajecia)} zajęć dla nauczyciela {nazwa} (ext_id={external_id})")
        else:
            print(f"⚠️ Nie udało się pobrać ICS dla nauczyciela {nazwa} (ext_id={external_id})")

    saved_teacher_events = save_zajecia_nauczyciela(wszystkie_zajecia_nauczyciela, nauczyciel_uuid_map)
    print(f"Zapisano {saved_teacher_events} zajęć nauczycieli (po deduplikacji)")

    if missing_uuid:
        print(f"⚠️ Podsumowanie: {len(missing_uuid)} nauczycieli nadal bez UUID (lista skrócona do 10):")
        for item in missing_uuid[:10]:
            print(f" - {item['nazwa']} (ext_id={item['external_id']}) {item['link']}")
    if created_on_demand:
        print(f"ℹ️ Utworzono on-demand {created_on_demand} brakujących rekordów nauczycieli")

    print("Zakończono proces MVP.")


if __name__ == "__main__":
    main()

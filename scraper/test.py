import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def test_kierunki(args):
    print("Test ETAP 1: Pobieranie kierunków studiów...")
    kierunki = scrape_kierunki()
    print(f"Pobrano {len(kierunki)} kierunków")
    if args.verbose:
        for i, k in enumerate(kierunki[:5]):
            print(f"Przykład {i + 1}: {k}")
    if args.save:
        saved = save_kierunki(kierunki)
        print(f"Zapisano {saved} kierunków do bazy")
        result = supabase.table('kierunki').select('count').execute()
        count = result.data[0]['count'] if result.data else 0
        print(f"W bazie znajduje się {count} kierunków")


def test_grupy(args):
    print("Test ETAP 2: Pobieranie grup dla kierunków...")
    if args.from_db:
        print("Pobieranie kierunków z bazy danych...")
        result = supabase.table('kierunki').select('*').execute()
        kierunki = result.data
        print(f"Pobrano {len(kierunki)} kierunków z bazy")
    else:
        print("Pobieranie kierunków z UZ...")
        kierunki = scrape_kierunki()
        print(f"Pobrano {len(kierunki)} kierunków z UZ")

    wszystkie_grupy = scrape_grupy_for_kierunki(kierunki)
    print(f"Pobrano {len(wszystkie_grupy)} grup")

    if args.verbose:
        for i, g in enumerate(wszystkie_grupy[:5]):
            print(f"Przykład {i + 1}: {g}")

    if args.save:
        # Sekcja: Mapowanie UUID kierunków (Figma: Kierunek, Wydział)
        kierunek_uuid_map = get_uuid_map("kierunki", "nazwa", "id")
        print(f"Pobrano {len(kierunek_uuid_map)} mapowań UUID kierunków")

        # Debug: sprawdź przykładowe klucze
        if kierunek_uuid_map:
            print(f"Przykładowe klucze UUID: {list(kierunek_uuid_map.keys())[:3]}")

        # Mapowanie z obsługą case-insensitive i dodanie brakujących pól do grup
        for g in wszystkie_grupy:
            # Jeśli grupa nie ma kierunek_nazwa/wydzial, spróbuj znaleźć na podstawie kierunku
            if not g.get("kierunek_nazwa") or not g.get("wydzial"):
                # Znajdź kierunek po ID lub nazwie
                kierunek = None
                if args.from_db and g.get("kierunek_id"):
                    kierunek = next((k for k in kierunki if k.get("id") == g.get("kierunek_id")), None)
                else:
                    # Spróbuj znaleźć po nazwie kierunku z listy
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
            if g["kierunek_id"] is None:
                print(f"⚠️ Nie znaleziono kierunek_id dla: {key}")

            # Usuń tymczasowe pola
            g.pop("kierunek_nazwa", None)
            g.pop("wydzial", None)

        # Sprawdź ile grup ma kierunek_id
        z_id = [g for g in wszystkie_grupy if g.get("kierunek_id")]
        bez_id = [g for g in wszystkie_grupy if not g.get("kierunek_id")]

        print(f"✅ {len(z_id)} grup ma kierunek_id")
        if bez_id:
            print(f"⚠️ {len(bez_id)} grup nie ma przypisanego kierunek_id")

        saved = save_grupy(wszystkie_grupy)
        print(f"Zapisano {saved} grup do bazy")

        # Sprawdź stan bazy
        result = supabase.table('grupy').select('count').execute()
        count = result.data[0]['count'] if result.data else 0
        print(f"W bazie znajduje się {count} grup")


def test_nauczyciele(args):
    print("Test ETAP 3: Pobieranie nauczycieli z planów grup...")

    if args.from_db:
        print("Pobieranie grup z bazy danych...")
        if args.limit:
            result = supabase.table('grupy').select('*').limit(args.limit).execute()
        else:
            result = supabase.table('grupy').select('*').execute()
        grupy = result.data
        print(f"Pobrano {len(grupy)} grup z bazy")
    else:
        print("Pobieranie grup z UZ...")
        kierunki = scrape_kierunki()
        grupy = scrape_grupy_for_kierunki(kierunki)[:args.limit or 10]
        print(f"Pobrano {len(grupy)} grup z UZ (z limitem {args.limit or 10})")

    nauczyciele_dict = {}

    def process_grupa(grupa):
        print(f"Przetwarzanie grupy: {grupa.get('kod_grupy') or grupa.get('grupa_id')}")
        html = fetch_page(grupa.get("link_strony_grupy"))
        nauczyciele = parse_nauczyciele_from_group_page(html, grupa_id=grupa.get("grupa_id"))
        print(f" Znaleziono {len(nauczyciele)} nauczycieli")
        for n in nauczyciele:
            link = n.get("link")
            if link and link not in nauczyciele_dict:
                print(f" Pobieranie szczegółów nauczyciela: {n.get('nazwa')}")
                html_n = fetch_page(link)
                details = parse_nauczyciel_details(html_n, n.get("nauczyciel_id")) if html_n else {}
                nauczyciele_dict[link] = {
                    "nazwa": n.get("nazwa"),
                    "instytut": details.get("instytut"),
                    "email": details.get("email"),
                    "link_strony_nauczyciela": link,
                    "link_ics_nauczyciela": details.get("link_ics_nauczyciela"),
                    "nauczyciel_id": n.get("nauczyciel_id"),
                }

    # Ustaw liczbę workerów na 32 dla szybszego przetwarzania
    with ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(process_grupa, grupy))

    nauczyciele_final = list(nauczyciele_dict.values())
    print(f"Znaleziono {len(nauczyciele_final)} unikalnych nauczycieli")
    if args.verbose:
        for i, n in enumerate(nauczyciele_final[:5]):
            print(f"Przykład {i + 1}: {n}")
    if args.save:
        saved = save_nauczyciele(nauczyciele_final)
        print(f"Zapisano {saved} nauczycieli do bazy")
        result = supabase.table('nauczyciele').select('count').execute()
        count = result.data[0]['count'] if result.data else 0
        print(f"W bazie znajduje się {count} nauczycieli")


def test_zajecia_grupy(args):
    print("Test ETAP 4: Pobieranie i zapisywanie zajęć grup...")
    if args.from_db:
        print("Pobieranie grup z bazy danych...")
        result = supabase.table('grupy').select('*').limit(args.limit or 10).execute()
        grupy = result.data
        print(f"Pobrano {len(grupy)} grup z bazy")
        wszystkie_id_grup = [g.get("grupa_id") for g in grupy if g.get("grupa_id")]
        grupa_map = {g.get("grupa_id"): g for g in grupy if g.get("grupa_id")}
    else:
        print("Pobieranie grup z UZ...")
        kierunki = scrape_kierunki()
        grupy = scrape_grupy_for_kierunki(kierunki)[:args.limit or 10]
        print(f"Pobrano {len(grupy)} grup z UZ (z limitem {args.limit or 10})")
        wszystkie_id_grup = [g.get("grupa_id") for g in grupy if g.get("grupa_id")]
        grupa_map = {g.get("grupa_id"): g for g in grupy if g.get("grupa_id")}

    print(f"Pobieranie ICS dla {len(wszystkie_id_grup)} grup...")
    wyniki = download_ics_for_groups_async(wszystkie_id_grup)
    wszystkie_zajecia_grupy = []
    for w in wyniki:
        if w["status"] == "success":
            grupa_id = w["grupa_id"]
            grupa = grupa_map.get(grupa_id, {})
            zajecia = parse_ics_file(
                w["ics_content"],
                link_ics_zrodlowy=w["link_ics_zrodlowy"],
            )
            for z in zajecia:
                z["grupa_id"] = grupa_id
            wszystkie_zajecia_grupy.extend(zajecia)
            print(f"Pobrano {len(zajecia)} zajęć dla grupy {grupa_id}")
        else:
            print(f"❌ Błąd pobierania ICS: {w['link_ics_zrodlowy']}")

    print(f"Łącznie pobrano {len(wszystkie_zajecia_grupy)} zajęć")
    if args.verbose:
        for i, z in enumerate(wszystkie_zajecia_grupy[:3]):
            print(f"Przykład {i + 1}: {z}")
    if args.save:
        grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")
        print(f"Pobrano {len(grupa_uuid_map)} mapowań UUID grup")
        saved = save_zajecia_grupy(wszystkie_zajecia_grupy, grupa_uuid_map)
        print(f"Zapisano {saved} zajęć grup do bazy")
        result = supabase.table('zajecia_grupy').select('count').execute()
        count = result.data[0]['count'] if result.data else 0
        print(f"W bazie znajduje się {count} zajęć grup")


def test_zajecia_nauczycieli(args):
    print("Test ETAP 5: Pobieranie i zapisywanie zajęć nauczycieli...")
    if args.from_db:
        print("Pobieranie nauczycieli z bazy danych...")
        result = supabase.table('nauczyciele').select('*').limit(args.limit or 10).execute()
        nauczyciele = result.data
        print(f"Pobrano {len(nauczyciele)} nauczycieli z bazy")
    else:
        print("Pobieranie nauczycieli z UZ...")
        kierunki = scrape_kierunki()
        grupy = scrape_grupy_for_kierunki(kierunki)[:args.limit or 20]
        nauczyciele_dict = {}
        for grupa in grupy:
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
                        "nauczyciel_id": n.get("nauczyciel_id"),
                    }
        nauczyciele = list(nauczyciele_dict.values())[:args.limit or 10]
        print(f"Pobrano {len(nauczyciele)} nauczycieli z UZ (z limitem {args.limit or 10})")

    nauczyciel_uuid_map = get_uuid_map("nauczyciele", "link_strony_nauczyciela", "id")
    print(f"Pobrano {len(nauczyciel_uuid_map)} mapowań UUID nauczycieli")
    wszystkie_zajecia_nauczyciela = []

    for n in nauczyciele:
        link = n.get("link_ics_nauczyciela")
        if args.from_db:
            nauczyciel_uuid = n.get("id")
        else:
            nauczyciel_uuid = nauczyciel_uuid_map.get(n.get("link_strony_nauczyciela"))

        # Wyciągnij prawdziwy ID nauczyciela z UZ z linku ICS
        nauczyciel_uz_id = None
        if link:
            match = re.search(r'ID=(\d+)', link)
            if match:
                nauczyciel_uz_id = match.group(1)

        print(f"Przetwarzanie nauczyciela: {n.get('nazwa')}")
        print(f" Link ICS: {link}")
        print(f" ID UZ: {nauczyciel_uz_id}, UUID: {nauczyciel_uuid}")

        if not link or not nauczyciel_uuid or not nauczyciel_uz_id:
            print(" Pominięto - brak linku ICS, UUID lub ID UZ")
            continue

        plan = pobierz_plan_ics_nauczyciela(nauczyciel_uz_id)
        if plan["status"] == "success" and plan["ics_content"]:
            zajecia = parse_ics_file(plan["ics_content"], link_ics_zrodlowy=plan["link_ics_zrodlowy"])
            for z in zajecia:
                z["nauczyciel_id"] = nauczyciel_uuid  # Użyj UUID do zapisu
            wszystkie_zajecia_nauczyciela.extend(zajecia)
            print(f" Pobrano {len(zajecia)} zajęć")
        else:
            print(f" ❌ Błąd pobierania ICS: {plan.get('error', 'Nieznany błąd')}")

    print(f"Łącznie pobrano {len(wszystkie_zajecia_nauczyciela)} zajęć nauczycieli")
    if args.verbose:
        for i, z in enumerate(wszystkie_zajecia_nauczyciela[:3]):
            print(f"Przykład {i + 1}: {z}")
    if args.save:
        saved = save_zajecia_nauczyciela(wszystkie_zajecia_nauczyciela, nauczyciel_uuid_map)
        print(f"Zapisano {saved} zajęć nauczycieli do bazy")
        result = supabase.table('zajecia_nauczyciela').select('count').execute()
        count = result.data[0]['count'] if result.data else 0
        print(f"W bazie znajduje się {count} zajęć nauczycieli")


def main():
    parser = argparse.ArgumentParser(description='Tester poszczególnych etapów scrapera')
    parser.add_argument('etap', choices=['kierunki', 'grupy', 'nauczyciele', 'zajecia_grupy', 'zajecia_nauczycieli'],
                        help='Etap do przetestowania')
    parser.add_argument('--save', action='store_true', help='Zapisz dane do bazy')
    parser.add_argument('--from-db', action='store_true', help='Pobierz dane z bazy zamiast scrapować')
    parser.add_argument('--verbose', action='store_true', help='Wyświetl więcej informacji')
    parser.add_argument('--limit', type=int, help='Limit liczby rekordów')
    args = parser.parse_args()

    if args.etap == 'kierunki':
        test_kierunki(args)
    elif args.etap == 'grupy':
        test_grupy(args)
    elif args.etap == 'nauczyciele':
        test_nauczyciele(args)
    elif args.etap == 'zajecia_grupy':
        test_zajecia_grupy(args)
    elif args.etap == 'zajecia_nauczycieli':
        test_zajecia_nauczycieli(args)


if __name__ == "__main__":
    main()

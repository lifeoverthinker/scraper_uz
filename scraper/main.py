import os
import re

from scraper.scrapers.kierunki_scraper import scrape_kierunki
from scraper.scrapers.grupy_scraper import scrape_grupy_for_kierunki
from scraper.parsers.nauczyciel_parser import (
    fetch_page,
    parse_nauczyciel_details,
    parse_nauczyciele_from_group_page,
)
from scraper.db import (
    get_uuid_map,
    save_grupy,
    save_kierunki,
    save_nauczyciele,
    save_zajecia_grupy,
    save_zajecia_nauczyciela,
    supabase,
)
from scraper.downloader import download_ics_for_groups_async
from scraper.ics_updater import parse_ics_file, pobierz_plan_ics_nauczyciela
from scraper.xml_source import (
    fetch_grupy_plan_events_xml,
    fetch_nauczyciel_plan_events_xml,
    scrape_grupy_for_kierunki_xml,
    scrape_kierunki_xml,
)


XML_FIRST = os.getenv("SCRAPER_XML_FIRST", "1") == "1"
EVENTS_XML_FIRST = os.getenv("SCRAPER_EVENTS_XML_FIRST", "1") == "1"


def _norm(value):
    return (value or "").strip().casefold()


def _extract_external_id(link: str) -> str | None:
    if not link:
        return None
    match = re.search(r"ID=(\d+)", link)
    return match.group(1) if match else None


def _load_kierunki():
    if XML_FIRST:
        kierunki_xml = scrape_kierunki_xml()
        complete = [k for k in kierunki_xml if k.get("nazwa") and k.get("wydzial")]
        if complete:
            print(f"Kierunki z XML: {len(kierunki_xml)} (kompletnych: {len(complete)})")
            return kierunki_xml
        print("XML kierunkow pusty lub bez wydzialow - fallback do HTML")

    kierunki_html = scrape_kierunki()
    print(f"Kierunki z HTML: {len(kierunki_html)}")
    return kierunki_html


def _load_grupy(kierunki):
    if XML_FIRST:
        grupy_xml = scrape_grupy_for_kierunki_xml(kierunki)
        if grupy_xml:
            print(f"Grupy z XML: {len(grupy_xml)}")
            return grupy_xml
        print("XML grup pusty - fallback do HTML")

    grupy_html = scrape_grupy_for_kierunki(kierunki)
    print(f"Grupy z HTML: {len(grupy_html)}")
    return grupy_html


def _map_groups_to_kierunki_uuid(grupy):
    kierunek_uuid_map = get_uuid_map("kierunki", "nazwa", "id")
    missing = 0

    for g in grupy:
        if not g.get("kierunek_id"):
            key = (_norm(g.get("kierunek_nazwa")), _norm(g.get("wydzial")))
            if key[0] and key[1]:
                g["kierunek_id"] = kierunek_uuid_map.get(key)

        if not g.get("kierunek_id"):
            missing += 1

        g.pop("kierunek_nazwa", None)
        g.pop("wydzial", None)

    return missing


def _get_all_grupa_ids_from_db():
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

    return grupa_ids


def _collect_group_events(grupa_ids):
    all_events = []
    grupa_ids_for_ics = []

    if EVENTS_XML_FIRST:
        for grupa_id in grupa_ids:
            xml_events = fetch_grupy_plan_events_xml(str(grupa_id))
            if xml_events:
                all_events.extend(xml_events)
            else:
                grupa_ids_for_ics.append(grupa_id)
    else:
        grupa_ids_for_ics = list(grupa_ids)

    if grupa_ids_for_ics:
        wyniki = download_ics_for_groups_async(grupa_ids_for_ics)
        for w in wyniki:
            if w["status"] != "success":
                print(f"Blad pobierania ICS: {w['link_ics_zrodlowy']}")
                continue

            events = parse_ics_file(w["ics_content"], link_ics_zrodlowy=w["link_ics_zrodlowy"])
            for event in events:
                event["grupa_id"] = w["grupa_id"]
            all_events.extend(events)

    return all_events


def _fetch_teacher_events(external_id: str):
    if EVENTS_XML_FIRST:
        xml_events = fetch_nauczyciel_plan_events_xml(external_id)
        if xml_events:
            return xml_events

    plan = pobierz_plan_ics_nauczyciela(external_id)
    if plan.get("status") != "success" or not plan.get("ics_content"):
        return []

    return parse_ics_file(plan["ics_content"], plan["link_ics_zrodlowy"])


def _update_only_kierunki():
    print("TRYB: tylko kierunki")
    kierunki = _load_kierunki()
    saved = save_kierunki(kierunki)
    print(f"Zapisano/upewniono sie o {saved} kierunkach. Koniec.")


def _update_only_groups():
    print("TRYB: tylko grupy (kierunki + grupy)")
    kierunki = _load_kierunki()
    save_kierunki(kierunki)

    grupy = _load_grupy(kierunki)
    missing = _map_groups_to_kierunki_uuid(grupy)
    if missing:
        print(f"UWAGA: {missing} grup bez mapowania kierunek_id")

    saved = save_grupy(grupy)
    print(f"Kierunkow: {len(kierunki)}, grup pobranych: {len(grupy)}, zapisanych: {saved}")


def _update_only_groups_events():
    print("TRYB: tylko zajecia grup")

    grupa_ids = _get_all_grupa_ids_from_db()
    print(f"Znaleziono {len(grupa_ids)} ID grup do odswiezenia")

    if not grupa_ids:
        print("Brak grup w bazie - uruchom pelny scrap albo tryb 'grupy'.")
        return

    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")
    wszystkie_zajecia_grupy = _collect_group_events(grupa_ids)

    saved = save_zajecia_grupy(wszystkie_zajecia_grupy, grupa_uuid_map)
    print(f"Zapisano (upsert) zajecia grup: {saved}")


def _update_only_teachers():
    print("TRYB: tylko nauczyciele (pelna paginacja + zajecia)")
    page_size = 500
    page = 0
    all_events = []
    total_teachers = 0
    total_events = 0

    while True:
        start = page * page_size
        end = start + page_size - 1
        res = supabase.table("nauczyciele").select("id, link_strony_nauczyciela, external_id").range(start, end).execute()
        data = res.data or []
        if not data:
            break

        print(f"Strona {page + 1}: {len(data)} nauczycieli")
        for row in data:
            ext_id = row.get("external_id") or _extract_external_id(row.get("link_strony_nauczyciela"))
            if not ext_id:
                continue

            events = _fetch_teacher_events(ext_id)
            if not events:
                continue

            for event in events:
                event["nauczyciel_id"] = row["id"]

            all_events.extend(events)
            total_events += len(events)
            total_teachers += 1

        page += 1

    if all_events:
        saved = save_zajecia_nauczyciela(all_events)
        print(f"Zapisano (przekazanych do upsert): {saved}")
    else:
        print("Brak wydarzen do zapisu.")

    print(f"Nauczycieli z wydarzeniami: {total_teachers}")
    print(f"Lacznie wydarzen parsowanych: {total_events}")


def main():
    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()
    if mode in {"kierunki", "kierunki_only", "directions"}:
        _update_only_kierunki()
        return
    if mode in {"grupy", "groups"}:
        _update_only_groups()
        return
    if mode in {"grupy_zajecia", "groups_events"}:
        _update_only_groups_events()
        return
    if mode in {"teachers", "teachers_events", "nauczyciele"}:
        _update_only_teachers()
        return

    print("ETAP 1: Pobieranie kierunkow studiow...")
    kierunki = _load_kierunki()
    save_kierunki(kierunki)
    print(f"Przetworzono {len(kierunki)} kierunkow")

    print("ETAP 2: Pobieranie grup dla kierunkow...")
    wszystkie_grupy = _load_grupy(kierunki)
    missing = _map_groups_to_kierunki_uuid(wszystkie_grupy)
    if missing:
        print(f"UWAGA: {missing} grup bez mapowania kierunek_id")

    saved_groups = save_grupy(wszystkie_grupy)
    print(f"Przetworzono {len(wszystkie_grupy)} grup, zapisano {saved_groups}")

    grupa_uuid_map = get_uuid_map("grupy", "grupa_id", "id")

    print("ETAP 3: Pobieranie nauczycieli z planow grup...")
    nauczyciele_dict = {}
    for grupa in wszystkie_grupy:
        html = fetch_page(grupa.get("link_strony_grupy"))
        nauczyciele = parse_nauczyciele_from_group_page(html, grupa_id=grupa.get("grupa_id"))
        for n in nauczyciele:
            link = n.get("link")
            if not link or link in nauczyciele_dict:
                continue

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

    nauczyciele_final = list(nauczyciele_dict.values())
    save_nauczyciele(nauczyciele_final)
    print(f"Przetworzono {len(nauczyciele_final)} nauczycieli")

    nauczyciel_uuid_map = get_uuid_map("nauczyciele", "link_strony_nauczyciela", "id")

    print("ETAP 4: Pobieranie i zapisywanie zajec grup...")
    wszystkie_id_grup = [g["grupa_id"] for g in wszystkie_grupy if g.get("grupa_id")]
    wszystkie_zajecia_grupy = _collect_group_events(wszystkie_id_grup)
    saved_group_events = save_zajecia_grupy(wszystkie_zajecia_grupy, grupa_uuid_map)
    print(f"Zapisano {saved_group_events} zajec grup")

    print("ETAP 5: Pobieranie i zapisywanie zajec nauczycieli...")
    wszystkie_zajecia_nauczyciela = []
    missing_uuid = []
    created_on_demand = 0
    allow_ondemand = os.getenv("TEACHER_UUID_ONDEMAND", "1") == "1"

    for n in nauczyciele_final:
        external_id = n.get("nauczyciel_id")
        nazwa = n.get("nazwa")
        link_strony = n.get("link_strony_nauczyciela")
        if not external_id:
            print(f"Brak external_id dla nauczyciela {nazwa} ({link_strony})")
            continue

        uuid_key = _norm(link_strony)
        uuid_row = nauczyciel_uuid_map.get(uuid_key)

        if not uuid_row and allow_ondemand:
            try:
                supabase.table("nauczyciele").upsert(
                    [
                        {
                            "nazwa": nazwa,
                            "link_strony_nauczyciela": link_strony,
                            "instytut": n.get("instytut"),
                            "email": n.get("email"),
                            "link_ics_nauczyciela": n.get("link_ics_nauczyciela"),
                        }
                    ],
                    on_conflict="link_strony_nauczyciela",
                ).execute()
                res = (
                    supabase.table("nauczyciele")
                    .select("id,link_strony_nauczyciela")
                    .eq("link_strony_nauczyciela", link_strony)
                    .limit(1)
                    .execute()
                )
                if res.data:
                    uuid_row = res.data[0]["id"]
                    nauczyciel_uuid_map[uuid_key] = uuid_row
                    created_on_demand += 1
            except Exception as e:
                print(f"Nie udalo sie dodac nauczyciela on-demand: {nazwa} -> {e}")

        if not uuid_row:
            missing_uuid.append({"external_id": external_id, "nazwa": nazwa, "link": link_strony})
            print(f"Brak UUID dla nauczyciela {nazwa} (ext_id={external_id})")
            continue

        zajecia = _fetch_teacher_events(external_id)
        if not zajecia:
            print(f"Nie udalo sie pobrac planu dla nauczyciela {nazwa} (ext_id={external_id})")
            continue

        for z in zajecia:
            z["nauczyciel_id"] = uuid_row

        wszystkie_zajecia_nauczyciela.extend(zajecia)
        print(f"Pobrano {len(zajecia)} zajec dla nauczyciela {nazwa} (ext_id={external_id})")

    saved_teacher_events = save_zajecia_nauczyciela(wszystkie_zajecia_nauczyciela, nauczyciel_uuid_map)
    print(f"Zapisano {saved_teacher_events} zajec nauczycieli (po deduplikacji)")

    if missing_uuid:
        print(f"Podsumowanie: {len(missing_uuid)} nauczycieli bez UUID (pokazuje do 10)")
        for item in missing_uuid[:10]:
            print(f" - {item['nazwa']} (ext_id={item['external_id']}) {item['link']}")

    if created_on_demand:
        print(f"Utworzono on-demand {created_on_demand} brakujacych rekordow nauczycieli")

    print("Zakonczono proces MVP.")


if __name__ == "__main__":
    main()


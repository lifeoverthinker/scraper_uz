from __future__ import annotations

import os
import re
import datetime as dt

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
    save_semester_state,
    get_semester_state,
    deactivate_expired_records,
)
from scraper.downloader import download_ics_for_groups_async
from scraper.ics_updater import pobierz_plan_ics_nauczyciela, parse_ics_file

from scraper.xml_client import XmlClient
from scraper.semester_manager import (
    parse_semester_state_from_meta,
    detect_semester_switch,
    SemesterState,
)
from scraper.xml_sync import sync_directions_and_groups_from_xml
from scraper.teacher_sync import sync_teacher_events_and_meta


def _extract_external_id(link: str) -> str | None:
    if not link:
        return None
    m = re.search(r"ID=(\d+)", link)
    return m.group(1) if m else None


def _run_xml_bootstrap():
    print("TRYB: xml_bootstrap (semestr + zapis stanu)")

    client = XmlClient(
        base_url=os.getenv("XML_BASE_URL", "https://plan.uz.zgora.pl/static_files/"),
        timeout=int(os.getenv("XML_TIMEOUT", "20")),
        max_retries=int(os.getenv("XML_MAX_RETRIES", "3")),
    )

    meta = client.fetch_semester_meta_from_file("grupy_lista_kierunkow.xml")
    current_state = parse_semester_state_from_meta({
        "current_semester_id": meta.current_semester_id,
        "current_semester_name_pl": meta.current_semester_name_pl,
        "current_semester_name_en": meta.current_semester_name_en,
        "previous_semester_id": meta.previous_semester_id,
        "previous_semester_name_pl": meta.previous_semester_name_pl,
        "previous_semester_name_en": meta.previous_semester_name_en,
        "generated_at": meta.generated_at,
    })

    prev_row = get_semester_state()
    prev_state = None
    if prev_row:
        prev_state = SemesterState(
            current_semester_id=prev_row.get("current_semester_id"),
            current_semester_name=prev_row.get("current_semester_name"),
            previous_semester_id=prev_row.get("previous_semester_id"),
            previous_semester_name=prev_row.get("previous_semester_name"),
            generated_at=prev_row.get("generated_at_source"),
        )

    switch = detect_semester_switch(prev_state, current_state)

    print("----- XML SEMESTER -----")
    print(f"current_id: {current_state.current_semester_id}")
    print(f"current_name: {current_state.current_semester_name}")
    print(f"previous_id: {current_state.previous_semester_id}")
    print(f"previous_name: {current_state.previous_semester_name}")
    print(f"generated_at: {current_state.generated_at}")
    print("----- SWITCH CHECK -----")
    print(f"switched: {switch.switched}")
    print(f"reason: {switch.reason}")
    print(f"old_semester_id: {switch.old_semester_id}")
    print(f"new_semester_id: {switch.new_semester_id}")

    if not current_state.current_semester_id:
        print("⚠️ Brak current_semester_id w XML — pomijam zapis semester_state.")
        return

    saved = save_semester_state(
        current_semester_id=current_state.current_semester_id,
        current_semester_name=current_state.current_semester_name,
        previous_semester_id=current_state.previous_semester_id,
        previous_semester_name=current_state.previous_semester_name,
        source_url=meta.source_url,
        generated_at_source=current_state.generated_at,
    )
    if saved > 0:
        print("✅ Zapisano semester_state.")
    else:
        print("⚠️ Nie zapisano semester_state (upsert zwrócił 0).")


def _run_xml_sync():
    print("TRYB: xml_sync (kierunki + grupy)")
    result = sync_directions_and_groups_from_xml(verbose=True)
    print(f"✅ XML sync result: {result}")


def _run_teacher_sync():
    print("TRYB: teacher_sync")
    result = sync_teacher_events_and_meta(verbose=True)
    print(f"✅ Teacher sync result: {result}")


def _run_cleanup_only():
    print("TRYB: cleanup_only")
    result = deactivate_expired_records(today=dt.date.today())
    print(f"✅ Cleanup result: {result}")


# -------- legacy modes --------

def _update_only_kierunki():
    print("TRYB: tylko kierunki")
    kierunki = scrape_kierunki()
    saved = save_kierunki(kierunki)
    print(f"Zapisano/upewniono się o {saved} kierunkach. Koniec.")


def _update_only_groups():
    print("TRYB: tylko grupy (kierunki + grupy)")
    kierunki = scrape_kierunki()
    save_kierunki(kierunki)
    grupy = scrape_grupy_for_kierunki(kierunki)
    save_grupy(grupy)
    print(f"Kierunków: {len(kierunki)}, grup: {len(grupy)} – koniec.")


def _update_only_groups_events():
    print("TRYB: tylko zajęcia grup (ICS)")
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
                grupa_ids.append(str(gid))
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
                z["grupa_id"] = str(w["grupa_id"])
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

        print(f"Strona {page + 1}: {len(data)} nauczycieli")
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


def main():
    mode = os.getenv("SCRAPER_ONLY", "").lower().strip()

    if mode in {"xml_bootstrap", "xml_semester"}:
        _run_xml_bootstrap()
        return

    if mode in {"xml_sync", "xml_groups"}:
        _run_xml_sync()
        return

    if mode in {"teacher_sync", "teachers_sync"}:
        _run_teacher_sync()
        return

    if mode in {"cleanup_only", "cleanup"}:
        _run_cleanup_only()
        return

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

    print("ETAP 1: Pobieranie kierunków studiów...")
    kierunki = scrape_kierunki()
    save_kierunki(kierunki)
    print(f"Przetworzono {len(kierunki)} kierunków\n")

    print("ETAP 2: Pobieranie grup dla kierunków...")
    wszystkie_grupy = scrape_grupy_for_kierunki(kierunki)

    kierunek_uuid_map = get_uuid_map("kierunki", "nazwa", "id")
    for g in wszystkie_grupy:
        key = (
            (g.get("kierunek_nazwa") or "").strip().casefold(),
            (g.get("wydzial") or "").strip().casefold(),
        )
        g["kierunek_id"] = kierunek_uuid_map.get(key)
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
                    "nauczyciel_id": n.get("nauczyciel_id"),
                }

    nauczyciele_final = list(nauczyciele_dict.values())
    save_nauczyciele(nauczyciele_final)
    print(f"Przetworzono {len(nauczyciele_final)} nauczycieli\n")

    nauczyciel_uuid_map = get_uuid_map("nauczyciele", "link_strony_nauczyciela", "id")

    print("ETAP 4: Pobieranie i zapisywanie zajęć grup...")
    wszystkie_id_grup = [str(g["grupa_id"]) for g in wszystkie_grupy if g.get("grupa_id")]
    wyniki = download_ics_for_groups_async(wszystkie_id_grup)
    wszystkie_zajecia_grupy = []

    for w in wyniki:
        if w["status"] == "success":
            zajecia = parse_ics_file(w["ics_content"], link_ics_zrodlowy=w["link_ics_zrodlowy"])
            for z in zajecia:
                z["grupa_id"] = str(w["grupa_id"])
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
        external_id = n.get("nauczyciel_id")
        nazwa = n.get("nazwa")
        link_strony = n.get("link_strony_nauczyciela")
        if not external_id:
            continue

        uuid_key = (link_strony or "").strip().casefold()
        uuid_row = nauczyciel_uuid_map.get(uuid_key)

        if not uuid_row and allow_ondemand:
            try:
                supabase.table("nauczyciele").upsert(
                    [{
                        "nazwa": nazwa,
                        "link_strony_nauczyciela": link_strony,
                        "instytut": n.get("instytut"),
                        "email": n.get("email"),
                        "link_ics_nauczyciela": n.get("link_ics_nauczyciela"),
                    }],
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
                print(f"❌ On-demand add teacher failed: {nazwa} -> {e}")

        if not uuid_row:
            missing_uuid.append({"external_id": external_id, "nazwa": nazwa, "link": link_strony})
            continue

        plan = pobierz_plan_ics_nauczyciela(external_id)
        if plan["status"] == "success" and plan["ics_content"]:
            zajecia = parse_ics_file(plan["ics_content"], link_ics_zrodlowy=plan["link_ics_zrodlowy"])
            for z in zajecia:
                z["nauczyciel_id"] = uuid_row
            wszystkie_zajecia_nauczyciela.extend(zajecia)

    saved_teacher_events = save_zajecia_nauczyciela(wszystkie_zajecia_nauczyciela, nauczyciel_uuid_map)
    print(f"Zapisano {saved_teacher_events} zajęć nauczycieli (po deduplikacji)")

    if missing_uuid:
        print(f"⚠️ Nauczyciele bez UUID: {len(missing_uuid)}")
        for item in missing_uuid[:10]:
            print(f" - {item['nazwa']} (ext_id={item['external_id']}) {item['link']}")
    if created_on_demand:
        print(f"ℹ️ Utworzono on-demand {created_on_demand} rekordów nauczycieli")

    print("Zakończono proces MVP.")


if __name__ == "__main__":
    main()
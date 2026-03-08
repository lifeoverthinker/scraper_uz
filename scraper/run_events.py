import xml.etree.ElementTree as ET
from scraper.db import supabase, save_zajecia_grupy
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_group_plan_events


def main():
    client = XmlClient()
    # Pobieramy listę wszystkich grup z katalogu
    res = supabase.table("grupy").select("grupa_id").execute()
    grupy = res.data or []

    print(f"🔄 Rozpoczynam synchronizację planów dla {len(grupy)} grup...")

    for row in grupy:
        gid = row['grupa_id']
        all_events_for_group = []  # Tutaj zbieramy WSZYSTKIE zajęcia (przyszłe i przeszłe)

        # Iterujemy przez plan bieżący oraz historyczny dla grupy
        for file_prefix in ["grupy_plan", "grupy_hplan"]:
            xml_res = client.fetch_xml(f"{file_prefix}.ID={gid}.xml")
            if not xml_res.content:
                continue

            try:
                root = ET.fromstring(xml_res.content)

                # 1. Szybka aktualizacja metadanych grupy (tryb i semestr) z głównego planu
                if file_prefix == "grupy_plan":
                    supabase.table("grupy").update({
                        "tryb": root.findtext("STUDIA_SYST"),
                        "semestr": root.findtext("SEMESTER")
                    }).eq("grupa_id", gid).execute()

                # 2. Parsowanie zdarzeń z XML i dodawanie do wspólnej listy
                events = parse_group_plan_events(xml_res.content)
                all_events_for_group.extend(events)

            except Exception as e:
                print(f"Błąd przetwarzania {file_prefix} dla grupy {gid}: {e}")

        # 3. Zapis do bazy - TYLKO RAZ DLA GRUPY (połączony plan + hplan)
        if all_events_for_group:
            try:
                saved = save_zajecia_grupy(all_events_for_group, gid)
                if saved > 0:
                    print(f"  [SUKCES] Zapisano łącznie {saved} zajęć (plan + hplan) dla grupy {gid}")
            except Exception as e:
                print(f"  [BŁĄD ZAPISU] Nie udało się zapisać zajęć dla grupy {gid}: {e}")


if __name__ == "__main__":
    main()
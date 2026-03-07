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
        # Pobieranie XML z planem grupy
        xml_res = client.fetch_xml(f"grupy_plan.ID={gid}.xml")
        if not xml_res.content:
            continue

        try:
            root = ET.fromstring(xml_res.content)

            # 1. Szybka aktualizacja metadanych grupy (tryb i semestr)
            supabase.table("grupy").update({
                "tryb": root.findtext("STUDIA_SYST"),
                "semestr": root.findtext("SEMESTER")
            }).eq("grupa_id", gid).execute()

            # 2. Parsowanie zdarzeń z XML
            events = parse_group_plan_events(xml_res.content)

            # 3. Zapis do bazy (przekazujemy gid bezpośrednio do nowej funkcji)
            saved = save_zajecia_grupy(events, gid)

            if saved > 0:
                print(f"  [Grupa {gid}]: Zsynchronizowano {saved} zajęć.")

        except Exception as e:
            print(f"  [Grupa {gid}]: Błąd synchronizacji: {e}")


if __name__ == "__main__":
    main()
import xml.etree.ElementTree as ET
from scraper.db import supabase, save_zajecia_grupy
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_group_plan_events

GROUP_PLAN_SOURCES = ["grupy_plan", "grupy_hplan"]


def main():
    """Synchronizuje zajecia dla wszystkich grup z planu biezacego i historycznego."""
    client = XmlClient()
    res = supabase.table("grupy").select("grupa_id").execute()
    grupy = res.data or []

    print(f"Rozpoczynam synchronizacje planow dla {len(grupy)} grup...")

    for row in grupy:
        gid = row["grupa_id"]
        all_events_for_group = []

        for source_prefix in GROUP_PLAN_SOURCES:
            xml_res = client.fetch_xml(f"{source_prefix}.ID={gid}.xml")
            if not xml_res.content:
                continue

            try:
                root = ET.fromstring(xml_res.content)

                # Aktualizacja metadanych grupy z glownego planu.
                if source_prefix == "grupy_plan":
                    tryb_val = root.findtext(".//STUDIA_SYST")
                    sem_val = root.findtext(".//SEMESTER")

                    update_data = {}
                    if tryb_val and tryb_val.strip():
                        update_data["tryb"] = tryb_val.strip()
                    if sem_val and sem_val.strip():
                        update_data["semestr"] = sem_val.strip()

                    if update_data:
                        supabase.table("grupy").update(update_data).eq("grupa_id", gid).execute()

                events = parse_group_plan_events(xml_res.content)
                all_events_for_group.extend(events)

            except Exception as e:
                print(f"Blad przetwarzania {source_prefix} dla grupy {gid}: {e}")

        if all_events_for_group:
            try:
                saved = save_zajecia_grupy(all_events_for_group, gid)
                if saved > 0:
                    print(f"[SUKCES] Zapisano lacznie {saved} zajec (plan + hplan) dla grupy {gid}")
            except Exception as e:
                print(f"[BLAD ZAPISU] Nie udalo sie zapisac zajec dla grupy {gid}: {e}")


if __name__ == "__main__":
    main()
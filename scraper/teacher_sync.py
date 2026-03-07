import xml.etree.ElementTree as ET
from scraper.db import supabase, save_zajecia_nauczyciela
from scraper.xml_parsers import parse_teacher_plan_events
from scraper.xml_client import XmlClient


def sync_teacher_events_and_meta(verbose=True):
    client = XmlClient()
    # Pobieramy nauczycieli (potrzebujemy wewnętrznego ID do relacji)
    res = supabase.table("nauczyciele").select("id, external_id").execute()
    teachers = res.data or []

    total_saved = 0

    if verbose:
        print(f"🔄 Rozpoczynam synchronizację planów dla {len(teachers)} nauczycieli...")

    for t in teachers:
        teacher_uuid = t['id']
        ext_id = t['external_id']
        if not ext_id:
            continue

        # Pobieranie planu przez XmlClient
        xml_res = client.fetch_xml(f"nauczyciel_plan.ID={ext_id}.xml")
        if not xml_res.content:
            continue

        try:
            root = ET.fromstring(xml_res.content)

            # Zbieranie jednostek organizacyjnych (JEDN1, JEDN2...)
            jednostki = set()
            for child in root:
                if child.tag.startswith("JEDN") and child.text:
                    jednostki.add(child.text.strip())
            jednostka_str = " | ".join(jednostki)

            # 1. Aktualizacja danych nauczyciela (email i jednostka)
            supabase.table("nauczyciele").update({
                "email": root.findtext("E_MAIL"),
                "jednostka": jednostka_str
            }).eq("id", teacher_uuid).execute()

            # 2. Parsowanie zajęć
            evs = parse_teacher_plan_events(xml_res.content)

            # Przygotowanie payloadu pod polskie kolumny (zgodnie z db.py)
            payload = []
            for e in evs:
                payload.append({
                    "uid": e.external_uid,
                    "id_semestru": e.id_semestru,
                    "starts_at": e.starts_at,
                    "ends_at": e.ends_at,
                    "subject": e.subject,
                    "class_type": e.class_type,
                    "room": e.room,
                    "groups_label": e.groups_label
                })

            # 3. Zapis zajęć (z mechanizmem usuwania nieobecnych w XML)
            if payload:
                saved = save_zajecia_nauczyciela(payload, teacher_uuid)
                total_saved += saved
                if verbose and saved > 0:
                    print(f"  [Nauczyciel {ext_id}]: Zapisano {saved} zajęć.")

        except Exception as e:
            print(f"  [Nauczyciel {ext_id}]: Błąd synchronizacji: {e}")

    return {"status": "ok", "events_saved": total_saved}


if __name__ == "__main__":
    sync_teacher_events_and_meta()
import xml.etree.ElementTree as ET
from scraper.db import supabase, save_zajecia_nauczyciela
from scraper.xml_parsers import parse_teacher_plan_events
from scraper.xml_client import XmlClient


def sync_teacher_events_and_meta(verbose=True):
    client = XmlClient()
    # Pobieramy nauczycieli - ważne: pobieramy też nazwę, by logować błędy
    res = supabase.table("nauczyciele").select("id, external_id, nazwisko_imie").execute()
    teachers = res.data or []

    total_saved = 0

    for t in teachers:
        teacher_uuid = t['id']
        ext_id = t['external_id']
        full_name = t['nazwisko_imie']  #

        if not ext_id:
            continue

        all_events_for_teacher = []
        jednostki = set()
        teacher_email = None

        for file_prefix in ["nauczyciel_plan", "nauczyciel_hplan"]:
            xml_res = client.fetch_xml(f"{file_prefix}.ID={ext_id}.xml")
            if not xml_res.content:
                continue

            try:
                # Używamy lxml/BeautifulSoup przez parse_teacher_plan_events
                evs = parse_teacher_plan_events(xml_res.content)

                # KLUCZOWE: Dodatkowa weryfikacja czy zajęcia należą do tego nauczyciela
                # (Zabezpieczenie przed błędami w samym XML uczelni)
                for e in evs:
                    all_events_for_teacher.append({
                        "uid": e.external_uid,
                        "id_semestru": e.id_semestru,
                        "starts_at": e.starts_at,
                        "ends_at": e.ends_at,
                        "subject": e.subject,
                        "class_type": e.class_type,
                        "room": e.room,
                        "groups_label": e.groups_label
                    })

                # Pobieranie metadanych (email/jednostka) bezpośrednio z XML
                root = ET.fromstring(xml_res.content)
                email_tag = root.findtext("E_MAIL")
                if email_tag:
                    teacher_email = email_tag
                for child in root:
                    if child.tag.startswith("JEDN") and child.text:
                        jednostki.add(child.text.strip())

            except Exception as err:
                if verbose:
                    print(f"  [Błąd {full_name}]: {err}")

        # Zapisujemy tylko dla TEGO KONKRETNEGO UUID
        if all_events_for_teacher or jednostki:
            jednostka_str = " | ".join(jednostki)
            supabase.table("nauczyciele").update({
                "email": teacher_email,
                "jednostka": jednostka_str
            }).eq("id", teacher_uuid).execute()

            if all_events_for_teacher:
                saved = save_zajecia_nauczyciela(all_events_for_teacher, teacher_uuid)
                total_saved += saved

    return {"status": "ok", "events_saved": total_saved}


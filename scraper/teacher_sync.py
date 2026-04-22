from scraper.db import supabase, save_zajecia_nauczyciela
from scraper.xml_parsers import parse_teacher_plan_events
from scraper.xml_client import XmlClient
from bs4 import BeautifulSoup  # Zamieniliśmy ET na BeautifulSoup

TEACHER_PLAN_SOURCES = ["nauczyciel_plan", "nauczyciel_hplan"]


def sync_teacher_events_and_meta(verbose=True):
    """Synchronizuje zajecia i metadane (email/jednostka) dla nauczycieli."""
    client = XmlClient()
    res = supabase.table("nauczyciele").select("id, external_id, nazwisko_imie").execute()
    teachers = res.data or []

    total_saved = 0

    if verbose:
        print(f"Rozpoczynam synchronizacje planow dla {len(teachers)} nauczycieli...")

    for teacher in teachers:
        teacher_uuid = teacher["id"]
        ext_id = teacher["external_id"]
        full_name = teacher["nazwisko_imie"]

        if not ext_id:
            continue

        all_events_for_teacher = []
        jednostki = set()
        teacher_email = None

        for source_prefix in TEACHER_PLAN_SOURCES:
            xml_res = client.fetch_xml(f"{source_prefix}.ID={ext_id}.xml")
            if not xml_res.content:
                continue

            try:
                # 1. Parsowanie zajęć
                events = parse_teacher_plan_events(xml_res.content)
                for event in events:
                    all_events_for_teacher.append({
                        "uid": event.external_uid,
                        "id_semestru": event.id_semestru,
                        "starts_at": event.starts_at,
                        "ends_at": event.ends_at,
                        "subject": event.subject,
                        "class_type": event.class_type,
                        "room": event.room,
                        "groups_label": event.groups_label,
                    })

                # 2. Parsowanie E-maila i Jednostki (Używamy BeautifulSoup!)
                soup = BeautifulSoup(xml_res.content, "xml")

                email_tag = soup.find("E_MAIL")
                if email_tag and email_tag.text:
                    teacher_email = email_tag.get_text(strip=True)

                # Zbieranie nazw jednostek
                for tag_name in ["JEDN", "JEDN_EN", "JEDN2", "JEDN2_EN"]:
                    jedn_node = soup.find(tag_name)
                    if jedn_node and jedn_node.text:
                        jednostki.add(jedn_node.get_text(strip=True))

            except Exception as err:
                if verbose:
                    print(f"[BLAD {full_name}]: {err}")

        # Zapis do bazy Supabase
        if all_events_for_teacher or jednostki:
            jednostka_str = " | ".join(jednostki) if jednostki else None

            supabase.table("nauczyciele").update({
                "email": teacher_email,
                "jednostka": jednostka_str,
            }).eq("id", teacher_uuid).execute()

            if all_events_for_teacher:
                saved = save_zajecia_nauczyciela(all_events_for_teacher, teacher_uuid)
                total_saved += saved

                if verbose and saved > 0:
                    print(f"[SUKCES] Zapisano {saved} zajec dla: {full_name}")

    return {"status": "ok", "events_saved": total_saved}
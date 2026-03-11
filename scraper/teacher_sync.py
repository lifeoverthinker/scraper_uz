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

        all_events_for_teacher = []
        jednostki = set()
        teacher_email = None

        # Pobieranie planu bieżącego oraz historycznego przez XmlClient
        for file_prefix in ["nauczyciel_plan", "nauczyciel_hplan"]:
            xml_res = client.fetch_xml(f"{file_prefix}.ID={ext_id}.xml")
            if not xml_res.content:
                continue

            try:
                root = ET.fromstring(xml_res.content)

                # Zbieranie jednostek organizacyjnych (JEDN1, JEDN2...)
                for child in root:
                    if child.tag.startswith("JEDN") and child.text:
                        jednostki.add(child.text.strip())
                
                # Zapisanie maila, jeśli występuje w danym pliku
                email_tag = root.findtext("E_MAIL")
                if email_tag:
                    teacher_email = email_tag

                # Parsowanie zajęć
                evs = parse_teacher_plan_events(xml_res.content)

                # Przygotowanie payloadu pod polskie kolumny (zgodnie z db.py)
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

            except Exception as e:
                print(f"  [Nauczyciel {ext_id}]: Błąd synchronizacji pliku {file_prefix}: {e}")

        # Jeśli zebraliśmy jakiekolwiek dane z obu plików, aktualizujemy bazę
        if all_events_for_teacher or jednostki:
            jednostka_str = " | ".join(jednostki)

            try:
                # 1. Aktualizacja danych nauczyciela (email i jednostka)
                supabase.table("nauczyciele").update({
                    "email": teacher_email,
                    "jednostka": jednostka_str
                }).eq("id", teacher_uuid).execute()

                # 2. Zapis zajęć (z mechanizmem usuwania nieobecnych w XML)
                if all_events_for_teacher:
                    saved = save_zajecia_nauczyciela(all_events_for_teacher, teacher_uuid)
                    total_saved += saved
                    if verbose and saved > 0:
                        print(f"  [Nauczyciel {ext_id}]: Zapisano {saved} zajęć (plan + hplan).")
            
            except Exception as db_err:
                print(f"  [Nauczyciel {ext_id}]: ⚠️ Błąd zapisu do bazy Supabase (przeciążenie): {db_err}")

    return {"status": "ok", "events_saved": total_saved}


if __name__ == "__main__":
    sync_teacher_events_and_meta()

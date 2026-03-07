import requests
import xml.etree.ElementTree as ET
from scraper.db import supabase, save_kierunki, save_grupy, save_nauczyciele, get_uuid_map
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_directions_from_xml, parse_groups_from_xml


def sync_directions_and_groups_from_xml(client=None, verbose=True):
    client = client or XmlClient()
    # 1. Najpierw zapisujemy kierunki
    root = client.fetch_xml("grupy_lista_kierunkow.xml")
    directions = parse_directions_from_xml(root.content)
    save_kierunki(directions)

    # 2. POBIERAMY MAPĘ UUID (external_id -> uuid z bazy)
    # To jest kluczowe, żeby nie wysyłać cyferek zamiast UUID
    kierunek_map = get_uuid_map("kierunki", "external_id", "id")

    all_groups = []
    for d in directions:
        resp = client.fetch_xml(f"grupy_lista_grup_kierunku.ID={d.external_id}.xml")
        if not resp.content: continue

        # Pobieramy UUID z mapy dla tego konkretnego kierunku
        k_uuid = kierunek_map.get(str(d.external_id).strip())
        if not k_uuid: continue  # Jeśli nie ma UUID, pomijamy, żeby nie było błędu

        groups = parse_groups_from_xml(resp.content, direction_external_id=d.external_id)
        for g in groups:
            all_groups.append({
                "grupa_id": g.external_id,
                "kod_grupy": g.code,
                "kierunek_id": k_uuid,  # WYSYŁAMY UUID, NIE NUMER!
                "link_strony_grupy": f"https://plan.uz.zgora.pl/grupy_plan.php?ID={g.external_id}",
                "tryb_studiow": g.study_mode or "nieznany"
            })
    save_grupy(all_groups)

    # 3. Nauczyciele
    r_w = requests.get("https://plan.uz.zgora.pl/static_files/nauczyciel_lista_wydzialow.xml")
    r_w.encoding = 'utf-8'
    root_w = ET.fromstring(r_w.text)
    for w in [i for i in root_w.findall(".//ITEM") if i.find("ID") is not None]:
        w_id = w.find("ID").text
        r_n = requests.get(f"https://plan.uz.zgora.pl/static_files/nauczyciel_lista_wydzialu.ID={w_id}.xml")
        r_n.encoding = 'utf-8'
        teachers_xml = ET.fromstring(r_n.text)

        payload = []
        for n in teachers_xml.findall(".//ITEM"):
            payload.append({
                "name": n.findtext("NAME"),  # db.py mapuje na 'nazwisko_imie'
                "unit_name": n.findtext("JEDN"),  # db.py mapuje na 'jednostka'
                "external_id": n.findtext("ID"),
                "email": n.findtext("E_MAIL")  # Upewnij się, że pobierasz e-mail, jeśli dostępny
            })
        save_nauczyciele(payload)
    return {"status": "ok"}
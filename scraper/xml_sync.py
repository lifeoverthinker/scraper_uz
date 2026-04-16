import requests
import xml.etree.ElementTree as ET
from scraper.db import save_kierunki, save_grupy, save_nauczyciele, get_uuid_map
from scraper.xml_client import XmlClient
from scraper.xml_parsers import parse_directions_from_xml, parse_groups_from_xml

DIRECTIONS_XML = "grupy_lista_kierunkow.xml"
GROUPS_XML_TEMPLATE = "grupy_lista_grup_kierunku.ID={direction_id}.xml"
TEACHER_FACULTIES_XML = "https://plan.uz.zgora.pl/static_files/nauczyciel_lista_wydzialow.xml"
TEACHER_FACULTY_XML_TEMPLATE = "https://plan.uz.zgora.pl/static_files/nauczyciel_lista_wydzialu.ID={faculty_id}.xml"
GROUP_PAGE_URL_TEMPLATE = "https://plan.uz.zgora.pl/grupy_plan.php?ID={group_id}"


def sync_directions_and_groups_from_xml(client=None, verbose=True):
    """Synchronizuje kierunki, grupy i nauczycieli na podstawie plikow XML UZ."""
    client = client or XmlClient()

    if verbose:
        print("Synchronizuje kierunki, grupy i nauczycieli z XML...")

    directions = _sync_directions(client)
    _sync_groups(client, directions)
    _sync_teachers()

    return {"status": "ok"}


def _sync_directions(client: XmlClient):
    directions_xml = client.fetch_xml(DIRECTIONS_XML)
    directions = parse_directions_from_xml(directions_xml.content)
    save_kierunki(directions)
    return directions


def _sync_groups(client: XmlClient, directions):
    kierunek_map = get_uuid_map("kierunki", "external_id", "id")
    all_groups = []

    for direction in directions:
        groups_xml = client.fetch_xml(GROUPS_XML_TEMPLATE.format(direction_id=direction.external_id))
        if not groups_xml.content:
            continue

        kierunek_uuid = kierunek_map.get(str(direction.external_id).strip())
        if not kierunek_uuid:
            continue

        for group in parse_groups_from_xml(groups_xml.content, direction_external_id=direction.external_id):
            all_groups.append({
                "grupa_id": group.external_id,
                "kod_grupy": group.code,
                "kierunek_id": kierunek_uuid,
                "link_strony_grupy": GROUP_PAGE_URL_TEMPLATE.format(group_id=group.external_id),
                "tryb_studiow": group.study_mode or "nieznany",
            })

    save_grupy(all_groups)


def _sync_teachers():
    wydzialy_resp = requests.get(TEACHER_FACULTIES_XML)
    wydzialy_resp.encoding = "utf-8"
    root_wydzialy = ET.fromstring(wydzialy_resp.text)

    for item in [node for node in root_wydzialy.findall(".//ITEM") if node.find("ID") is not None]:
        wydzial_id = item.find("ID").text
        if not wydzial_id:
            continue

        nauczyciele_resp = requests.get(TEACHER_FACULTY_XML_TEMPLATE.format(faculty_id=wydzial_id))
        nauczyciele_resp.encoding = "utf-8"
        teachers_xml = ET.fromstring(nauczyciele_resp.text)

        payload = []
        for teacher in teachers_xml.findall(".//ITEM"):
            payload.append({
                "name": teacher.findtext("NAME"),
                "unit_name": teacher.findtext("JEDN"),
                "external_id": teacher.findtext("ID"),
                "email": teacher.findtext("E_MAIL"),
            })

        save_nauczyciele(payload)

import datetime
from icalendar import Calendar
import requests
import time

BASE_URL = "https://plan.uz.zgora.pl/"


def fetch_ics_content(url: str, max_retries: int = 3, retry_delay: int = 5) -> str | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30, headers=headers)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2
    return None


def parse_ics_file(ics_content: str, link_ics_zrodlowy: str = None) -> list[dict]:
    """Parsuje plik ICS i zwraca listę wydarzeń (zajęć)."""
    import re  # Import na początku funkcji

    if not ics_content:
        return []
    events = []
    try:
        cal = Calendar.from_ical(ics_content)
        for component in cal.walk('VEVENT'):
            start_time = component.get('dtstart').dt
            end_time = component.get('dtend').dt
            summary = str(component.get('summary', ''))
            location = str(component.get('location', ''))
            uid = str(component.get('uid', ''))

            # Rodzaj zajęć (rz)
            rz = None
            categories = component.get('categories')
            if categories:
                if hasattr(categories, 'to_ical'):
                    rz = categories.to_ical().decode(errors="ignore").strip()
                else:
                    rz = str(categories).strip()
                if rz and len(rz) > 10:
                    rz = rz[:10]
                if rz.lower().startswith("<icalendar"):
                    rz = None
            else:
                # Wyciągnij z nawiasów w SUMMARY
                rz_match = re.search(r'\((W|C|Ć|L|P|S|E|I|T|K|X|Z|Zp)\)', summary)
                rz = rz_match.group(1) if rz_match else None

            # Przedmiot
            przedmiot = summary
            match_przedmiot = re.match(r'^([^(]+)', summary)
            if match_przedmiot:
                przedmiot = match_przedmiot.group(1).strip()

            # Wyciągnięcie nauczyciela z SUMMARY (po dwukropku)
            nauczyciel = None
            if ': ' in summary:
                parts = summary.split(': ', 1)
                nauczyciel = parts[1].strip() if len(parts) > 1 else None
            # Usuń fragment z podgrupą
            if nauczyciel and '(PG:' in nauczyciel:
                nauczyciel = nauczyciel.split('(PG:')[0].strip()

            # Wyciągnięcie podgrupy
            podgrupa = None
            podgrupa_match = re.search(r'\(PG:\s*([^)]+)\)', summary)
            if podgrupa_match:
                podgrupa = podgrupa_match.group(1).strip()

            # Wyciągnięcie grup (po dwukropku, po znaku ')')
            grupy = None
            grupy_match = re.search(r'\)\s*:\s*([^\n]+)', summary)
            if grupy_match:
                grupy = grupy_match.group(1).strip()
            elif ': ' in summary:
                parts = summary.split(': ', 1)
                if len(parts) > 1:
                    grupy = parts[1].strip()

            event = {
                'przedmiot': przedmiot,
                'od': start_time.isoformat() if hasattr(start_time, "isoformat") else start_time,
                'do_': end_time.isoformat() if hasattr(end_time, "isoformat") else end_time,
                'miejsce': location,
                'rz': rz,
                'link_ics_zrodlowy': link_ics_zrodlowy,
                'podgrupa': podgrupa,
                'uid': uid,
                'nauczyciel': nauczyciel,
                'grupy': grupy  # KLUCZOWE POLE dla zajęć nauczycieli
            }
            events.append(event)
    except Exception as e:
        print(f"Błąd podczas parsowania pliku ICS: {e}")
    return events


def pobierz_plan_ics_grupy(grupa_id: str) -> dict:
    """Pobiera plan grupy w formacie ICS."""
    ics_link = f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG"
    ics_content = fetch_ics_content(ics_link)
    return {
        'grupa_id': grupa_id,
        'ics_content': ics_content,
        'link_ics_zrodlowy': ics_link,
        'data_aktualizacji': datetime.datetime.now().isoformat(),
        'status': 'success' if ics_content else 'error'
    }


def pobierz_plan_ics_nauczyciela(nauczyciel_id: str) -> dict:
    """
    Pobiera plan nauczyciela w formacie ICS:
    - najpierw letni (GG&S=0)
    - potem zimowy (GG&S=1)
    - jeśli oba są dostępne, łączy wydarzenia
    - jeśli żaden nie działa, pobiera GG ogólny
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    ics_links = [
        f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG&S=0",
        f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG&S=1"
    ]
    ogolny_link = f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG"

    ics_contents = []
    for url in ics_links:
        try:
            response = requests.get(url, timeout=30, headers=headers)
            if response.status_code == 200 and "VEVENT" in response.text:
                ics_contents.append((url, response.text))
        except Exception:
            continue

    # Jeśli oba semestry są dostępne, połącz wydarzenia bez duplikatów UID
    if ics_contents:
        if len(ics_contents) == 2:
            from icalendar import Calendar
            cal1 = Calendar.from_ical(ics_contents[0][1])
            cal2 = Calendar.from_ical(ics_contents[1][1])
            merged_cal = Calendar()
            for k, v in cal1.items():
                merged_cal.add(k, v)
            uids = set()
            for component in list(cal1.walk('VEVENT')) + list(cal2.walk('VEVENT')):
                uid = str(component.get('uid', ''))
                if uid not in uids:
                    merged_cal.add_component(component)
                    uids.add(uid)
            return {
                'nauczyciel_id': nauczyciel_id,
                'ics_content': merged_cal.to_ical().decode(),
                'link_ics_zrodlowy': f"{ics_contents[0][0]} + {ics_contents[1][0]}",
                'status': 'success',
                'data_aktualizacji': datetime.datetime.now().isoformat()
            }
        else:
            url, ics_content = ics_contents[0]
            return {
                'nauczyciel_id': nauczyciel_id,
                'ics_content': ics_content,
                'link_ics_zrodlowy': url,
                'status': 'success',
                'data_aktualizacji': datetime.datetime.now().isoformat()
            }

    # Jeśli żaden nie działa, spróbuj ogólnego
    try:
        response = requests.get(ogolny_link, timeout=30, headers=headers)
        if response.status_code == 200 and "VEVENT" in response.text:
            return {
                'nauczyciel_id': nauczyciel_id,
                'ics_content': response.text,
                'link_ics_zrodlowy': ogolny_link,
                'status': 'success',
                'data_aktualizacji': datetime.datetime.now().isoformat()
            }
    except Exception:
        pass

    # Jeśli nic nie działa
    return {
        'nauczyciel_id': nauczyciel_id,
        'ics_content': None,
        'link_ics_zrodlowy': ogolny_link,
        'status': 'error',
        'data_aktualizacji': datetime.datetime.now().isoformat()
    }

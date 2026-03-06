from bs4 import BeautifulSoup
from icalendar import Calendar
import re
from typing import Tuple, List, Dict, Optional, Any

# --- Sekcja: Parsowanie ICS grupy ---

def wyodrebnij_dane_z_summary_grupa(summary: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Ekstrahuje przedmiot, nauczyciela i podgrupę (PG) z opisu ICS GRUPY.
    """
    przedmiot = summary or ""
    nauczyciel = None
    pg = None

    if not isinstance(przedmiot, str):
        przedmiot = str(przedmiot)

    # common patterns: "PRZEDMIOT (PROWADZĄCY): NAUCZYCIEL (PG: X)"
    match = re.search(r"^(.*?)\s*\([^\)]+\):\s*(.+?)(?:\s*\(PG:.*\))?$", przedmiot)
    if match:
        przedmiot = match.group(1).strip()
        nauczyciel = match.group(2).strip()
    else:
        przedmiot = przedmiot.strip()

    pg_match = re.search(r"\(PG:\s*([^)]+)\)", przedmiot)
    if pg_match:
        pg = pg_match.group(1).strip()
    # remove PG marker from teacher if present
    if nauczyciel:
        nauczyciel = re.sub(r"\(PG:.*?\)", "", nauczyciel).strip()
    return przedmiot, nauczyciel or None, pg or None


def parse_ics(
        ics_content: str,
        grupa_id: Optional[str] = None,
        ics_url: Optional[str] = None,
        kod_grupy: Optional[str] = None,
        kierunek_nazwa: Optional[str] = None,
        grupa_map: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Parsuje plik ICS grupy i zwraca listę wydarzeń (zajęć).
    Format wyjściowy zgodny z oczekiwaniami run_events/db: klucze typu subject/start_time/end_time/location/uid/teacher_name/podgrupa/grupa_id/link_ics_zrodlowy/rz
    """
    if not ics_content:
        return []
    events = []
    try:
        cal = Calendar.from_ical(ics_content)
        for component in cal.walk('VEVENT'):
            summary = str(component.get('summary', '') or '')
            categories = component.get('categories')
            dtstart = component.get('dtstart')
            dtend = component.get('dtend')
            start_time = getattr(dtstart, "dt", None)
            end_time = getattr(dtend, "dt", None)
            location = str(component.get('location', '') or '')
            uid = str(component.get('uid', '') or '')

            # RZ z kategorii
            rz = None
            if categories:
                try:
                    if hasattr(categories, 'to_ical'):
                        rz = categories.to_ical().decode(errors="ignore").strip()
                    else:
                        rz = str(categories).strip()
                    if rz and len(rz) > 10:
                        rz = rz[:10]
                except Exception:
                    rz = None

            przedmiot, nauczyciel, podgrupa = wyodrebnij_dane_z_summary_grupa(summary)

            event = {
                "subject": przedmiot,
                "start_time": start_time.isoformat() if hasattr(start_time, "isoformat") else start_time,
                "end_time": end_time.isoformat() if hasattr(end_time, "isoformat") else end_time,
                "location": location,
                "rz": rz,
                "link_ics_zrodlowy": ics_url,
                "podgrupa": podgrupa,
                "uid": uid,
                "teacher_name": nauczyciel,
                "kod_grupy": kod_grupy,
                "kierunek_nazwa": kierunek_nazwa,
                "grupa_id": grupa_id
            }
            events.append(event)
    except Exception as e:
        print(f"Błąd podczas parsowania pliku ICS: {e}")
    return events


# --- Sekcja: Parsowanie szczegółów grupy z HTML ---

def parse_grupa_details(html_content: str) -> Dict[str, Any]:
    """
    Parsuje HTML planu zajęć grupy, wyciągając kod grupy, tryb studiów i semestr.
    """
    if not html_content:
        return {}

    soup = BeautifulSoup(html_content, 'html.parser')

    # Kod grupy z drugiego H2 (np. 21F-ANG-SD23)
    h2_elements = soup.find_all('h2')
    kod_grupy = ""
    if len(h2_elements) >= 2:
        kod_grupy = h2_elements[1].get_text(strip=True) or ""

    # Informacje z H3
    h3 = soup.find('h3')
    kierunek_nazwa = None
    tryb_studiow = None
    semestr = None

    if h3:
        h3_html = str(h3)
        parts = re.split(r'<br\s*/?>', h3_html, flags=re.IGNORECASE)
        if len(parts) > 0:
            kierunek_nazwa = BeautifulSoup(parts[0], 'html.parser').get_text(strip=True) or None
        if len(parts) > 1:
            druga_czesc = BeautifulSoup(parts[1], 'html.parser').get_text(strip=True)
            if 'niestacjonarne' in druga_czesc.lower():
                tryb_studiow = 'niestacjonarne'
            elif 'stacjonarne' in druga_czesc.lower():
                tryb_studiow = 'stacjonarne'
        if len(parts) > 2:
            trzecia_czesc = BeautifulSoup(parts[2], 'html.parser').get_text(strip=True)
            if 'letni' in trzecia_czesc.lower():
                semestr = 'letni'
            elif 'zimowy' in trzecia_czesc.lower():
                semestr = 'zimowy'

    # fallback: if kod_grupy empty keep None
    kod_grupy = kod_grupy or None

    return {
        "kod_grupy": kod_grupy,
        "tryb_studiow": tryb_studiow,
        "semestr": semestr,
        "kierunek_nazwa": kierunek_nazwa,
    }
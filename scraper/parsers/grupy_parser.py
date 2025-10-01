from bs4 import BeautifulSoup
from icalendar import Calendar
import re
from typing import Tuple, List, Dict, Optional, Any


# --- Sekcja: Parsowanie ICS grupy ---

def wyodrebnij_dane_z_summary_grupa(summary: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Ekstrahuje przedmiot, nauczyciela i podgrupę (PG) z opisu ICS GRUPY.
    """
    przedmiot = summary
    nauczyciel = None
    pg = None
    match = re.search(r"^(.*?)\s*\([^\)]+\):\s*(.+?)(?:\s*\(PG:.*\))?$", summary)
    if match:
        przedmiot = match.group(1).strip()
        nauczyciel = match.group(2).strip()
    else:
        przedmiot = summary.strip()
    pg_match = re.search(r"\(PG:\s*([^)]+)\)", summary)
    if pg_match:
        pg = pg_match.group(1).strip()
    if nauczyciel:
        nauczyciel = re.sub(r"\(PG:.*?\)", "", nauczyciel).strip()
    return przedmiot, nauczyciel, pg


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
    """
    if not ics_content:
        return []
    events = []
    try:
        cal = Calendar.from_ical(ics_content)
        for component in cal.walk('VEVENT'):
            summary = str(component.get('summary', ''))
            categories = component.get('categories')
            start_time = component.get('dtstart').dt
            end_time = component.get('dtend').dt
            location = str(component.get('location', ''))
            uid = str(component.get('uid', ''))

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
                "przedmiot": przedmiot,
                "od": start_time.isoformat() if hasattr(start_time, "isoformat") else start_time,
                "do_": end_time.isoformat() if hasattr(end_time, "isoformat") else end_time,
                "miejsce": location,
                "rz": rz,
                "link_ics_zrodlowy": ics_url,
                "podgrupa": podgrupa,
                "uid": uid,
                "nauczyciel_nazwa": nauczyciel,
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
    soup = BeautifulSoup(html_content, 'html.parser')

    # Sekcja: Kod grupy z drugiego H2 (Figma: Nagłówek grupy - np. 21F-ANG-SD23)
    h2_elements = soup.find_all('h2')
    kod_grupy = ""
    if len(h2_elements) >= 2:
        kod_grupy = h2_elements[1].get_text(strip=True)

    # Sekcja: Informacje z H3 (Figma: Szczegóły kierunku, trybu i semestru)
    h3 = soup.find('h3')
    kierunek_nazwa = None
    tryb_studiow = None
    semestr = None

    if h3:
        # Pobierz HTML zawartość H3 i podziel po <br>
        h3_html = str(h3)

        # Podziel zawartość po <br> i <br />
        import re
        parts = re.split(r'<br\s*/?>', h3_html, flags=re.IGNORECASE)

        # Pierwsza część: nazwa kierunku (przed pierwszym <br>)
        if len(parts) > 0:
            kierunek_nazwa = BeautifulSoup(parts[0], 'html.parser').get_text(strip=True)

        # Druga część: tryb studiów (po pierwszym <br>)
        if len(parts) > 1:
            druga_czesc = BeautifulSoup(parts[1], 'html.parser').get_text(strip=True)

            # Sprawdź czy zawiera "stacjonarne" czy "niestacjonarne"
            if 'niestacjonarne' in druga_czesc.lower():
                tryb_studiow = 'niestacjonarne'
            elif 'stacjonarne' in druga_czesc.lower():
                tryb_studiow = 'stacjonarne'

        # Trzecia część: semestr (po drugim <br>) - tylko słowo "letni" lub "zimowy"
        if len(parts) > 2:
            trzecia_czesc = BeautifulSoup(parts[2], 'html.parser').get_text(strip=True)
            if 'letni' in trzecia_czesc.lower():
                semestr = 'letni'
            elif 'zimowy' in trzecia_czesc.lower():
                semestr = 'zimowy'

    print(f"  Parser: kod='{kod_grupy}', tryb='{tryb_studiow}', semestr='{semestr}', kierunek='{kierunek_nazwa}'")

    return {
        "kod_grupy": kod_grupy,
        "tryb_studiow": tryb_studiow,
        "semestr": semestr,
        "kierunek_nazwa": kierunek_nazwa
    }

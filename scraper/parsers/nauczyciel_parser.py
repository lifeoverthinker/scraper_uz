from bs4 import BeautifulSoup
from scraper.utils import sanitize_string, fetch_page
from icalendar import Calendar
import re
from typing import List, Dict, Optional, Any

BASE_URL = "https://plan.uz.zgora.pl/"


def sprawdz_nieregularne_zajecia(html: str, identyfikator: str = "") -> bool:
    """
    Sprawdza, czy w planie znajduje się rubryka 'Nieregularne' lub 'brak zaplanowanych zajęć'.
    Zwraca True jeśli tak.
    """
    if not html:
        return False

    soup = BeautifulSoup(html, "html.parser")
    html_lower = html.lower()

    # Sprawdź różne warianty
    if ("nieregularne" in html_lower or
            "brak zaplanowanych zajęć" in html_lower or
            "brak zajęć" in html_lower):

        # Sprawdź czy to jedyne zajęcia (czy są tylko nieregularne)
        td = soup.find("td", class_="gray-day")
        if td and "nieregularne" in td.get_text(strip=True).lower():
            # Sprawdź czy są jakieś inne zajęcia poza nieregularnymi
            regular_rows = soup.find_all("tr", class_=lambda x: x and "dayn" not in x and "gray" not in x)
            if not regular_rows or len(regular_rows) <= 1:  # Tylko nagłówek
                print(f"ℹ️ Plan {identyfikator} zawiera tylko zajęcia nieregularne – nie pobieram ICS.")
                return True
            else:
                print(f"ℹ️ Plan {identyfikator} zawiera zajęcia nieregularne – te zajęcia NIE będą obecne w pliku ICS.")
                return False

    return False


def parse_nauczyciele_from_group_page(html: str, grupa_id: str = None) -> List[Dict[str, Any]]:
    """Parsuje HTML planu zajęć grupy i wyodrębnia linki do stron nauczycieli."""
    if html is None:
        print(f"❌ Strona grupy jest pusta, pomijam parse_nauczyciele_from_group_page")
        return []
    sprawdz_nieregularne_zajecia(html, f"grupy {grupa_id}" if grupa_id else "")
    soup = BeautifulSoup(html, "html.parser")
    wynik = []
    znalezieni_nauczyciele = set()
    nauczyciel_links = soup.find_all("a", href=lambda href: href and "nauczyciel_plan.php?ID=" in href)
    for link in nauczyciel_links:
        nauczyciel_url = BASE_URL + link["href"]
        nauczyciel_id = link["href"].split("ID=")[1] if "ID=" in link["href"] else None
        nauczyciel_name = sanitize_string(link.get_text(strip=True))
        if nauczyciel_url not in znalezieni_nauczyciele:
            znalezieni_nauczyciele.add(nauczyciel_url)
            nauczyciel_data = {
                "nazwa": nauczyciel_name,
                "link": nauczyciel_url,
                "nauczyciel_id": nauczyciel_id
            }
            if grupa_id:
                nauczyciel_data["grupa_id"] = grupa_id
            wynik.append(nauczyciel_data)
    return wynik


def parse_nauczyciel_details(html: str, nauczyciel_id: str = None) -> Dict[str, Any]:
    if html is None:
        print(f"❌ Strona nauczyciela jest pusta, pomijam parse_nauczyciel_details")
        return {}

    sprawdz_nieregularne_zajecia(html, f"nauczyciela {nauczyciel_id}" if nauczyciel_id else "")

    soup = BeautifulSoup(html, "html.parser")
    dane = {}

    # Imię i nazwisko (drugi H2 po "Plan zajęć")
    h2_tags = soup.find_all("h2")
    for h2 in h2_tags:
        text = h2.get_text(strip=True)
        if text and "Plan zajęć" not in text:
            dane["nauczyciel_nazwa"] = sanitize_string(text)
            break

    # Instytuty/wydziały (każdy h3)
    instytuty = []
    for h3 in soup.find_all("h3"):
        sublines = [frag.strip() for frag in h3.stripped_strings if frag.strip()]
        instytuty.extend(sublines)
    if instytuty:
        dane["instytut"] = " | ".join(instytuty)

    # Email
    email = None
    for h4 in soup.find_all("h4"):
        a = h4.find("a", href=lambda href: href and "mailto:" in href)
        if a:
            email = a.get_text(strip=True)
            break
    if not email:
        a = soup.find("a", href=lambda href: href and "mailto:" in href)
        if a:
            email = a.get_text(strip=True)
    if email:
        dane["email"] = email

    # Link do strony HTML planu nauczyciela
    if nauczyciel_id:
        dane["link_strony_nauczyciela"] = f"{BASE_URL}nauczyciel_plan.php?ID={nauczyciel_id}"

        # Link do ICS nauczyciela: preferuj letni, potem zimowy, na końcu ogólny
        ics_links = [
            f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG&S=0",
            f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG&S=1",
            f"{BASE_URL}nauczyciel_ics.php?ID={nauczyciel_id}&KIND=GG"
        ]
        import requests
        for url in ics_links:
            try:
                resp = requests.head(url, timeout=5)
                if resp.status_code == 200 and "text/calendar" in resp.headers.get("content-type", ""):
                    dane["link_ics_nauczyciela"] = url
                    break
            except Exception:
                continue
        # Jeśli żaden nie działa, ustaw ogólny
        if "link_ics_nauczyciela" not in dane:
            dane["link_ics_nauczyciela"] = ics_links[-1]

    return dane

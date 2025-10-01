from bs4 import BeautifulSoup
from scraper.utils import fetch_page

BASE_URL = "https://plan.uz.zgora.pl/"


def scrape_kierunki() -> list[dict]:
    """
    Pobiera i zwraca listę kierunków studiów z głównej strony planu.
    Zwraca listę słowników: {'nazwa': ..., 'wydzial': ..., 'link_strony_kierunku': ...}
    """
    URL = BASE_URL + "grupy_lista_kierunkow.php"
    print(f"🔍 Pobieram dane z: {URL}")
    html = fetch_page(URL)
    if not html:
        print("❌ Nie udało się pobrać strony z listą kierunków.")
        return []
    return parse_departments_and_courses(html)


def parse_departments_and_courses(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("div", class_="container main")
    if not container:
        print("❌ Nie znaleziono głównego kontenera.")
        return []
    kierunki = []
    current_wydzial = None
    for element in container.find_all("li", class_="lista-grup-item"):
        sub_ul = element.find("ul", class_="lista-grup")
        if sub_ul:
            # To jest nagłówek wydziału
            current_wydzial = element.contents[0].strip()
            continue
        anchor = element.find("a", href=True)
        # Pomijaj, jeśli nie ma linku lub nie ma tekstu (czyli nie jest to kierunek)
        if not anchor or "ID=" not in anchor['href'] or not anchor.text.strip():
            continue
        nazwa_kierunku = anchor.text.strip()
        link_strony_kierunku = BASE_URL + anchor['href'] if not anchor['href'].startswith('http') else anchor['href']
        kierunki.append({
            "nazwa": nazwa_kierunku,
            "wydzial": current_wydzial,
            "link_strony_kierunku": link_strony_kierunku
        })
    # Ostateczny filtr na wszelki wypadek
    kierunki = [k for k in kierunki if k.get('nazwa') and k.get('nazwa').strip()]
    return kierunki

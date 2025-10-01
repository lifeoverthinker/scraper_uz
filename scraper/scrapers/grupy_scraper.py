from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from scraper.utils import fetch_page
from scraper.downloader import BASE_URL
from scraper.parsers.grupy_parser import parse_grupa_details

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable


def find_ics_links(html_grupy):
    soup = BeautifulSoup(html_grupy, "html.parser")
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'grupy_ics.php' in href:
            full_link = href if href.startswith('http') else BASE_URL + href.lstrip('/')
            links.append(full_link)
    return links


def parse_grupa_with_fetch(link, nazwa_kierunku, wydzial, kierunek_id):
    html_grupy = fetch_page(link)
    if not html_grupy:
        print(f"⚠️ Nie udało się pobrać HTML dla grupy: {link}")
        return []

    szczegoly = parse_grupa_details(html_grupy)
    kod_grupy = szczegoly.get('kod_grupy', '')
    tryb_studiow = szczegoly.get('tryb_studiow')
    ics_links = find_ics_links(html_grupy)

    import re
    m = re.search(r'ID=(\d+)', link)
    grupa_id = m.group(1) if m else None

    # Debug: log informacji o przetwarzanej grupie
    print(f"  Debug grupa: kod={kod_grupy}, id={grupa_id}, ics_count={len(ics_links)}, tryb={tryb_studiow}")

    grupy = []
    # Zmieniony warunek - wymaga tylko grupa_id (kod_grupy może być pusty)
    if grupa_id:
        # Jeśli nie ma linków ICS, utwórz jeden standardowy
        if not ics_links:
            ics_links = [f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG"]

        for ics_link in ics_links:
            grupy.append({
                'kod_grupy': kod_grupy or f"GRUPA-{grupa_id}",  # Fallback jeśli brak kodu
                'kierunek_id': kierunek_id,
                'kierunek_nazwa': nazwa_kierunku,  # DODANE - potrzebne do mapowania UUID
                'wydzial': wydzial,  # DODANE - potrzebne do mapowania UUID
                'link_strony_grupy': link,
                'link_ics_grupy': ics_link,
                'tryb_studiow': tryb_studiow,
                'grupa_id': grupa_id
            })
    else:
        print(f"⚠️ Brak grupa_id dla linku: {link}")

    return grupy


def parse_grupy(html, nazwa_kierunku, wydzial, kierunek_id, max_workers=10):
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find("table", class_="table-bordered")
    if not table:
        print(f"⚠️ Brak grup na stronie kierunku: {nazwa_kierunku}")
        return []

    all_links = []
    for row in table.find_all("tr"):
        td = row.find("td")
        if not td:
            continue
        a = td.find("a")
        if not a:
            continue
        grupa_href = a.get("href")
        if not grupa_href:
            continue
        full_link = f"{BASE_URL}{grupa_href}" if not grupa_href.startswith('http') else grupa_href
        all_links.append(full_link)

    print(f"  Znaleziono {len(all_links)} linków grup dla {nazwa_kierunku}")

    grupy = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(parse_grupa_with_fetch, link, nazwa_kierunku, wydzial, kierunek_id)
            for link in all_links
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Grupy"):
            result = future.result()
            grupy.extend(result)
            if result:
                print(f"  Pobrano {len(result)} grup z linku")

    return grupy


def remove_duplicates(grupy):
    seen = set()
    unique = []
    for g in grupy:
        # Sekcja: Deduplikacja grup (Figma: Kod grupy, Kierunek ID)
        key = (g['kod_grupy'], g.get('kierunek_id'))
        if key not in seen:
            unique.append(g)
            seen.add(key)
    return unique


def scrape_grupy_for_kierunki(kierunki, verbose=True, max_workers=10):
    wszystkie_grupy = []
    for kierunek in tqdm(kierunki, desc="Kierunki"):
        if verbose:
            print(f"Pobieram grupy dla kierunku: {getattr(kierunek, 'nazwa', None) or kierunek.get('nazwa')}")

        # Sekcja: Pobranie danych kierunku (Figma: Link, Wydział, Nazwa, ID)
        link_kierunku = (getattr(kierunek, 'link_strony_grupy', None) or
                         kierunek.get('link_strony_grupy') or
                         getattr(kierunek, 'link_strony_kierunku', None) or
                         kierunek.get('link_strony_kierunku'))
        wydzial = getattr(kierunek, 'wydzial', None) or kierunek.get('wydzial')
        nazwa_kierunku = getattr(kierunek, 'nazwa', None) or kierunek.get('nazwa')
        kierunek_id = getattr(kierunek, 'id', None) or kierunek.get('id')

        if not link_kierunku:
            print(f"⚠️ Brak linku dla kierunku: {nazwa_kierunku}")
            continue

        html = fetch_page(link_kierunku)
        if not html:
            print(f"⚠️ Nie udało się pobrać HTML dla kierunku: {nazwa_kierunku}")
            continue

        grupy = parse_grupy(html, nazwa_kierunku, wydzial, kierunek_id, max_workers=max_workers)
        wszystkie_grupy.extend(grupy)
        print(f"  Pobrano łącznie {len(grupy)} grup dla kierunku")

    # Sekcja: Deduplikacja wszystkich grup (Figma: Usuwanie duplikatów)
    wszystkie_grupy = remove_duplicates(wszystkie_grupy)
    print(f"Po deduplikacji: {len(wszystkie_grupy)} unikalnych grup")
    return wszystkie_grupy


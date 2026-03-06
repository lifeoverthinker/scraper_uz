from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
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
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "grupy_ics.php" in href:
            full_link = href if href.startswith("http") else BASE_URL + href.lstrip("/")
            links.append(full_link)
    return links


def _prefer_ics_link(ics_links, grupa_id):
    if not ics_links:
        return f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG"

    lowered = [link.lower() for link in ics_links]
    for marker in ("&s=0", "&s=1"):
        for idx, link in enumerate(lowered):
            if marker in link:
                return ics_links[idx]

    return ics_links[0]


def parse_grupa_with_fetch(link, nazwa_kierunku, wydzial, kierunek_id):
    html_grupy = fetch_page(link)
    if not html_grupy:
        print(f"Nie udalo sie pobrac HTML dla grupy: {link}")
        return []

    szczegoly = parse_grupa_details(html_grupy)
    kod_grupy = szczegoly.get("kod_grupy", "")
    tryb_studiow = szczegoly.get("tryb_studiow")
    semestr = szczegoly.get("semestr")
    parsed_kierunek_nazwa = szczegoly.get("kierunek_nazwa")
    ics_links = find_ics_links(html_grupy)

    match = re.search(r"ID=(\d+)", link)
    grupa_id = match.group(1) if match else None

    print(
        f"  Debug grupa: kod={kod_grupy}, id={grupa_id}, "
        f"ics_count={len(ics_links)}, tryb={tryb_studiow}, semestr={semestr}"
    )

    if not grupa_id:
        print(f"Brak grupa_id dla linku: {link}")
        return []

    return [{
        "kod_grupy": kod_grupy or f"GRUPA-{grupa_id}",
        "kierunek_id": kierunek_id,
        "kierunek_nazwa": parsed_kierunek_nazwa or nazwa_kierunku,
        "wydzial": wydzial,
        "link_strony_grupy": link,
        "link_ics_grupy": _prefer_ics_link(ics_links, grupa_id),
        "tryb_studiow": tryb_studiow,
        "semestr": semestr,
        "grupa_id": grupa_id,
    }]


def parse_grupy(html, nazwa_kierunku, wydzial, kierunek_id, max_workers=10):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="table-bordered")
    if not table:
        print(f"Brak grup na stronie kierunku: {nazwa_kierunku}")
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
        full_link = f"{BASE_URL}{grupa_href}" if not grupa_href.startswith("http") else grupa_href
        all_links.append(full_link)

    print(f"  Znaleziono {len(all_links)} linkow grup dla {nazwa_kierunku}")

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
        grupa_id = g.get("grupa_id")
        if grupa_id:
            key = ("grupa_id", str(grupa_id))
        else:
            key = (
                "fallback",
                g.get("kod_grupy"),
                g.get("kierunek_nazwa"),
                g.get("wydzial"),
                g.get("tryb_studiow"),
                g.get("semestr"),
            )

        if key not in seen:
            unique.append(g)
            seen.add(key)
    return unique


def scrape_grupy_for_kierunki(kierunki, verbose=True, max_workers=10):
    wszystkie_grupy = []
    for kierunek in tqdm(kierunki, desc="Kierunki"):
        if verbose:
            print(f"Pobieram grupy dla kierunku: {getattr(kierunek, 'nazwa', None) or kierunek.get('nazwa')}")

        link_kierunku = (
            getattr(kierunek, "link_strony_grupy", None)
            or kierunek.get("link_strony_grupy")
            or getattr(kierunek, "link_strony_kierunku", None)
            or kierunek.get("link_strony_kierunku")
        )
        wydzial = getattr(kierunek, "wydzial", None) or kierunek.get("wydzial")
        nazwa_kierunku = getattr(kierunek, "nazwa", None) or kierunek.get("nazwa")
        kierunek_id = getattr(kierunek, "id", None) or kierunek.get("id")

        if not link_kierunku:
            print(f"Brak linku dla kierunku: {nazwa_kierunku}")
            continue

        html = fetch_page(link_kierunku)
        if not html:
            print(f"Nie udalo sie pobrac HTML dla kierunku: {nazwa_kierunku}")
            continue

        grupy = parse_grupy(html, nazwa_kierunku, wydzial, kierunek_id, max_workers=max_workers)
        wszystkie_grupy.extend(grupy)
        print(f"  Pobrano lacznie {len(grupy)} grup dla kierunku")

    wszystkie_grupy = remove_duplicates(wszystkie_grupy)
    print(f"Po deduplikacji: {len(wszystkie_grupy)} unikalnych grup")
    return wszystkie_grupy

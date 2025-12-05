import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

URL = "https://uz.zgora.pl/kalendarz"
BASE_URL = "https://uz.zgora.pl"

# Mapa polskich miesięcy na numery
MONTHS_MAP = {
    "styczeń": 1, "luty": 2, "marzec": 3, "kwiecień": 4, "maj": 5, "czerwiec": 6,
    "lipiec": 7, "sierpień": 8, "wrzesień": 9, "październik": 10, "listopad": 11, "grudzień": 12
}


def clean_text(text):
    """Usuwa zbędne białe znaki i nową linię."""
    if not text:
        return ""
    return " ".join(text.split())


def get_text_after_label(item, label_text):
    """Pobiera tekst znajdujący się bezpośrednio po pogrubionej etykiecie (np. 'Data:')."""
    label_tag = item.find("b", string=re.compile(label_text))
    if label_tag and label_tag.next_sibling:
        return clean_text(label_tag.next_sibling)
    return ""


def parse_events():
    try:
        response = requests.get(URL)
        response.raise_for_status()
        html = response.text
    except Exception as e:
        print(f"Błąd pobierania strony: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    events = []

    current_date = datetime.now()

    for item in soup.select(".calendar-list-item"):
        try:
            # 1. Tytuł i Link
            title_tag = item.select_one(".h4 a.title")
            if not title_tag:
                continue

            title = clean_text(title_tag.text)
            link = title_tag["href"]
            if not link.startswith("http"):
                link = BASE_URL + link

            # 2. Data (Logika: Najpierw box boczny, potem tekst 'Data:')
            day_tag = item.select_one(".date-cont .day")
            month_tag = item.select_one(".date-cont .month")

            event_date_obj = None
            date_iso = ""
            display_date = ""

            if day_tag and month_tag:
                day = clean_text(day_tag.text)
                month_name = clean_text(month_tag.text).lower()
                month_num = MONTHS_MAP.get(month_name, current_date.month)

                # Prosta logika roku: jeśli miesiąc wydarzenia jest mniejszy niż aktualny (np. jest grudzień, a wydarzenie w styczniu),
                # to zakładamy następny rok. W przeciwnym razie bieżący.
                year = current_date.year
                if month_num < current_date.month - 1:  # Margines błędu
                    year += 1

                try:
                    event_date_obj = datetime(year, month_num, int(day))
                    date_iso = event_date_obj.strftime("%Y-%m-%d")  # Format do bazy danych (sortowanie)
                    display_date = event_date_obj.strftime("%d.%m.%Y")  # Format do wyświetlania
                except ValueError:
                    pass

            # Jeśli data nie została ustalona z boxa, próbujemy z pola tekstowego "Data:"
            if not event_date_obj:
                date_text = get_text_after_label(item, "Data:")
                if date_text:
                    display_date = date_text  # Tutaj może być zakres np. "24.10 - 05.12.2025"
                    # Próba wyciągnięcia pierwszej daty do sortowania
                    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text)
                    if match:
                        d, m, y = match.groups()
                        date_iso = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                        try:
                            event_date_obj = datetime(int(y), int(m), int(d))
                        except:
                            pass

            # Pomijanie wydarzeń przeszłych
            if event_date_obj and event_date_obj.date() < current_date.date():
                continue

            # 3. Godzina
            time_info = get_text_after_label(item, "Godzina:")
            if time_info:
                time_info = time_info.replace("-", "–").strip()  # Ujednolicenie myślnika

            # 4. Lokalizacja
            location = get_text_after_label(item, "Miejsce wydarzenia:")

            # 5. Organizator
            organizer = get_text_after_label(item, "Organizator:")

            # 6. Cena / Wstęp / Opis
            # Ponieważ lista nie ma pełnego opisu, budujemy go z ceny i kategorii
            price = get_text_after_label(item, "Cena wejścia:")
            if not price:
                full_text = item.get_text().lower()
                if "wstęp wolny" in full_text or "bezpłatne" in full_text:
                    price = "Wstęp wolny"
                else:
                    price = "Informacje u organizatora"

            # 7. Obrazek
            img_tag = item.select_one("img")
            image_url = ""
            if img_tag and img_tag.get("src"):
                src = img_tag["src"]
                if not src.startswith("http"):
                    image_url = BASE_URL + src
                else:
                    image_url = src

            events.append({
                "title": title,
                "date_iso": date_iso,  # Do sortowania: "2025-12-04"
                "display_date": display_date,  # Do wyświetlania: "04.12.2025"
                "time": time_info,
                "location": location,
                "description": price,  # Używamy ceny jako krótkiego opisu w liście
                "organizer": organizer,
                "image_url": image_url,
                "link": link
            })

        except Exception as e:
            print(f"Błąd przetwarzania wydarzenia: {e}")
            continue

    return events


if __name__ == "__main__":
    import json

    data = parse_events()
    # Wypisujemy JSON na standardowe wyjście, aby Android/Server mógł to odebrać
    print(json.dumps(data, ensure_ascii=False, indent=4))
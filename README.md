# Scraper Planu Zajęć UZ (XML Edition)

Prosty scraper, który pobiera plan zajęć Uniwersytetu Zielonogórskiego z plików XML i zapisuje dane do Supabase.

## Co robi projekt
- pobiera kierunki, grupy i nauczycieli z XML,
- zapisuje dane do bazy Supabase,
- pobiera plany zajęć grup i nauczycieli,
- aktualizuje dane bez ręcznego klikania.

## Dlaczego XML
- XML jest szybszy i prostszy niż scrapowanie HTML,
- dane są bardziej stabilne,
- łatwiej je przetwarzać w Pythonie.

## Najważniejsze tabele w bazie
- `zajecia_grupy` / `zajecia_nauczyciela` - zajęcia i terminy,
- `nauczyciele` - imię/nazwisko, e-mail, jednostka,
- `grupy` - nazwa grupy, tryb, semestr,
- `kierunki` - lista kierunków,
- `semester_state` - bieżący stan semestru.

## Uruchomienie lokalne
1. Dodaj plik `.env` z danymi:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
2. Zainstaluj zależności:
   - `pip install -r scraper/requirements.txt`
3. Uruchom projekt:
   - `python -m scraper.main`

## GitHub Actions
Repozytorium ma workflow `sync.yml`, który uruchamia synchronizację automatycznie kilka razy dziennie.

## Krótko do opisu w inżynierce
Projekt zamienia ręczne pobieranie danych z planu zajęć na automatyczną synchronizację z XML do bazy danych. Dzięki temu dane są pobierane szybciej, prościej i w bardziej uporządkowany sposób.

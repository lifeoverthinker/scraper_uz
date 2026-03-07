# Scraper Planu Zajęć UZ (XML Edition)

System automatycznego pobierania i synchronizacji planu zajęć Uniwersytetu Zielonogórskiego z bazą danych Supabase.

## Kluczowe cechy (Inżynierka)
- **100% XML-based**: Rezygnacja z powolnego scrapingu HTML na rzecz natywnych plików XML.
- **Wydajność**: Czas pełnej synchronizacji skrócony z ~60 min do <20 min.
- **Kompletność danych**: Automatyczne wyciąganie maili, instytutów, trybów studiów i semestrów bezpośrednio z nagłówków XML.
- **Automatyzacja**: Integracja z GitHub Actions (workflow `sync.yml`) uruchamiana 5 razy dziennie.

## Struktura bazy (Supabase)
Dane są mapowane w języku polskim dla pełnej spójności z polską dokumentacją XML:
- **zajecia_grupy / zajecia_nauczyciela**: (poczatek, koniec, przedmiot, rodzaj_zajec, sala, nauczyciel, podgrupa)
- **nauczyciele**: (nazwisko_imie, email, jednostka)
- **grupy**: (nazwa, tryb, semestr, grupa_id)
- **semester_state**: Centralne zarządzanie stanem semestrów (aktualny/poprzedni) i datą ostatniej synchronizacji.

## Start lokalny
1. Skonfiguruj plik `.env` (SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY).
2. Zainstaluj biblioteki: `pip install -r scraper/requirements.txt`.
3. Uruchom: `python -m scraper.main`.
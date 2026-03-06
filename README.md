# scraper_uz

Prosty scraper planu UZ (XML-first) zapisujacy do Supabase.

## 1. Wymagania
- Python 3.11+
- zmienne:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY`

Przyklad `.env`:

```env
SUPABASE_URL=https://YOUR_PROJECT.supabase.co
SUPABASE_SERVICE_ROLE_KEY=YOUR_SERVICE_ROLE_KEY
```

## 2. Start lokalny
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r scraper\requirements.txt
python -m scraper.main
```

## 3. Tryby (`SCRAPER_ONLY`)
- `kierunki` - tylko kierunki
- `grupy` - kierunki + grupy
- `grupy_zajecia` - tylko zajecia grup
- `teachers` - tylko zajecia nauczycieli
- brak `SCRAPER_ONLY` - pelny pipeline
- `events_all` - tylko w GitHub Actions: `grupy_zajecia` + `teachers` sekwencyjnie

Przyklad:
```powershell
$env:SCRAPER_ONLY="grupy_zajecia"
python -m scraper.main
```

## 4. XML-first flagi
- `SCRAPER_XML_FIRST=1`
- `SCRAPER_EVENTS_XML_FIRST=1`

Przyklad:
```powershell
$env:SCRAPER_XML_FIRST="1"
$env:SCRAPER_EVENTS_XML_FIRST="1"
python -m scraper.main
```

## 5. GitHub Actions (w tle)
Plik: `.github/workflows/run_scraper.yml`

Co robi workflow:
- ma trigger manualny (`workflow_dispatch`)
- ma harmonogram co 4h
- domyslnie odpala `events_all` (grupy -> nauczyciele, sekwencyjnie)

Sekrety wymagane w repo:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

## 6. Ważne: nie odpalaj rownolegle
Nie uruchamiaj jednoczesnie kilku trybow zapisujacych do tych samych tabel.
Rownolegle runy moga powodowac konflikty upsert.

## 7. Szybka kontrola po runie
```sql
select count(*) as grupy from public.grupy;
select count(*) as zajecia_grupy from public.zajecia_grupy;
select count(*) as nauczyciele from public.nauczyciele;
select count(*) as zajecia_nauczyciela from public.zajecia_nauczyciela;
```

## 8. Typowy blad: `start_time null`
Jesli w XML sa rekordy bez dat/godzin, zapis do `zajecia_grupy` moze wywalic blad NOT NULL.
W kodzie jest filtr odrzucajacy rekordy bez poprawnego `od` / `do_`.

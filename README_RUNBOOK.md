# Scraper UZ – Runbook (Catalog Only)

## Tryb domyślny: `catalog_only`

`catalog_only`:
1. `xml_bootstrap` (stan semestru)
2. `xml_sync` (kierunki, grupy, nauczyciele-katalog)

> Brak synchronizacji eventów (`zajecia_*`).

## Lokalnie (PowerShell)

```bash
pip install -r .\scraper\requirements.txt
$env:SCRAPER_ONLY="catalog_only"
python -m scraper.main
```

## Szybki smoke test

```bash
$env:SCRAPER_ONLY="xml_sync"
$env:SMOKE_LIMIT_DIRECTIONS="2"
$env:SMOKE_LIMIT_GROUPS_PER_DIRECTION="5"
$env:SKIP_TEACHER_ENRICHMENT="1"
python -m scraper.main
```

## Manualne tryby

```bash
$env:SCRAPER_ONLY="xml_bootstrap"; python -m scraper.main
$env:SCRAPER_ONLY="xml_sync";      python -m scraper.main
$env:SCRAPER_ONLY="catalog_only";  python -m scraper.main
```

## Health check SQL

```sql
select current_semester_id, current_semester_name, updated_at
from semester_state
order by updated_at desc;

select count(*) as kierunki_count from public.kierunki;
select count(*) as grupy_count from public.grupy;
select count(*) as nauczyciele_count from public.nauczyciele;

select count(*) as grupy_tryb_unknown
from public.grupy
where lower(coalesce(tryb_studiow,'')) = 'nieznany';
```
# Scraper UZ – Runbook (MVP)

## 1) Dostępne tryby uruchamiania

Aplikacja korzysta ze zmiennej `SCRAPER_ONLY`.

### Tryby produkcyjne
- `xml_bootstrap` – odczyt semestru z XML + zapis `semester_state`
- `xml_sync` – sync kierunków/grup + zajęcia grup + `group_schedule_meta`
- `teacher_sync` – sync zajęć nauczycieli + `teacher_schedule_meta`
- `cleanup_only` – dezaktywacja wygasłych rekordów

### Tryby legacy
- `kierunki`
- `grupy`
- `grupy_zajecia`
- `teachers`

---

## 2) Uruchamianie lokalne (PowerShell)

```bash
$env:SCRAPER_ONLY="xml_bootstrap"; python -m scraper.main
$env:SCRAPER_ONLY="xml_sync"; python -m scraper.main
$env:SCRAPER_ONLY="teacher_sync"; python -m scraper.main
$env:SCRAPER_ONLY="cleanup_only"; python -m scraper.main
```

Pełny (legacy) pipeline:
```bash
Remove-Item Env:SCRAPER_ONLY -ErrorAction SilentlyContinue
python -m scraper.main
```

---

## 3) Kolejność uruchamiania (zalecana)

1. `xml_bootstrap`
2. `xml_sync`
3. `teacher_sync`
4. `cleanup_only`

---

## 4) Workflow GitHub Actions

Workflow: `.github/workflows/run_scraper.yml`

Obsługuje:
- `workflow_dispatch` (manual)
- `schedule` (cron)

Sekrety wymagane w repo:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

---

## 5) Szybki health-check SQL

```sql
select count(*) as grupy_count from public.grupy;
select count(*) as zajecia_grupy_count from public.zajecia_grupy;
select count(*) as active_group_meta from public.group_schedule_meta where is_active = true;

select count(*) as nauczyciele_count from public.nauczyciele;
select count(*) as zajecia_nauczyciela_count from public.zajecia_nauczyciela;
select count(*) as active_teacher_meta from public.teacher_schedule_meta where is_active = true;
```

---

## 6) Typowe problemy i akcje

### A) `XML not found (404)` dla `grupy_lista_grup_kierunku.ID=...`
To normalne dla node'ów katalogowych (wydziały/foldery).  
Nie wymaga akcji, jeśli finalnie sync zapisuje grupy/eventy.

### B) `events_saved = 0`
Sprawdź:
1. czy grupy/nauczyciele istnieją w DB
2. czy linki ICS są poprawne
3. logi workflow (`scraper_run.log` artifact)

### C) `null value violates not-null constraint`
Sprawdź mapowanie payloadu do tabeli oraz wartości domyślne (`tryb_studiow`, FK itd.).

---

## 7) Operacyjne dobre praktyki

- Nie uruchamiaj równolegle kilku pełnych synców.
- Po większych zmianach zawsze zrób:
  - `xml_sync`
  - `teacher_sync`
  - health-check SQL
- Trzymaj artifact logów min. 7 dni.
import aiohttp
import asyncio

BASE_URL = "https://plan.uz.zgora.pl/"


async def fetch_ics_with_fallback(session: aiohttp.ClientSession, grupa_id: str, max_retries: int = 3) -> dict:
    """
    Pobiera ICS-y dla grupy w kolejności:
    1. ...&s=0 (letni)
    2. ...&s=1 (zimowy)
    3. bez &s (domyślny)
    Zwraca dict: {'status', 'ics_content', 'link_ics_zrodlowy', 'grupa_id'}
    """
    urls = [
        f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG&s=0",
        f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG&s=1",
        f"{BASE_URL}grupy_ics.php?ID={grupa_id}&KIND=GG"
    ]
    for url in urls:
        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if text.strip().startswith("BEGIN:VCALENDAR") and "VEVENT" in text:
                            return {
                                'status': 'success',
                                'ics_content': text,
                                'link_ics_zrodlowy': url,
                                'grupa_id': grupa_id
                            }
                        else:
                            break  # To nie jest plik ICS
                    elif resp.status == 404:
                        break  # przejdź do kolejnego url
                    else:
                        await asyncio.sleep(1)
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    continue
    # Jeśli żaden nie istnieje:
    return {
        'status': 'not_found',
        'ics_content': None,
        'link_ics_zrodlowy': urls[-1],
        'grupa_id': grupa_id
    }


async def fetch_all_ics(grupa_ids: list[str], max_concurrent: int = 100) -> list[dict]:
    """
    Asynchronicznie pobiera ICS-y dla wszystkich grup.
    """
    results = []
    sema = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        async def limited_fetch(grupa_id):
            async with sema:
                return await fetch_ics_with_fallback(session, grupa_id)
        tasks = [limited_fetch(grupa_id) for grupa_id in grupa_ids]
        for fut in asyncio.as_completed(tasks):
            result = await fut
            results.append(result)
    return results


def download_ics_for_groups_async(grupa_ids: list[str], max_concurrent: int = 100) -> list[dict]:
    """
    Wywołuje asynchroniczny fetch dla wszystkich grup, z większą współbieżnością.
    """
    return asyncio.run(fetch_all_ics(grupa_ids, max_concurrent=max_concurrent))

import requests
import datetime
import time


def sanitize_string(text: str) -> str:
    """
    Oczyszcza string z niedrukowalnych znaków i nieprawidłowych kodowań.
    """
    if text is None:
        return ""
    polish_chars = "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ"
    return "".join(c for c in str(text) if c.isprintable() or c in polish_chars)


def fetch_page(url: str, max_retries: int = 3, sleep_time: float = 2) -> str:
    """
    Pobiera zawartość strony HTML z obsługą błędów i automatycznymi retry.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"❌ Błąd pobierania strony: {url} — {e}")
            if attempt < max_retries - 1:
                time.sleep(sleep_time)
    # Wszystkie próby się nie powiodły:
    return None


def zajecia_to_serializable(zajecia: list) -> list:
    """
    Zamienia obiekty zajęć na słowniki serializowalne do JSON (np. datetime na string).
    """

    def convert(obj):
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert(x) for x in obj]
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            return obj

    return [convert(z) for z in zajecia]

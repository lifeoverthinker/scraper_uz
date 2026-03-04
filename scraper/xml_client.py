from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DEFAULT_BASE_URL = "https://plan.uz.zgora.pl/static_files/"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class XmlFetchResult:
    url: str
    status_code: int
    content: Optional[str]
    fetched_at_utc: datetime
    from_cache: bool = False


@dataclass(frozen=True)
class SemesterMeta:
    current_semester_id: Optional[str]
    current_semester_name_pl: Optional[str]
    current_semester_name_en: Optional[str]
    previous_semester_id: Optional[str]
    previous_semester_name_pl: Optional[str]
    previous_semester_name_en: Optional[str]
    generated_at: Optional[str]
    source_url: str


class XmlClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 20,
        max_retries: int = 3,
        backoff_start_seconds: float = 1.0,
        user_agent: str = "my_uz_xml_client/1.2",
    ) -> None:
        self.base_url = base_url if base_url.endswith("/") else f"{base_url}/"
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_start_seconds = backoff_start_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
            }
        )

    def fetch_xml(self, file_name: str) -> XmlFetchResult:
        return self._fetch_url(self._build_url(file_name))

    def fetch_raw_url(self, url: str) -> XmlFetchResult:
        return self._fetch_url(url)

    def fetch_semester_meta_from_file(self, file_name: str) -> SemesterMeta:
        result = self.fetch_xml(file_name)
        if not result.content:
            raise ValueError(f"Brak zawartości XML dla {file_name} ({result.url})")
        return self.parse_semester_meta(result.content, source_url=result.url)

    @staticmethod
    def parse_semester_meta(xml_content: str, source_url: str = "") -> SemesterMeta:
        soup = BeautifulSoup(xml_content, "xml")
        root = soup.find("ROOT") or soup.find()
        if root is None:
            raise ValueError("Niepoprawny XML: brak ROOT")

        current_id = _pick_first_value(root, ["SEMESTER_ID", "CURRENT_SEMESTER_ID", "SEMESTR_BIEZACY_ID"])
        current_pl = _pick_first_value(root, ["SEMESTER", "CURRENT_SEMESTER_NAME", "SEMESTR_BIEZACY_NAZWA"])
        current_en = _pick_first_value(root, ["SEMESTER_EN", "CURRENT_SEMESTER_NAME_EN", "SEMESTR_BIEZACY_NAZWA_EN"])

        prev_id = _pick_first_value(root, ["SEMESTER_PREV_ID", "PREVIOUS_SEMESTER_ID", "SEMESTR_POPRZEDNI_ID"])
        prev_pl = _pick_first_value(root, ["SEMESTER_PREV", "PREVIOUS_SEMESTER_NAME", "SEMESTR_POPRZEDNI_NAZWA"])
        prev_en = _pick_first_value(root, ["SEMESTER_PREV_EN", "PREVIOUS_SEMESTER_NAME_EN", "SEMESTR_POPRZEDNI_NAZWA_EN"])

        generated_at = _pick_first_value(root, ["GENERATED", "GENERATED_AT", "DATA_GENEROWANIA", "TIMESTAMP"])

        return SemesterMeta(
            current_semester_id=_clean(current_id),
            current_semester_name_pl=_clean(current_pl),
            current_semester_name_en=_clean(current_en),
            previous_semester_id=_clean(prev_id),
            previous_semester_name_pl=_clean(prev_pl),
            previous_semester_name_en=_clean(prev_en),
            generated_at=_clean(generated_at),
            source_url=source_url,
        )

    def _build_url(self, file_name: str) -> str:
        return urljoin(self.base_url, file_name.lstrip("/"))

    def _fetch_url(self, url: str) -> XmlFetchResult:
        last_exc: Optional[Exception] = None
        backoff = self.backoff_start_seconds

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                status = resp.status_code

                if status == 404:
                    logger.warning("XML not found (404): %s", url)
                    return XmlFetchResult(url=url, status_code=status, content=None, fetched_at_utc=datetime.utcnow())

                if 200 <= status < 300:
                    # KLUCZOWE: wymuszenie UTF-8 (bo serwer czasem podaje złe kodowanie)
                    try:
                        text = resp.content.decode("utf-8", errors="replace")
                    except Exception:
                        resp.encoding = "utf-8"
                        text = resp.text or ""

                    return XmlFetchResult(
                        url=url,
                        status_code=status,
                        content=text,
                        fetched_at_utc=_response_time_or_now(resp),
                    )

                if 500 <= status < 600 and attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                return XmlFetchResult(url=url, status_code=status, content=None, fetched_at_utc=datetime.utcnow())

            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                break

        raise RuntimeError(f"Nie udało się pobrać XML: {url}. Ostatni błąd: {last_exc}") from last_exc


def _pick_first_value(root_tag, candidate_names: list[str]) -> Optional[str]:
    for name in candidate_names:
        found = root_tag.find(lambda t: t.name and t.name.lower() == name.lower())
        if found and found.text:
            return found.text

    attrs = getattr(root_tag, "attrs", {}) or {}
    attrs_lower = {str(k).lower(): v for k, v in attrs.items()}
    for name in candidate_names:
        val = attrs_lower.get(name.lower())
        if val is not None:
            return str(val)
    return None


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip()
    return v if v else None


def _response_time_or_now(resp: requests.Response) -> datetime:
    date_hdr = resp.headers.get("Date")
    if not date_hdr:
        return datetime.utcnow()
    try:
        return parsedate_to_datetime(date_hdr).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()
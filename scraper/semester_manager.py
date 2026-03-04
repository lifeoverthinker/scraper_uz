from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import re

from rapidfuzz import fuzz


@dataclass(frozen=True)
class SemesterState:
    current_semester_id: Optional[str]
    current_semester_name: Optional[str]
    previous_semester_id: Optional[str]
    previous_semester_name: Optional[str]
    generated_at: Optional[str] = None


@dataclass(frozen=True)
class SemesterSwitchResult:
    switched: bool
    old_semester_id: Optional[str]
    new_semester_id: Optional[str]
    reason: str


@dataclass(frozen=True)
class GroupMatchCandidate:
    old_code: str
    new_code: str
    confidence: float
    strategy: str  # exact | year_prefix | fuzzy


# =========================
# Semester switch
# =========================

def detect_semester_switch(
    previous_state: Optional[SemesterState],
    current_state: SemesterState,
) -> SemesterSwitchResult:
    """
    Wykrywa zmianę semestru na podstawie ID semestru.
    """
    if previous_state is None:
        return SemesterSwitchResult(
            switched=False,
            old_semester_id=None,
            new_semester_id=current_state.current_semester_id,
            reason="first_run_no_previous_state",
        )

    old_id = _norm(previous_state.current_semester_id)
    new_id = _norm(current_state.current_semester_id)

    if old_id and new_id and old_id != new_id:
        return SemesterSwitchResult(
            switched=True,
            old_semester_id=old_id,
            new_semester_id=new_id,
            reason="semester_id_changed",
        )

    # fallback: nazwa semestru (zimowy/letni), gdy ID brak
    old_name = _semester_kind(previous_state.current_semester_name)
    new_name = _semester_kind(current_state.current_semester_name)

    if old_name and new_name and old_name != new_name:
        return SemesterSwitchResult(
            switched=True,
            old_semester_id=old_id,
            new_semester_id=new_id,
            reason="semester_name_changed",
        )

    return SemesterSwitchResult(
        switched=False,
        old_semester_id=old_id,
        new_semester_id=new_id,
        reason="no_change",
    )


# =========================
# Group code matching
# =========================

def match_group_code(
    old_code: str,
    new_codes: list[str],
    fuzzy_threshold: float = 78.0,
) -> Optional[GroupMatchCandidate]:
    """
    Zwraca najlepsze dopasowanie dla kodu grupy:
    1) exact
    2) transformacja prefiksu roku (11->21->31...)
    3) fuzzy po znormalizowanym rdzeniu
    """
    if not old_code or not new_codes:
        return None

    old_norm = normalize_group_code(old_code)
    new_norm_map = {normalize_group_code(c): c for c in new_codes}

    # 1) exact
    if old_norm in new_norm_map:
        return GroupMatchCandidate(
            old_code=old_code,
            new_code=new_norm_map[old_norm],
            confidence=100.0,
            strategy="exact",
        )

    # 2) year prefix
    transformed = _increment_year_prefix(old_norm)
    if transformed and transformed in new_norm_map:
        return GroupMatchCandidate(
            old_code=old_code,
            new_code=new_norm_map[transformed],
            confidence=95.0,
            strategy="year_prefix",
        )

    # 3) fuzzy
    best_code = None
    best_score = -1.0
    for n_norm, original in new_norm_map.items():
        score = float(fuzz.ratio(_code_signature(old_norm), _code_signature(n_norm)))
        if score > best_score:
            best_score = score
            best_code = original

    if best_code is not None and best_score >= fuzzy_threshold:
        return GroupMatchCandidate(
            old_code=old_code,
            new_code=best_code,
            confidence=best_score,
            strategy="fuzzy",
        )

    return None


def top_group_code_candidates(
    old_code: str,
    new_codes: list[str],
    top_k: int = 3,
) -> list[GroupMatchCandidate]:
    """
    Kandydaci do modala użytkownika (gdy auto-match niepewny).
    """
    if not old_code or not new_codes:
        return []

    old_sig = _code_signature(normalize_group_code(old_code))
    scored: list[GroupMatchCandidate] = []

    for c in new_codes:
        n = normalize_group_code(c)
        score = float(fuzz.ratio(old_sig, _code_signature(n)))
        scored.append(
            GroupMatchCandidate(
                old_code=old_code,
                new_code=c,
                confidence=score,
                strategy="fuzzy",
            )
        )

    scored.sort(key=lambda x: x.confidence, reverse=True)
    return scored[:top_k]


# =========================
# Helpers
# =========================

def normalize_group_code(code: str) -> str:
    """
    Normalizacja kodu grupy:
    - trim
    - upper
    - usunięcie wielokrotnych spacji
    """
    return re.sub(r"\s+", "", code.strip().upper())


def _increment_year_prefix(code: str) -> Optional[str]:
    """
    Dla wzorca typu 11INF-SP -> 21INF-SP.
    Podnosi tylko pierwszą cyfrę dwucyfrowego prefiksu.
    """
    m = re.match(r"^(\d)(\d)([A-Z].*)$", code)
    if not m:
        return None
    first, second, rest = m.groups()
    if not first.isdigit() or not second.isdigit():
        return None
    new_first = str((int(first) + 1) % 10)
    return f"{new_first}{second}{rest}"


def _code_signature(code: str) -> str:
    """
    Podpis do fuzzy porównań:
    - wycina prefiks rocznika na początku (2 cyfry), żeby lepiej łapać INF-SP vs INF-SSI-SP
    """
    n = normalize_group_code(code)
    n = re.sub(r"^\d{2}", "", n)
    return n


def _semester_kind(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    low = name.strip().lower()
    if "zim" in low:
        return "zimowy"
    if "let" in low:
        return "letni"
    return None


def _norm(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    x = v.strip()
    return x if x else None


def parse_semester_state_from_meta(meta: dict) -> SemesterState:
    """
    Adapter pod Twój XmlClient/SemesterMeta.
    Oczekuje kluczy:
    - current_semester_id
    - current_semester_name_pl / current_semester_name_en
    - previous_semester_id
    - previous_semester_name_pl / previous_semester_name_en
    - generated_at
    """
    current_name = meta.get("current_semester_name_pl") or meta.get("current_semester_name_en")
    previous_name = meta.get("previous_semester_name_pl") or meta.get("previous_semester_name_en")

    return SemesterState(
        current_semester_id=meta.get("current_semester_id"),
        current_semester_name=current_name,
        previous_semester_id=meta.get("previous_semester_id"),
        previous_semester_name=previous_name,
        generated_at=meta.get("generated_at"),
    )
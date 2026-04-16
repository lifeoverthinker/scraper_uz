from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

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


def detect_semester_switch(
    previous_state: Optional[SemesterState],
    current_state: SemesterState,
) -> SemesterSwitchResult:
    """Sprawdza, czy zmienił się semestr."""
    if previous_state is None:
        return SemesterSwitchResult(
            switched=False,
            old_semester_id=None,
            new_semester_id=current_state.current_semester_id,
            reason="first_run_no_previous_state",
        )

    old_id = _clean_text(previous_state.current_semester_id)
    new_id = _clean_text(current_state.current_semester_id)

    if old_id and new_id and old_id != new_id:
        return SemesterSwitchResult(
            switched=True,
            old_semester_id=old_id,
            new_semester_id=new_id,
            reason="semester_id_changed",
        )

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


def match_group_code(
    old_code: str,
    new_codes: list[str],
    fuzzy_threshold: float = 78.0,
) -> Optional[GroupMatchCandidate]:
    """Zwraca najlepsze dopasowanie kodu grupy."""
    if not old_code or not new_codes:
        return None

    old_normalized = normalize_group_code(old_code)
    normalized_map = {normalize_group_code(code): code for code in new_codes}

    if old_normalized in normalized_map:
        return GroupMatchCandidate(
            old_code=old_code,
            new_code=normalized_map[old_normalized],
            confidence=100.0,
            strategy="exact",
        )

    transformed = _increment_year_prefix(old_normalized)
    if transformed and transformed in normalized_map:
        return GroupMatchCandidate(
            old_code=old_code,
            new_code=normalized_map[transformed],
            confidence=95.0,
            strategy="year_prefix",
        )

    best_code = None
    best_score = -1.0
    for normalized_code, original_code in normalized_map.items():
        score = float(fuzz.ratio(_code_signature(old_normalized), _code_signature(normalized_code)))
        if score > best_score:
            best_score = score
            best_code = original_code

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
    """Zwraca kilka najlepszych kandydatów, gdy auto-match nie jest pewny."""
    if not old_code or not new_codes:
        return []

    old_signature = _code_signature(normalize_group_code(old_code))
    scored: list[GroupMatchCandidate] = []

    for code in new_codes:
        normalized = normalize_group_code(code)
        score = float(fuzz.ratio(old_signature, _code_signature(normalized)))
        scored.append(
            GroupMatchCandidate(
                old_code=old_code,
                new_code=code,
                confidence=score,
                strategy="fuzzy",
            )
        )

    scored.sort(key=lambda item: item.confidence, reverse=True)
    return scored[:top_k]


def normalize_group_code(code: str) -> str:
    """Czyści kod grupy: trim, uppercase i bez spacji."""
    return re.sub(r"\s+", "", code.strip().upper())


def _increment_year_prefix(code: str) -> Optional[str]:
    """Zmienia prefiks typu 11INF-SP na 21INF-SP."""
    match = re.match(r"^(\d)(\d)([A-Z].*)$", code)
    if not match:
        return None

    first_digit, second_digit, rest = match.groups()
    new_first_digit = str((int(first_digit) + 1) % 10)
    return f"{new_first_digit}{second_digit}{rest}"


def _code_signature(code: str) -> str:
    """Tworzy krótszy podpis do fuzzy porównań."""
    normalized = normalize_group_code(code)
    return re.sub(r"^\d{2}", "", normalized)


def _semester_kind(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    low = name.strip().lower()
    if "zim" in low:
        return "zimowy"
    if "let" in low:
        return "letni"
    return None


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    return text if text else None


def parse_semester_state_from_meta(meta: dict) -> SemesterState:
    """Konwertuje dane z XmlClient do SemesterState."""
    current_name = meta.get("current_semester_name_pl") or meta.get("current_semester_name_en")
    previous_name = meta.get("previous_semester_name_pl") or meta.get("previous_semester_name_en")

    return SemesterState(
        current_semester_id=meta.get("current_semester_id"),
        current_semester_name=current_name,
        previous_semester_id=meta.get("previous_semester_id"),
        previous_semester_name=previous_name,
        generated_at=meta.get("generated_at"),
    )
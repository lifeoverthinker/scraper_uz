from dataclasses import dataclass
from typing import Optional


@dataclass
class Kierunek:
    nazwa: str
    wydzial: str


@dataclass
class Grupa:
    kod_grupy: str
    kierunek_id: str  # uuid (FK)
    link_strony_grupy: Optional[str] = None
    link_ics_grupy: Optional[str] = None
    tryb_studiow: Optional[str] = None
    semestr: Optional[str] = None


@dataclass
class Nauczyciel:
    nazwa: str
    instytut: Optional[str] = None
    email: Optional[str] = None
    link_strony_nauczyciela: Optional[str] = None
    link_ics_nauczyciela: Optional[str] = None


@dataclass
class ZajeciaGrupy:
    uid: str
    podgrupa: Optional[str]
    od: str
    do_: str
    przedmiot: str
    rz: Optional[str]
    nauczyciel: Optional[str]
    miejsce: Optional[str]
    grupa_id: str  # uuid (FK)
    link_ics_zrodlowy: Optional[str] = None


@dataclass
class ZajeciaNauczyciela:
    uid: str
    od: str
    do_: str
    przedmiot: str
    rz: Optional[str]
    grupy: Optional[str]
    miejsce: Optional[str]
    nauczyciel_id: str  # uuid (FK)
    link_ics_zrodlowy: Optional[str] = None

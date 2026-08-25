from dataclasses import dataclass
from typing import Optional


# Kierunek
@dataclass
class Kierunek:
    nazwa: str
    wydzial: str
    external_id: Optional[str] = None


# Grupa
@dataclass
class Grupa:
    grupa_id: str
    nazwa: str
    kierunek_id: str  # uuid (FK)
    tryb: Optional[str] = None
    semestr: Optional[str] = None
    link_strony_grupy: Optional[str] = None
    link_ics_grupy: Optional[str] = None


# Nauczyciel
@dataclass
class Nauczyciel:
    nazwisko_imie: str
    jednostka: Optional[str] = None
    email: Optional[str] = None
    external_id: Optional[str] = None
    link_strony_nauczyciela: Optional[str] = None
    link_ics_nauczyciela: Optional[str] = None


# Zajęcia grupy
@dataclass
class ZajeciaGrupy:
    uid: str
    grupa_id: str  # uuid (FK)
    przedmiot: str
    poczatek: Optional[str]
    koniec: Optional[str]
    rodzaj_zajec: Optional[str] = None
    sala: Optional[str] = None
    nauczyciel: Optional[str] = None
    podgrupa: Optional[str] = None
    id_semestru: Optional[str] = None
    link_ics_zrodlowy: Optional[str] = None


# Zajęcia nauczyciela
@dataclass
class ZajeciaNauczyciela:
    uid: str
    nauczyciel_id: str  # uuid (FK)
    przedmiot: str
    poczatek: Optional[str]
    koniec: Optional[str]
    rodzaj_zajec: Optional[str] = None
    sala: Optional[str] = None
    grupy: Optional[str] = None
    id_semestru: Optional[str] = None
    link_ics_zrodlowy: Optional[str] = None

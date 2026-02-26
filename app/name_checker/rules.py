import re
from collections.abc import Callable
from dataclasses import dataclass


def _roman_to_int(s: str) -> int:
    rom = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    val = 0
    for i in range(len(s)):
        if i + 1 < len(s) and rom[s[i]] < rom[s[i + 1]]:
            val -= rom[s[i]]
        else:
            val += rom[s[i]]
    return val


@dataclass
class NameRule:
    name: str
    pattern: re.Pattern[str]
    replacement: str
    replacer: Callable[[re.Match[str]], str] | None = None

    def match(self, value: str) -> str | None:
        m = self.pattern.match(value.strip())
        if m is None:
            return None
        if self.replacer is not None:
            return self.replacer(m)
        return self.replacement.format(*m.groups())


RULES: list[NameRule] = [
    NameRule(
        name="NGC",
        pattern=re.compile(r"^NGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="NGC {0}",
    ),
    NameRule(
        name="NGC",
        pattern=re.compile(r"^NGC\s*0*(\d+)([a-zA-Z]{1,3})$", re.IGNORECASE),
        replacement="NGC {0}{1}",
    ),
    NameRule(
        name="SDSS",
        pattern=re.compile(
            r"^SDSSJ\s*(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="SDSS J{0}{1}{2}{3}{4}{5}{6}",
    ),
    NameRule(
        name="PGC",
        pattern=re.compile(r"^PGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="PGC {0}",
    ),
    NameRule(
        name="2MASS",
        pattern=re.compile(r"^2MASSJ\s*(\d{8})([+-])(\d{7})$", re.IGNORECASE),
        replacement="2MASS J{0}{1}{2}",
    ),
    NameRule(
        name="2MASX",
        pattern=re.compile(r"^2MASXJ\s*(\d{8})([+-])(\d{7})$", re.IGNORECASE),
        replacement="2MASX J{0}{1}{2}",
    ),
    NameRule(
        name="WINGS",
        pattern=re.compile(
            r"^WINGSJ\s*(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="WINGS J{0}{1}{2}{3}{4}{5}{6}",
    ),
    NameRule(
        name="SMDG",
        pattern=re.compile(r"^SMDG\s*(\d{7})([+-])(\d{6})$", re.IGNORECASE),
        replacement="SMDG {0}{1}{2}",
    ),
    NameRule(
        name="SMDG",
        pattern=re.compile(r"^SMDG\s*(\d{4})([+-])(\d{2})$", re.IGNORECASE),
        replacement="SMDG {0}{1}{2}",
    ),
    NameRule(
        name="MAGE",
        pattern=re.compile(r"^MAGE\s*(\d{4})([+-])(\d{4})$", re.IGNORECASE),
        replacement="MAGE {0}{1}{2}",
    ),
    NameRule(
        name="FASHI",
        pattern=re.compile(r"^FASHI\s*(\d{4})([+-])(\d{2})$", re.IGNORECASE),
        replacement="FASHI {0}{1}{2}",
    ),
    NameRule(
        name="GAMA",
        pattern=re.compile(r"^GAMA\s*0*(\d+)$", re.IGNORECASE),
        replacement="GAMA {0}",
    ),
    NameRule(
        name="MGC",
        pattern=re.compile(r"^MGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="MGC {0}",
    ),
    NameRule(
        name="M",
        pattern=re.compile(r"^(?:MESSIER|M)\s*0*(\d+)$", re.IGNORECASE),
        replacement="M {0}",
    ),
    NameRule(
        name="HIP",
        pattern=re.compile(r"^HIP\s*0*(\d+)$", re.IGNORECASE),
        replacement="HIP {0}",
    ),
    NameRule(
        name="HD",
        pattern=re.compile(r"^HD\s*0*(\d+)$", re.IGNORECASE),
        replacement="HD {0}",
    ),
    NameRule(
        name="BD",
        pattern=re.compile(r"^BD\s*([+-])(\d{2})(\d+)$", re.IGNORECASE),
        replacement="BD {0}{1}{2}",
    ),
    NameRule(
        name="Andromeda",
        pattern=re.compile(
            r"^(?:And|Andromeda)\s*(\d+|[IVXLCDM]+)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: (
            f"Andromeda {int(m.group(1)) if m.group(1).isdigit() else _roman_to_int(m.group(1).upper())}"
        ),
    ),
    NameRule(
        name="ESO",
        pattern=re.compile(r"^ESO\s*0*(\d+)-0*(\d+)$", re.IGNORECASE),
        replacement="ESO {0}-{1}",
    ),
    NameRule(
        name="CGCG",
        pattern=re.compile(r"^CGCG\s*(\d{3})-0*(\d{2,3})$", re.IGNORECASE),
        replacement="CGCG {0}-{1}",
    ),
    NameRule(
        name="KUG",
        pattern=re.compile(r"^KUG\s*(\d{4})([+-])(\d{2,3})$", re.IGNORECASE),
        replacement="KUG {0}{1}{2}",
    ),
    NameRule(
        name="SBS",
        pattern=re.compile(r"^SBS\s*(\d{4})([+-])(\d{2,3})$", re.IGNORECASE),
        replacement="SBS {0}{1}{2}",
    ),
    NameRule(
        name="AM",
        pattern=re.compile(r"^AM\s*(\d{4})([+-])(\d{2,3})$", re.IGNORECASE),
        replacement="AM {0}{1}{2}",
    ),
    NameRule(
        name="HIPASS",
        pattern=re.compile(r"^HIPASS\s*J\s*(\d{4})([+-])(\d{2})([a-z])?$", re.IGNORECASE),
        replacement="HIPASS J{0}{1}{2}",
        replacer=lambda m: f"HIPASS J{m.group(1)}{m.group(2)}{m.group(3)}{m.group(4) or ''}",
    ),
    NameRule(
        name="UGCA",
        pattern=re.compile(r"^UGCA\s*0*(\d+)$", re.IGNORECASE),
        replacement="UGCA {0}",
    ),
    NameRule(
        name="UGC",
        pattern=re.compile(r"^UGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="UGC {0}",
    ),
    NameRule(
        name="IC",
        pattern=re.compile(r"^IC\s*0*(\d+)$", re.IGNORECASE),
        replacement="IC {0}",
    ),
    NameRule(
        name="DDO",
        pattern=re.compile(r"^DDO\s*0*(\d+)$", re.IGNORECASE),
        replacement="DDO {0}",
    ),
    NameRule(
        name="AGC",
        pattern=re.compile(r"^AGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="AGC {0}",
    ),
    NameRule(
        name="VCC",
        pattern=re.compile(r"^VCC\s*0*(\d+)$", re.IGNORECASE),
        replacement="VCC {0}",
    ),
    NameRule(
        name="EVCC",
        pattern=re.compile(r"^EVCC\s*0*(\d+)$", re.IGNORECASE),
        replacement="EVCC {0}",
    ),
    NameRule(
        name="UCD",
        pattern=re.compile(r"^UCD\s*0*(\d+)$", re.IGNORECASE),
        replacement="UCD {0}",
    ),
    NameRule(
        name="Dw",
        pattern=re.compile(r"^Dw\s*(\d{4})([+-])(\d{2})(\d{2})?(b)?$", re.IGNORECASE),
        replacement="Dw {0}{1}{2}",
        replacer=lambda m: f"Dw {m.group(1)}{m.group(2)}{m.group(3)}{m.group(4) or ''}{(m.group(5) or '').lower()}",
    ),
    NameRule(
        name="KSP-DW",
        pattern=re.compile(r"^KSP-DW\s*0*(\d+)$", re.IGNORECASE),
        replacement="KSP-DW {0}",
    ),
    NameRule(
        name="LSBC",
        pattern=re.compile(r"^LSBC\s*D\s*0*(\d+)-0*(\d+)$", re.IGNORECASE),
        replacement="LSBC D{0}-{1}",
    ),
    NameRule(
        name="LV",
        pattern=re.compile(r"^LV\s*J\s*(\d{4})([+-])(\d{4})$", re.IGNORECASE),
        replacement="LV J{0}{1}{2}",
    ),
    NameRule(
        name="MCG",
        pattern=re.compile(r"^MCG\s*([+-]?\d+)-0*(\d+)-0*(\d+)$", re.IGNORECASE),
        replacement="MCG {0}-{1}-{2}",
    ),
    NameRule(
        name="KDG",
        pattern=re.compile(r"^KDG\s*0*(\d+)$", re.IGNORECASE),
        replacement="KDG {0}",
    ),
    NameRule(
        name="KKSG",
        pattern=re.compile(r"^KKSG\s*0*(\d+)$", re.IGNORECASE),
        replacement="KKSG {0}",
    ),
    NameRule(
        name="KKS",
        pattern=re.compile(r"^KKS\s*0*(\d+)$", re.IGNORECASE),
        replacement="KKS {0}",
    ),
    NameRule(
        name="KKH",
        pattern=re.compile(r"^KKH\s*0*(\d+)$", re.IGNORECASE),
        replacement="KKH {0}",
    ),
    NameRule(
        name="KKR",
        pattern=re.compile(r"^KKR\s*0*(\d+)$", re.IGNORECASE),
        replacement="KKR {0}",
    ),
    NameRule(
        name="KK",
        pattern=re.compile(r"^KK\s*0*(\d+)$", re.IGNORECASE),
        replacement="KK {0}",
    ),
    NameRule(
        name="BTS",
        pattern=re.compile(r"^BTS\s*0*(\d+)$", re.IGNORECASE),
        replacement="BTS {0}",
    ),
    NameRule(
        name="JKB",
        pattern=re.compile(r"^JKB\s*0*(\d+)$", re.IGNORECASE),
        replacement="JKB {0}",
    ),
]

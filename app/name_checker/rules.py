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
        name="HIPASS",
        pattern=re.compile(r"^HIPASS\s*J\s*(\d{4})([+-])(\d{2})$", re.IGNORECASE),
        replacement="HIPASS J{0}{1}{2}",
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
        name="Dw",
        pattern=re.compile(r"^Dw\s*(\d{4})([+-])(\d{4})$", re.IGNORECASE),
        replacement="Dw {0}{1}{2}",
    ),
]

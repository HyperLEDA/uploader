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
        name="ISI96",
        pattern=re.compile(r"^ISI96_(\d{4})([+-])(\d{4})$", re.IGNORECASE),
        replacement="ISI96_{0}{1}{2}",
    ),
    NameRule(
        name="M",
        pattern=re.compile(r"^(?:MESSIER|M)\s*0*(\d+)$", re.IGNORECASE),
        replacement="M {0}",
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
        name="ESO",
        pattern=re.compile(r"^ESO\s*0*(\d+)-0*(\d+)([a-zA-Z]{1,3})$", re.IGNORECASE),
        replacement="ESO {0}-{1}{2}",
    ),
    NameRule(
        name="ESO",
        pattern=re.compile(r"^ESO\s*0*(\d+)-G\s*0*(\d+)$", re.IGNORECASE),
        replacement="ESO {0}-G{1}",
    ),
    NameRule(
        name="CGCG",
        pattern=re.compile(r"^CGCG\s*(\d{3})-0*(\d{2,3})$", re.IGNORECASE),
        replacement="CGCG {0}-{1}",
    ),
    NameRule(
        name="DR8",
        pattern=re.compile(r"^DR8-(\d{4})([pm])(\d{3})-(\d{1,5})$", re.IGNORECASE),
        replacement="DR8-{0}{1}{2}-{3}",
    ),
    NameRule(
        name="AM",
        pattern=re.compile(r"^AM\s*(\d{4})([+-])(\d{2,3})$", re.IGNORECASE),
        replacement="AM {0}{1}{2}",
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
        name="6dF",
        pattern=re.compile(
            r"^6dFJ(\d{7})([+-])(\d{6})\:?$",
            re.IGNORECASE,
        ),
        replacement="6dF J{0}{1}{2}",
    ),
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^ABELL\s*0*(\d+)$", re.IGNORECASE),
        replacement="ABELL {0}",
        replacer=lambda m: f"ABELL {int(m.group(1))}",
    ),
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^ABELL\s*0*(\d+)_(\d+)$", re.IGNORECASE),
        replacement="ABELL {0}_{1}",
        replacer=lambda m: f"ABELL {int(m.group(1))}_{int(m.group(2))}",
    ),
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^ABELL\s*0*(\d+):\[([^\]]+)\](\d+)$", re.IGNORECASE),
        replacement="ABELL {0} [{1}] {2}",
        replacer=lambda m: f"ABELL {int(m.group(1))} [{m.group(2)}] {m.group(3)}",
    ),
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^ABELL\s*0*(\d+)_(\d+):\[([^\]]+)\](\d+)$", re.IGNORECASE),
        replacement="ABELL {0}_{1} [{2}] {3}",
        replacer=lambda m: f"ABELL {int(m.group(1))}_{int(m.group(2))} [{m.group(3)}] {m.group(4)}",
    ),
    NameRule(
        name="CNOC2",
        pattern=re.compile(r"^CNOC2_(\d+)\.(\d+)$", re.IGNORECASE),
        replacement="CNOC2_{0}.{1}",
    ),
    NameRule(
        name="2MFGC",
        pattern=re.compile(r"^2MFGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="2MFGC {0}",
        replacer=lambda m: f"2MFGC {int(m.group(1))}",
    ),
    NameRule(
        name="VVDS",
        pattern=re.compile(r"^VVDS\s*(\d+)$", re.IGNORECASE),
        replacement="VVDS {0}",
    ),
    NameRule(
        name="GALFA",
        pattern=re.compile(
            r"^GALFAJ(\d+(?:\.\d+)?)\+(\d+(?:\.\d+)?)\+(\d+)$",
            re.IGNORECASE,
        ),
        replacement="GALFA J{0}+{1}+{2}",
    ),
    NameRule(
        name="USGC",
        pattern=re.compile(r"^USGC([A-Za-z]+)(\d+)$", re.IGNORECASE),
        replacement="USGC {0}{1}",
        replacer=lambda m: f"USGC {m.group(1).upper()}{m.group(2)}",
    ),
    NameRule(
        name="RXJ",
        pattern=re.compile(
            r"^RXJ(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2}(?:\.\d+)?)(?::\[([^\]]+)\](\d+))?$",
            re.IGNORECASE,
        ),
        replacement="RX J{0}{1}{2}{3}{4}",
        replacer=lambda m: (
            f"RX J{m.group(1)}{m.group(2)}{m.group(3)}{m.group(4)}{m.group(5)}"
            + (f" [{m.group(6)}] {m.group(7)}" if m.group(6) else "")
        ),
    ),
    NameRule(
        name="CLJ",
        pattern=re.compile(
            r"^CLJ(\d{4})([+-])(\d{4})(?::\[([^\]]+)\](\d+))?$",
            re.IGNORECASE,
        ),
        replacement="CL J{0}{1}{2}",
        replacer=lambda m: (
            f"CL J{m.group(1)}{m.group(2)}{m.group(3)}" + (f" [{m.group(4)}] {m.group(5)}" if m.group(4) else "")
        ),
    ),
    NameRule(
        name="SMMJ",
        pattern=re.compile(
            r"^SMMJ(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="SMM J{0}{1}{2}{3}{4}{5}{6}",
    ),
    NameRule(
        name="NGC",
        pattern=re.compile(
            r"^NGC\s*0*(\d+)([a-zA-Z]{0,3}):\[([^\]]+)\](\d+)$",
            re.IGNORECASE,
        ),
        replacement="NGC {0}{1} [{2}] {3}",
        replacer=lambda m: f"NGC {int(m.group(1))}{m.group(2)} [{m.group(3)}] {m.group(4)}",
    ),
    NameRule(
        name="NGC",
        pattern=re.compile(r"^N\s*0*(\d+)$", re.IGNORECASE),
        replacement="NGC {0}",
    ),
    NameRule(
        name="NGC",
        pattern=re.compile(r"^N\s*0*(\d+)([a-zA-Z]{1,3})$", re.IGNORECASE),
        replacement="NGC {0}{1}",
    ),
    NameRule(
        name="3C",
        pattern=re.compile(r"^3C\s*(\d+(?:\.\d+)?)$", re.IGNORECASE),
        replacement="3C {0}",
    ),
    NameRule(
        name="3C",
        pattern=re.compile(r"^3C\s*(\d+(?:\.\d+)?)([a-zA-Z]{1,3})$", re.IGNORECASE),
        replacement="3C {0}{1}",
    ),
    NameRule(
        name="2dFGRS",
        pattern=re.compile(r"^2dfgrs\s*([NS]\d+Z\d+)$", re.IGNORECASE),
        replacement="2dFGRS {0}",
    ),
    NameRule(
        name="J",
        pattern=re.compile(
            r"^J\s*(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="J{0}{1}{2}{3}{4}{5}{6}",
    ),
    NameRule(
        name="CAT JHHMMSS.sss+DDMMSS.sss",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})J(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: (
            f"{m.group(1).upper()} J{m.group(2)}{m.group(3)}{m.group(4)}"
            f"{m.group(5)}{m.group(6)}{m.group(7)}{m.group(8)}"
        ),
    ),
    NameRule(
        name="CAT JHHMMSSss+DDMMSSs",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,5})J(\d{8})([+-])(\d{7})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} J{m.group(2)}{m.group(3)}{m.group(4)}",
    ),
    NameRule(
        name="CAT JHHMM+DDMM",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,5})J(\d{2})(\d{2})([+-])(\d{2})(\d{2})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} J{m.group(2)}{m.group(3)}{m.group(4)}{m.group(5)}{m.group(6)}",
    ),
    NameRule(
        name="CAT HHMM+DDMM",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,5})\s*(\d{2})(\d{2})([+-])(\d{2})(\d{2})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2)}{m.group(3)}{m.group(4)}{m.group(5)}{m.group(6)}",
    ),
    NameRule(
        name="CAT JDD.ddd+DD.ddd",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})J(\d{1,3}\.\d+)([+-])(\d{1,3}\.\d+)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} J{m.group(2)}{m.group(3)}{m.group(4)}",
    ),
    NameRule(
        name="CAT J HHMM+DD",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})\s*(J?)\s*(\d{4})([+-])(\d{2,3})([a-z])?$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: (
            f"{m.group(1).upper()}{' J' if m.group(2) else ' '}{m.group(3)}{m.group(4)}{m.group(5)}{m.group(6) or ''}"
        ),
    ),
    NameRule(
        name="CAT HHMMSSs+DDMMSS",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})\s*(\d{7})([+-])(\d{6})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2)}{m.group(3)}{m.group(4)}",
    ),
    NameRule(
        name="CAT HHMMSS+DDMMS",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})(\d{6})([+-])(\d{5})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2)}{m.group(3)}{m.group(4)}",
    ),
    NameRule(
        name="CAT HHMMSS.sss+DDMMSS",
        pattern=re.compile(
            r"^([A-Za-z0-9]{2,6})(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: (
            f"{m.group(1).upper()} {m.group(2)}{m.group(3)}{m.group(4)}{m.group(5)}{m.group(6)}{m.group(7)}{m.group(8)}"
        ),
    ),
    NameRule(
        name="CAT N",
        pattern=re.compile(
            r"^([A-Za-z]{2,7})\s*0*(\d{1,10})([a-zA-Z]{1,3})?$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {int(m.group(2))}{m.group(3) or ''}",
    ),
    NameRule(
        name="CAT +HHMMSS",
        pattern=re.compile(
            r"^([A-Za-z]{2,5})\s*([+-])(\d{6})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2)}{m.group(3)}",
    ),
    NameRule(
        name="CAT +DD.d",
        pattern=re.compile(
            r"^((?:[A-Za-z][A-Za-z0-9]{1,4}|[0-9][A-Za-z][A-Za-z0-9]{0,3}|[0-9]{2}[A-Za-z][A-Za-z0-9]{0,2}|[0-9]{3}[A-Za-z][A-Za-z0-9]?))([+-])(\d{2}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2)}{m.group(3)}",
    ),
    NameRule(
        name="[REF]J",
        pattern=re.compile(
            r"^\[([A-Za-z]+\d+)\]\s*J(\d{6}(?:\.\d+)?)([+-])(\d{6}(?:\.\d+)?)$",
            re.IGNORECASE,
        ),
        replacement="[{0}] J{1}{2}{3}",
    ),
    NameRule(
        name="[REF]HHMM+DDMM",
        pattern=re.compile(
            r"^\[([A-Za-z]+\d+)\](\d{4})([+-])(\d{4})$",
            re.IGNORECASE,
        ),
        replacement="[{0}] {1}{2}{3}",
    ),
    NameRule(
        name="[REF]N",
        pattern=re.compile(
            r"^\[([A-Za-z]+\d+)\]0*(\d+)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"[{m.group(1)}] {int(m.group(2))}",
    ),
    NameRule(
        name="CAT N-N-N",
        pattern=re.compile(
            r"^([A-Za-z]{2,6})\s*0*(\d{1,5})-0*(\d{1,5})-0*(\d{1,5})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {int(m.group(2))}-{int(m.group(3))}-{int(m.group(4))}",
    ),
    NameRule(
        name="CGMW",
        pattern=re.compile(
            r"^CGMW([1-5])-0*(\d{5})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"CGMW {m.group(1)}-{int(m.group(2))}",
    ),
    NameRule(
        name="[REF] *",
        pattern=re.compile(
            r"^\[([A-Za-z]+\d+)\]\s*(.+)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"[{m.group(1)}] {m.group(2).strip()}",
    ),
]


def match(name: str) -> tuple[str, str] | None:
    value = name.strip() if name else ""
    if not value:
        return None
    for rule in RULES:
        formatted = rule.match(value)
        if formatted is not None:
            return (formatted, rule.name)
    return None

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
    # Most popular catalogs
    NameRule(
        name="PGC",
        pattern=re.compile(r"^(?:LEDA|PGC|P|#)?\s*0*(\d+)$", re.IGNORECASE),
        replacement="PGC {0}",
    ),
    NameRule(
        name="SDSS",
        pattern=re.compile(r"^SDSS\s*J(\d{6}\.\d{2}[+-]\d{6}\.\d)$", re.IGNORECASE),
        replacement="SDSS J{0}",
    ),
    NameRule(
        name="2MASS",
        pattern=re.compile(r"^(2MAS[SX])\s*J\s*(\d{8}[+-]\d{7})$", re.IGNORECASE),
        replacement="{0} J{1}",
        replacer=lambda m: f"{m.group(1).upper()} J{m.group(2)}",
    ),
    NameRule(
        name="M",
        pattern=re.compile(r"^M(?:ESSIER)?\s*0*(\d+)$", re.IGNORECASE),
        replacement="M {0}",
    ),
    NameRule(
        name="NGC",
        pattern=re.compile(r"^N(?:GC)?\s*0*(\d{1,4})\s*([A-Z]?)$", re.IGNORECASE),
        replacement="NGC {0}{1}",
        replacer=lambda m: f"NGC {int(m.group(1))}{m.group(2).upper()}",
    ),
    NameRule(
        name="IC",
        pattern=re.compile(r"^IC?\s*0*(\d{1,4})\s*([A-Z]?)$", re.IGNORECASE),
        replacement="IC {0}{1}",
        replacer=lambda m: f"IC {int(m.group(1))}{m.group(2).upper()}",
    ),
    NameRule(
        name="UGC",
        pattern=re.compile(r"^U(?:GCG?)?\s*0*(\d{1,5})\s*([A-Z]?)$", re.IGNORECASE),
        replacement="UGC {0}{1}",
        replacer=lambda m: f"UGC {int(m.group(1))}{m.group(2).upper()}",
    ),
    NameRule(
        name="UGCA",
        pattern=re.compile(r"^U(?:GC)?A\s*0*(\d{1,3})$", re.IGNORECASE),
        replacement="UGCA {0}",
    ),
    NameRule(
        name="AGC",
        pattern=re.compile(r"^AGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="AGC {0}",
    ),
    NameRule(
        name="ESO",
        pattern=re.compile(r"^ESO\s*0*(\d+)-\s*G?\s*0*(\d+)([a-z]?)$", re.IGNORECASE),
        replacement="ESO {0}-{1}{2}",
        replacer=lambda m: f"ESO {m.group(1)}-{m.group(2)}{m.group(3).upper() or ''}",
    ),
    NameRule(
        name="MCG",
        pattern=re.compile(r"^MCG\s*([+-]?)(\d{1,2})-(\d{1,2})-(\d+)$", re.IGNORECASE),
        replacement="MCG {0}-{1}-{2}",
        replacer=lambda m: f"MCG {m.group(1) or '+'}{m.group(2).zfill(2)}-{m.group(3).zfill(2)}-{m.group(4).zfill(3)}",
    ),
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^(?:ABELL?|ABGC|ACO)\s*(S?)0*(\d+)$", re.IGNORECASE),
        replacement="ACO {0}",
        replacer=lambda m: f"ACO {'S ' if m.group(1) else ''}{m.group(2)}",
    ),
    # Andromeda
    NameRule(
        name="Andromeda",
        pattern=re.compile(r"^(?:And|Andromeda)\s*(\d+|[IVXLCDM]+)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"And {int(m.group(1)) if m.group(1).isdigit() else _roman_to_int(m.group(1).upper())}",
    ),
    # Eponym
    NameRule(
        name="Eponym",
        pattern=re.compile(r"^Arp\s*0*(\d{1,3})$", re.IGNORECASE),
        replacement="Arp {0}",
    ),
    NameRule(
        name="Eponym",
        pattern=re.compile(r"^(?:Frl|Fair|Fairall)\s*0*(\d{1,4})$", re.IGNORECASE),
        replacement="Frl {0}",
    ),
    NameRule(
        name="Eponym",
        pattern=re.compile(r"^Maffei\s*([12])$", re.IGNORECASE),
        replacement="Maffei {0}",
    ),
    NameRule(
        name="Eponym",
        pattern=re.compile(r"^(?:Mkn|Mrk|Markarian|Markarjan)\s*0*(\d{1,4})$", re.IGNORECASE),
        replacement="Mrk {0}",
    ),
    NameRule(
        name="Eponym",
        pattern=re.compile(
            r"^(?:Ho|Holm|Holmberg)\s*(\d|[IVX]+)$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"Holmberg {int(m.group(1)) if m.group(1).isdigit() else _roman_to_int(m.group(1).upper())}",
    ),
    # LSB galaxies
    NameRule(
        name="LSBG",
        pattern=re.compile(r"^(?:LSBG|\[MDS99\])?\s*F([1-5]\d{2})-(\d{1,3})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"[MDS99] F{m.group(1)}-{m.group(2).zfill(3)}",
    ),
    NameRule(
        name="LSBG",
        pattern=re.compile(r"^(?:LSBG|ISI96|\[ISI96\])\s*(\d{4}[+-]\d{4})([abx]?)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"[ISI96] {m.group(1)}{m.group(2).lower()}",
    ),
    # General rules
    NameRule(
        name="[REF]J",
        pattern=re.compile(
            r"^\[([A-Z]{1,3}(?:[6-9]\d|20\d{2}))\]\s*J(\d{6}(?:\.\d+)?[+-]\d{6}(?:\.\d+)?)$", re.IGNORECASE
        ),
        replacement="[{0}] J{1}",
        replacer=lambda m: f"[{m.group(1).upper()}] J{m.group(2)}",
    ),
    NameRule(
        name="[REF]HHMM+DDMM",
        pattern=re.compile(r"^\[([A-Z]{1,3}(?:[6-9]\d|20\d{2}))\]\s*(\d{4}[+-]\d{4})$", re.IGNORECASE),
        replacement="[{0}] {1}",
        replacer=lambda m: f"[{m.group(1).upper()}] {m.group(2)}",
    ),
    NameRule(
        name="[REF]N",
        pattern=re.compile(r"^\[([A-Z]{1,3}(?:[6-9]\d|20\d{2}))\]\s*0*(\d+)\s*([a-z]?)$", re.IGNORECASE),
        replacement="[{0}] {1}{2}",
        replacer=lambda m: f"[{m.group(1).upper()}] {m.group(2)}{m.group(3).lower()}",
    ),
    # NameRule(
    #     name="[REF] *",
    #     pattern=re.compile(r"^\[([A-Z]{1,3}(?:[6-9]\d|20\d{2}))\]\s*(.+)$", re.IGNORECASE),
    #     replacement="",
    #     replacer=lambda m: f"[{m.group(1)}] {m.group(2).strip()}",
    # ),
    # Mixed characters in an acronym
    NameRule(
        name="6dF",
        pattern=re.compile(r"^6dF\s*J\s*(\d{7}[+-]\d{6})\:?$", re.IGNORECASE),
        replacement="6dF J{0}",
    ),
    NameRule(
        name="USGC",
        pattern=re.compile(r"^USGC\s*([US])\s*(\d+)$", re.IGNORECASE),
        replacement="USGC {0}{1}",
        replacer=lambda m: f"USGC {m.group(1).upper()}{m.group(2)}",
    ),
    # Non standard
    NameRule(
        name="Dw",
        pattern=re.compile(r"^Dw\s*J?(\d{4}[+-]\d{2,4})([ab]?)$", re.IGNORECASE),
        replacement="dwJ{0}{1}",
        replacer=lambda m: f"dwJ{m.group(1)}{m.group(2).lower() or ''}",
    ),
    # General rules
    NameRule(
        name="CAT N",
        pattern=re.compile(r"^([A-Za-z]{2,7})\s*0*(\d+)([a-z]?)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {int(m.group(2))}{m.group(3).lower()}",
    ),
    NameRule(
        name="CAT HHMMSSss+DDMMSSs",
        pattern=re.compile(r"^([a-z0-9]{2,6}?)\s*([JB]?)\s*(\d{8}[+-]\d{7})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}",
    ),
    NameRule(
        name="CAT HHMMSSs+DDMMSS",
        pattern=re.compile(r"^([a-z0-9]{2,6}?)\s*([JB]?)\s*(\d{7}[+-]\d{6})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}",
    ),
    NameRule(
        name="CAT HHMMSS.sss+DDMMSS.sss",
        pattern=re.compile(r"^([a-z0-9]{2,6}?)\s*([JB]?)\s*(\d{6}(?:\.\d+)?[+-]\d{6}(?:\.\d+)?)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}",
    ),
    NameRule(
        name="CAT HHMM+DDMM",
        pattern=re.compile(r"^([a-z0-9]{2,6}?)\s*([JB]?)\s*(\d{4}[+-]\d{4})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}",
    ),
    NameRule(
        name="CAT HHMM+DD",
        pattern=re.compile(r"^([a-z0-9]{2,6})\s*([JB]?)\s*(\d{4}[+-]\d{2,3})([a-z]?)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}{m.group(4) or ''}",
    ),
    NameRule(
        name="CAT DDD.ddd+DD.ddd",
        pattern=re.compile(r"^([a-z0-9]{2,6})\s*J\s*(\d{1,3}\.\d+[+-]\d{1,3}\.\d+)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} J{m.group(2)}",
    ),
    NameRule(
        name="CAT HHMMSS+DDMMS",
        pattern=re.compile(r"^([a-z0-9]{2,6})\s*([JB]?)\s*(\d{6}[+-]\d{5})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {m.group(2).upper()}{m.group(3)}",
    ),
    NameRule(
        name="CAT N-N-N",
        pattern=re.compile(r"^([a-z]{2,6})\s*0*(\d{1,5})-0*(\d{1,5})-0*(\d{1,5})$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"{m.group(1).upper()} {int(m.group(2))}-{int(m.group(3))}-{int(m.group(4))}",
    ),
    # Other catalogs
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
        name="KSP-DW",
        pattern=re.compile(r"^KSP-DW\s*0*(\d+)$", re.IGNORECASE),
        replacement="KSP-DW {0}",
    ),
    NameRule(
        name="LSBC",
        pattern=re.compile(r"^LSBC\s*D\s*0*(\d+)-0*(\d+)$", re.IGNORECASE),
        replacement="LSBC D{0}-{1}",
    ),
    # NameRule(
    #     name="LV",
    #     pattern=re.compile(r"^LV\s*J\s*(\d{4})([+-])(\d{4})$", re.IGNORECASE),
    #     replacement="LV J{0}{1}{2}",
    # ),
    # NameRule(
    #     name="ABELL",
    #     pattern=re.compile(r"^ABELL\s*0*(\d+)_(\d+)$", re.IGNORECASE),
    #     replacement="ABELL {0}_{1}",
    #     replacer=lambda m: f"ABELL {int(m.group(1))}_{int(m.group(2))}",
    # ),
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
    # NameRule(
    #     name="VVDS",
    #     pattern=re.compile(r"^VVDS\s*(\d+)$", re.IGNORECASE),
    #     replacement="VVDS {0}",
    # ),
    NameRule(
        name="GALFA",
        pattern=re.compile(
            r"^GALFAJ(\d+(?:\.\d+)?)\+(\d+(?:\.\d+)?)\+(\d+)$",
            re.IGNORECASE,
        ),
        replacement="GALFA J{0}+{1}+{2}",
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
    # NameRule(
    #     name="CLJ",
    #     pattern=re.compile(
    #         r"^CLJ(\d{4})([+-])(\d{4})(?::\[([^\]]+)\](\d+))?$",
    #         re.IGNORECASE,
    #     ),
    #     replacement="CL J{0}{1}{2}",
    #     replacer=lambda m: (
    #         f"CL J{m.group(1)}{m.group(2)}{m.group(3)}" + (f" [{m.group(4)}] {m.group(5)}" if m.group(4) else "")
    #     ),
    # ),
    # NameRule(
    #     name="SMMJ",
    #     pattern=re.compile(
    #         r"^SMMJ(\d{2})(\d{2})(\d{2}(?:\.\d+)?)([+-])(\d{2})(\d{2})(\d{2}(?:\.\d+)?)$",
    #         re.IGNORECASE,
    #     ),
    #     replacement="SMM J{0}{1}{2}{3}{4}{5}{6}",
    # ),
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
        name="CGMW",
        pattern=re.compile(
            r"^CGMW([1-5])-0*(\d{5})$",
            re.IGNORECASE,
        ),
        replacement="",
        replacer=lambda m: f"CGMW {m.group(1)}-{int(m.group(2))}",
    ),
    # Reformating
    NameRule(
        name="Reformat",
        pattern=re.compile(r"^.*?\[TKA2006\]\s*F\s*0*(\d)-\s*0*(\d{1,2})\s*([ab]?)$", re.IGNORECASE),
        replacement="[TKA2006] F{0}-{1}{2}",
        replacer=lambda m: f"[TKA2006] F{m.group(1)}-{int(m.group(2))}{m.group(3).lower() or ''}",
    ),
    NameRule(
        name="Reformat",
        pattern=re.compile(r"^ABELL.*\[M(?:CF)?2008\]\s*0*(\d+)$", re.IGNORECASE),
        replacement="[MCF2008] {0}",
    ),
    NameRule(
        name="Reformat",
        pattern=re.compile(r"^ABELL.*\[([a-z]{1,3}(?:20\d\d|\d\d))\]\s*0*(\d+)$", re.IGNORECASE),
        replacement="",
        replacer=lambda m: f"[{m.group(1).upper()}] {int(m.group(2))}",
    ),
    # Check CDS for the format
    # [BBG2007]
    # [BMA2005]
    # [BOW83]
    # [D80]
    # [DFL99]
    # [DSP99]
    # [HO98]
    # [JFH99]
    # [NAM2006]
    # [PBL2000]
    # [PL95]
    # [PSE2006]
    # [SBM98]
    # [VMP2002]
    # [YEA96]
    NameRule(
        name="ABELL",
        pattern=re.compile(r"^ABELL\s*0*(\d+)_(\d+):\[([^\]]+)\](\d+)$", re.IGNORECASE),
        replacement="ABELL {0}_{1} [{2}] {3}",
        replacer=lambda m: f"ABELL {int(m.group(1))}_{int(m.group(2))} [{m.group(3)}] {m.group(4)}",
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

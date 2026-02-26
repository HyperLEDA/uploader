import re
from dataclasses import dataclass


@dataclass
class NameRule:
    name: str
    pattern: re.Pattern[str]
    replacement: str

    def match(self, value: str) -> str | None:
        m = self.pattern.match(value.strip())
        if m is None:
            return None
        return self.replacement.format(*m.groups())


RULES: list[NameRule] = [
    NameRule(
        name="NGC",
        pattern=re.compile(r"^NGC\s*0*(\d+)$", re.IGNORECASE),
        replacement="NGC {0}",
    ),
]

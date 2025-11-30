import re
from .config import ROMAN

re_split = re.compile(r",")
re_line = re.compile(
    r"\s*(?P<m1>I|II|III|IV|V|VI|VII|VIII|IX|X)\s*(?:-\s*(?P<m2>I|II|III|IV|V|VI|VII|VIII|IX|X))?\s+(?P<name>.+?)\s*$"
)

def parse_dirasakan(text: str) -> list[tuple[str, int, int]]:
    if not text or text.strip() == "-":
        return []

    output = []
    for part in re_split.split(text):
        part = part.strip()
        if not part:
            continue

        m = re_line.match(part)
        if not m:
            continue

        m1 = ROMAN[m.group("m1")]
        m2_text = m.group("m2")
        m2 = ROMAN[m2_text] if m2_text else m1

        name = m.group("name").strip()
        if name.startswith("-"):
            name = re.sub(r"^-+\s*", "", name)

        output.append((name, m1, m2))
    return output
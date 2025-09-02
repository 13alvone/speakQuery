import re
from pathlib import Path

ALLOWED_PREFIXES = ("[i]", "[!]", "[x]", "[DEBUG]")
LOG_PATTERN = re.compile(r"logging\.(debug|info|warning|error)\(")

PREFIX_PATTERN = re.compile(r"logging\.(debug|info|warning|error)\(\s*f?[\"'](?!\[(i|!|x|DEBUG)\])")

def test_log_messages_have_prefixes():
    root = Path(__file__).resolve().parent.parent
    files = [p for p in root.rglob('*.py')]
    missing = []
    for file in files:
        text = file.read_text()
        for i, line in enumerate(text.splitlines(), 1):
            if PREFIX_PATTERN.search(line):
                missing.append(f"{file.relative_to(root)}:{i}: {line.strip()}")
    assert not missing, "Missing log prefixes:\n" + "\n".join(missing)

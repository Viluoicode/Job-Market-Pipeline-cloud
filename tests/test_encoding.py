"""Every tracked text file is clean UTF-8: no BOM, no mojibake, no characters lost to "?".

Saving through a non-UTF-8 writer (e.g. Windows PowerShell 5.1 redirection) once replaced
Vietnamese letters with "?" and turned em dashes into cp1252 mojibake. This guard keeps CI red
until such a file is fixed.
"""
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BINARY = {".png", ".jpg", ".jpeg", ".gif", ".ico"}
# UTF-8 bytes decoded as cp1252, e.g. U+2014 -> "\u00e2\u20ac\u201d"; plus the replacement char.
MOJIBAKE = re.compile("\u00e2\u20ac|\u00c3[\u0080-\u00bf]|\u00c2[\u0080-\u00bf]|\ufffd")
# A letter, "?", a letter inside prose ("kh?ng", "hi?n") means a character was lost.
LOST_LETTER = re.compile(r"[^\W\d_]\?[^\W\d_]")


def tracked_text_files():
    names = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, check=True,
                           capture_output=True).stdout.decode("utf-8").split("\0")
    return [n for n in names if n and Path(n).suffix.lower() not in BINARY and (ROOT / n).is_file()]


@pytest.mark.parametrize("name", tracked_text_files())
def test_file_is_clean_utf8(name):
    raw = (ROOT / name).read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), "UTF-8 BOM"
    text = raw.decode("utf-8")  # raises if the file is not UTF-8
    bad = [i for i, line in enumerate(text.splitlines(), 1) if MOJIBAKE.search(line)]
    if name.endswith(".md"):
        bad += [i for i, line in enumerate(text.splitlines(), 1)
                if "://" not in line and LOST_LETTER.search(line)]
    assert not bad, f"encoding damage on lines {sorted(set(bad))}"

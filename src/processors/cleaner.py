"""
Content Cleaner - Remove noise and normalize text
"""
import re
from typing import Any


class ContentCleaner:
    NOISE_PATTERNS = [
        (re.compile(r"<script[^>]*>.*?</script>", re.DOTALL), ""),
        (re.compile(r"<style[^>]*>.*?</style>", re.DOTALL), ""),
        (re.compile(r'class="[^"]*"', re.DOTALL), ""),
        (re.compile(r'id="[^"]*"', re.DOTALL), ""),
        (re.compile(r'data-[a-z-]+="[^"]*"', re.DOTALL), ""),
    ]

    def clean(self, text: str, options: dict[str, Any] | None = None) -> str:
        options = options or {}
        for pattern, replacement in self.NOISE_PATTERNS:
            text = pattern.sub(replacement, text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if options.get("remove_short_lines"):
            lines = text.split("\n")
            lines = [line for line in lines if len(line) > 50]
            text = "\n".join(lines)
        import unicodedata
        text = unicodedata.normalize("NFKC", text)
        return text

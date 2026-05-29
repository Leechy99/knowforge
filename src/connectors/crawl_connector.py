"""
Web Crawl Connector - Crawl web pages
"""
import asyncio
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup


class CrawlConnector:
    def __init__(
        self,
        max_depth: int = 2,
        max_concurrent: int = 10,
        timeout: int = 30,
    ):
        self.max_depth = max_depth
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.visited_urls: set[str] = set()
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def crawl_url(
        self,
        url: str,
        selectors: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        async with self.semaphore:
            try:
                async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, "lxml")
                    for tag in soup(["script", "style", "nav", "footer", "header"]):
                        tag.decompose()
                    selectors = selectors or {}
                    title = soup.select_one(selectors.get("title", "title"))
                    main_content = soup.select_one(selectors.get("content", "main, article, .content"))
                    return {
                        "url": url,
                        "status_code": response.status_code,
                        "title": title.get_text(strip=True) if title else "",
                        "content": main_content.get_text(separator="\n", strip=True) if main_content else soup.get_text(separator="\n", strip=True),
                        "links": [a.get("href") for a in soup.find_all("a", href=True) if self._is_valid_link(a["href"])],
                        "metadata": self._extract_metadata(soup),
                    }
            except Exception as e:
                return {
                    "url": url,
                    "error": str(e),
                    "status_code": 0,
                    "title": "",
                    "content": "",
                    "links": [],
                    "metadata": {},
                }

    def _is_valid_link(self, href: str) -> bool:
        if not href or href.startswith("#") or href.startswith("javascript:"):
            return False
        parsed = urlparse(href)
        return bool(parsed.scheme in ("http", "https"))

    def _extract_metadata(self, soup: BeautifulSoup) -> dict[str, str]:
        metadata = {}
        for tag in soup.find_all("meta"):
            name = tag.get("name") or tag.get("property", "")
            content = tag.get("content", "")
            if name and content:
                metadata[name] = content
        return metadata
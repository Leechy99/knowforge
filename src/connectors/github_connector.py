"""
GitHub Connector - Fetch GitHub Trending and Repository Info
"""
import asyncio
import base64
from datetime import datetime, timedelta
from typing import Any

import httpx


class GitHubConnector:
    BASE_URL = "https://api.github.com"

    def __init__(self, access_token: str | None = None):
        self.access_token = access_token
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if access_token:
            self.headers["Authorization"] = f"Bearer {access_token}"

    async def fetch_trending_repos(
        self,
        language: str | None = None,
        since: str = "daily",
    ) -> list[dict[str, Any]]:
        url = f"{self.BASE_URL}/search/repositories"
        params = {
            "q": f"created:>{self._get_date_threshold(since)}",
            "sort": "stars",
            "order": "desc",
            "per_page": 100,
        }
        if language:
            params["q"] += f" language:{language}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            return [
                {
                    "id": str(repo["id"]),
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "description": repo["description"],
                    "url": repo["html_url"],
                    "stars": repo["stargazers_count"],
                    "forks": repo["forks_count"],
                    "language": repo.get("language"),
                    "created_at": repo["created_at"],
                    "topics": repo.get("topics", []),
                    "license": repo.get("license", {}).get("name") if repo.get("license") else None,
                }
                for repo in data.get("items", [])
            ]

    async def fetch_repo_details(self, owner: str, repo: str) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            repo_response = await client.get(
                f"{self.BASE_URL}/repos/{owner}/{repo}",
                headers=self.headers,
            )
            repo_response.raise_for_status()
            repo_data = repo_response.json()
            readme_response = await client.get(
                f"{self.BASE_URL}/repos/{owner}/{repo}/readme",
                headers=self.headers,
            )
            readme = ""
            if readme_response.status_code == 200:
                readme = base64.b64decode(readme_response.json()["content"]).decode("utf-8")
            return {
                "id": str(repo_data["id"]),
                "name": repo_data["name"],
                "full_name": repo_data["full_name"],
                "description": repo_data["description"],
                "url": repo_data["html_url"],
                "stars": repo_data["stargazers_count"],
                "forks": repo_data["forks_count"],
                "language": repo_data.get("language"),
                "topics": repo_data.get("topics", []),
                "license": repo_data.get("license", {}).get("name") if repo_data.get("license") else None,
                "readme": readme,
                "created_at": repo_data["created_at"],
            }

    def _get_date_threshold(self, since: str) -> str:
        days = {"daily": 1, "weekly": 7, "monthly": 30}.get(since, 1)
        threshold = datetime.utcnow() - timedelta(days=days)
        return threshold.strftime("%Y-%m-%d")
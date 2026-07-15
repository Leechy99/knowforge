"""
Content Deduplicator - Detect and remove duplicate content
"""
import hashlib


class Deduplicator:
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        self.seen_hashes: set[str] = set()
        self.seen_simples: dict[str, str] = {}

    def compute_hash(self, content: str) -> str:
        return hashlib.md5(content.encode()).hexdigest()

    def compute_simhash(self, content: str) -> str:
        try:
            from simhash import Simhash

            return str(Simhash(content).value)
        except ModuleNotFoundError:
            return str(int(self.compute_hash(content)[:16], 16))

    def is_duplicate(self, content: str) -> tuple[bool, str | None]:
        content_hash = self.compute_hash(content)
        if content_hash in self.seen_hashes:
            return True, content_hash
        simhash = self.compute_simhash(content)
        try:
            from simhash import Simhash

            for stored_simhash, stored_hash in self.seen_simples.items():
                distance = Simhash(content).distance(Simhash(int(stored_simhash)))
                if distance / 64 < (1 - self.similarity_threshold):
                    return True, stored_hash
        except ModuleNotFoundError:
            pass
        self.seen_simples[simhash] = content_hash
        self.seen_hashes.add(content_hash)
        return False, None

    def reset(self) -> None:
        self.seen_hashes.clear()
        self.seen_simples.clear()

"""
File Watcher Connector - Monitor folder for new files
"""
import asyncio
from pathlib import Path
from typing import Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from src.utils.file_utils import normalize_extension


class FileWatcherConnector:
    def __init__(
        self,
        watch_path: str,
        supported_extensions: list[str] = None,
        recursive: bool = True,
    ):
        self.watch_path = Path(watch_path)
        self.supported_extensions = supported_extensions or [
            "pdf", "docx", "md", "markdown", "html", "htm", "txt", "json"
        ]
        self.recursive = recursive
        self.observer: Observer | None = None
        self.event_queue: asyncio.Queue = asyncio.Queue()

    def start(self):
        event_handler = FileWatcherHandler(
            supported_extensions=self.supported_extensions,
            event_queue=self.event_queue,
        )
        self.observer = Observer()
        self.observer.schedule(event_handler, str(self.watch_path), recursive=self.recursive)
        self.observer.start()

    def stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()

    async def get_next_event(self) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None


class FileWatcherHandler(FileSystemEventHandler):
    def __init__(self, supported_extensions: list[str], event_queue: asyncio.Queue):
        self.supported_extensions = supported_extensions
        self.event_queue = event_queue

    def on_created(self, event: FileSystemEvent):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        ext = normalize_extension(file_path.suffix)
        if ext in self.supported_extensions:
            try:
                size = file_path.stat().st_size
            except OSError:
                size = 0
            asyncio.create_task(
                self.event_queue.put({
                    "event_type": "file_created",
                    "path": str(file_path),
                    "extension": ext,
                    "size": size,
                })
            )

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            self.on_created(event)
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from models import PendingQuoteSegment
from quote_service import QuoteService
from sqlite_store import QuoteRepository


class FakeEvent:
    def get_group_id(self) -> str:
        return "123456"

    def get_sender_id(self) -> str:
        return "20002"

    def get_messages(self) -> list[object]:
        return []


class FakeImageService:
    async def build_reply_segments(self, event: object, message: object) -> list[PendingQuoteSegment]:
        return [PendingQuoteSegment(type="text", text="后台渲染测试")]

    async def build_current_segments(self, event: object, **kwargs: object) -> list[PendingQuoteSegment]:
        return []


class FakeNapcatService:
    async def fetch_onebot_message(self, event: object, message_id: str | None) -> dict[str, object]:
        return {"sender": {"user_id": "10001"}, "message": []}

    def extract_forward_reference(self, message: object) -> tuple[None, None]:
        return None, None

    async def resolve_user_name(self, event: object, qq: str) -> str:
        return "测试用户"

    async def resolve_signature_name(self, event: object, quote: object, use_group_signature: bool) -> str:
        return "测试用户"


class BlockingRenderer:
    def __init__(self, source: Path):
        self.source = source
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def should_fallback_to_plain(self, quote: object) -> bool:
        return False

    async def render_quote_image(self, quote: object, signature: str) -> str:
        self.started.set()
        await self.release.wait()
        return str(self.source)


class PreRenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_returns_before_background_render_finishes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            renderer = BlockingRenderer(source)
            service = QuoteService(
                repository=repository,
                image_service=FakeImageService(),
                napcat_service=FakeNapcatService(),
                renderer=renderer,
                http_client=None,
                global_mode=False,
                text_mode=False,
                render_cache=True,
                image_signature_use_group=False,
                blacklist=set(),
                render_wait_timeout=0.01,
            )

            response = await service.add_quote(FakeEvent())
            self.assertEqual(response.kind, "plain")
            self.assertTrue(service._render_tasks)
            await asyncio.wait_for(renderer.started.wait(), timeout=1.0)
            self.assertTrue(any(not task.done() for task in service._render_tasks))

            cold_response = await service.random_quote(FakeEvent())
            self.assertEqual(cold_response.kind, "plain")
            self.assertIn("后台渲染测试", cold_response.text)

            renderer.release.set()
            await asyncio.gather(*list(service._render_tasks))
            quotes = repository.list_quotes("123456")
            self.assertEqual(len(quotes), 1)
            cache_path = repository.get_store("123456").cache_path(quotes[0].id)
            self.assertEqual(cache_path.read_bytes(), b"fake-png")
            warm_response = await service.random_quote(FakeEvent())
            self.assertEqual(warm_response.kind, "image_path")
            self.assertEqual(Path(warm_response.path), cache_path)
            await service.shutdown()


if __name__ == "__main__":
    unittest.main()

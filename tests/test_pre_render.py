from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

import quote_service as quote_service_module
from models import PendingQuoteSegment, Quote
from quote_service import QuoteService
from sqlite_store import QuoteRepository


class FakeEvent:
    def get_group_id(self) -> str:
        return "123456"

    def get_sender_id(self) -> str:
        return "20002"

    def get_messages(self) -> list[object]:
        return []


class SegmentEvent:
    def __init__(self, segments: list[object]):
        self.segments = segments

    def get_messages(self) -> list[object]:
        return self.segments


class FakePlain:
    def __init__(self, text: str):
        self.text = text


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


class CapturingRenderer:
    def __init__(self, source: Path):
        self.source = source
        self.signatures: list[str] = []

    def should_fallback_to_plain(self, quote: object) -> bool:
        return False

    async def render_quote_image(self, quote: object, signature: str) -> str:
        self.signatures.append(signature)
        return str(self.source)


class PreRenderTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_tag_requires_plain_only_message(self) -> None:
        original_components = quote_service_module.Comp
        quote_service_module.Comp = type("Components", (), {"Plain": FakePlain})
        service = object.__new__(QuoteService)
        try:
            self.assertEqual(
                service.extract_exact_plain_text(
                    SegmentEvent([FakePlain("  Ta"), FakePlain("g  ")])
                ),
                "Tag",
            )
            self.assertIsNone(
                service.extract_exact_plain_text(
                    SegmentEvent([FakePlain("Tag"), object()])
                )
            )
            self.assertIsNone(service.extract_exact_plain_text(SegmentEvent([])))
        finally:
            quote_service_module.Comp = original_components

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

    async def test_upload_pre_renders_all_global_binding_signatures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            await repository.create_binding("123456", "10001", "local-tag")
            await repository.create_binding("654321", "10001", "remote-tag")
            renderer = CapturingRenderer(source)
            service = QuoteService(
                repository=repository,
                image_service=FakeImageService(),
                napcat_service=FakeNapcatService(),
                renderer=renderer,
                http_client=None,
                global_mode=True,
                text_mode=False,
                render_cache=True,
                image_signature_use_group=False,
                blacklist=set(),
                render_wait_timeout=1.0,
            )

            response = await service.add_quote(FakeEvent())
            self.assertEqual(response.kind, "plain")
            await asyncio.gather(*list(service._render_tasks))
            self.assertEqual(
                set(renderer.signatures),
                {"测试用户", "local-tag", "remote-tag"},
            )
            quote = repository.list_quotes("123456")[0]
            store = repository.get_store("123456")
            self.assertTrue(store.cache_path(quote.id).exists())
            self.assertTrue(store.cache_path(quote.id, "local-tag").exists())
            self.assertTrue(store.cache_path(quote.id, "remote-tag").exists())
            await service.shutdown()

    async def test_signature_override_uses_separate_cache_variant(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            quote = Quote(
                id="q_signature",
                qq="10001",
                name="测试用户",
                text="签名测试",
                created_by="20002",
                created_at=10.0,
                group="123456",
                content_fingerprint="signature-fingerprint",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text="签名测试")],
            )
            renderer = CapturingRenderer(source)
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
                render_wait_timeout=1.0,
            )

            tag_response = await service.random_quote(
                FakeEvent(),
                uid="10001",
                signature_override="tag",
            )
            tag_cache = repository.get_store("123456").cache_path("q_signature", "tag")
            self.assertEqual(tag_response.kind, "image_path")
            self.assertEqual(Path(tag_response.path), tag_cache)
            self.assertEqual(renderer.signatures, ["tag"])

            cached_tag_response = await service.random_quote(
                FakeEvent(),
                uid="10001",
                signature_override="tag",
            )
            self.assertEqual(cached_tag_response.kind, "image_path")
            self.assertEqual(renderer.signatures, ["tag"])

            default_response = await service.random_quote(FakeEvent(), uid="10001")
            default_cache = repository.get_store("123456").cache_path("q_signature")
            self.assertEqual(default_response.kind, "image_path")
            self.assertEqual(Path(default_response.path), default_cache)
            self.assertEqual(renderer.signatures, ["tag", "测试用户"])

            self.assertTrue(await service.delete_quote("q_signature"))
            self.assertFalse(tag_cache.exists())
            self.assertFalse(default_cache.exists())
            await service.shutdown()


if __name__ == "__main__":
    unittest.main()

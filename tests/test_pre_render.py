from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import quote_service as quote_service_module
from models import PendingQuoteSegment, Quote, QuoteSegment
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


class FakeImage:
    def __init__(self, file: str):
        self.file = file

    @staticmethod
    def fromURL(url: str) -> "FakeImage":
        return FakeImage(url)

    @staticmethod
    def fromFileSystem(path: str) -> "FakeImage":
        return FakeImage(str(Path(path).resolve()))


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

    def test_plain_quote_response_includes_sender_avatar(self) -> None:
        original_components = quote_service_module.Comp
        quote_service_module.Comp = type(
            "Components",
            (),
            {"Plain": FakePlain, "Image": FakeImage},
        )
        service = object.__new__(QuoteService)
        quote = Quote(
            id="q_text_avatar",
            qq="10001",
            name="原昵称",
            text="纯文字语录",
            created_by="20002",
            created_at=1.0,
            group="123456",
        )
        try:
            response = service._plain_quote_response(quote, "名言")

            self.assertEqual(response.kind, "chain")
            self.assertEqual(len(response.chain), 2)
            self.assertEqual(
                response.chain[0].file,
                "https://q1.qlogo.cn/g?b=qq&nk=10001&s=100",
            )
            self.assertEqual(response.chain[1].text, "「纯文字语录」 — 名言")
        finally:
            quote_service_module.Comp = original_components

    async def test_image_quote_chain_appends_bound_sender_id(self) -> None:
        original_components = quote_service_module.Comp
        quote_service_module.Comp = type(
            "Components",
            (),
            {"Plain": FakePlain, "Image": FakeImage},
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            image_path = root / "quote.png"
            image_path.write_bytes(b"fake-png")
            service = object.__new__(QuoteService)
            service.repository = SimpleNamespace(
                root=root,
                find_assets=lambda group, image_ids: {
                    "asset-1": SimpleNamespace(rel_path="quote.png")
                },
            )
            quote = Quote(
                id="q_image_sender",
                qq="10001",
                name="原昵称",
                text="",
                created_by="20002",
                created_at=1.0,
                group="123456",
                image_ids=["asset-1"],
                segments=[QuoteSegment(type="image", asset_id="asset-1")],
            )
            try:
                chain = service.build_standard_quote_chain(quote, "名言")
                service.http_client = None

                self.assertEqual(len(chain), 2)
                self.assertIsInstance(chain[0], FakeImage)
                self.assertEqual(chain[1].text, "\n— 名言")
                self.assertEqual(
                    await service.build_delete_fingerprint(quote, chain=chain),
                    await service._fingerprint_standard_chain(chain),
                )

                unbound_chain = service.build_standard_quote_chain(quote)
                self.assertEqual(unbound_chain[-1].text, "\n— 10001")
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
            self.assertIn("我学会啦，来问问我吧！", response.text)
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

    async def test_missing_owner_quote_uses_memory_feedback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repository = QuoteRepository(root)
            service = QuoteService(
                repository=repository,
                image_service=FakeImageService(),
                napcat_service=FakeNapcatService(),
                renderer=CapturingRenderer(root / "unused.png"),
                http_client=None,
                global_mode=False,
                text_mode=False,
                render_cache=True,
                image_signature_use_group=False,
                blacklist=set(),
            )

            response = await service.random_quote(
                FakeEvent(),
                uid="10001",
                silent_if_empty=False,
            )
            self.assertEqual(response.kind, "plain")
            self.assertEqual(
                response.text,
                "这个会话的记忆库里还没有这位的语录。再教我一点吧！",
            )
            self.assertIsNone(
                await service.random_quote(
                    FakeEvent(),
                    uid="10001",
                    silent_if_empty=True,
                )
            )
            await service.shutdown()

    async def test_quote_list_is_paginated_and_summarized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repository = QuoteRepository(root)
            for index in range(11):
                quote = Quote(
                    id=f"q_list_{index:02d}",
                    qq="10001",
                    name="测试用户",
                    text=f"第 {index} 条语录",
                    created_by="20002",
                    created_at=float(index),
                    group="123456",
                    content_fingerprint=f"list-fingerprint-{index}",
                )
                await repository.create_quote_with_segments(
                    "123456",
                    quote,
                    [PendingQuoteSegment(type="text", text=quote.text)],
                )
            service = QuoteService(
                repository=repository,
                image_service=FakeImageService(),
                napcat_service=FakeNapcatService(),
                renderer=CapturingRenderer(root / "unused.png"),
                http_client=None,
                global_mode=True,
                text_mode=False,
                render_cache=True,
                image_signature_use_group=False,
                blacklist=set(),
            )

            first_page = await service.build_quote_list_text("123456")
            second_page = await service.build_quote_list_text("123456", page=2)

            self.assertIn("第 1/2 页，共 11 条", first_page)
            self.assertIn("1. 测试用户（10001）：第 10 条语录", first_page)
            self.assertIn("发送 /语录列表 2 查看下一页", first_page)
            self.assertIn("第 2/2 页，共 11 条", second_page)
            self.assertIn("11. 测试用户（10001）：第 0 条语录", second_page)
            self.assertEqual(
                await service.build_quote_list_text("123456", page=3),
                "页码超出范围：当前共有 2 页、11 条语录。",
            )
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

    async def test_regular_random_quote_uses_bound_tag_for_cache_and_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            await repository.create_binding("123456", "10001", "名言")
            quote = Quote(
                id="q_bound_random",
                qq="10001",
                name="原昵称",
                text="绑定签名测试",
                created_by="20002",
                created_at=10.0,
                group="123456",
                content_fingerprint="bound-random-fingerprint",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )
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

            response = await service.random_quote(FakeEvent())
            self.assertEqual(response.kind, "plain")
            self.assertEqual(response.text, "「绑定签名测试」 — 名言")
            renderer.release.set()
            await asyncio.gather(*list(service._render_tasks))
            tag_cache = repository.get_store("123456").cache_path("q_bound_random", "名言")
            self.assertTrue(tag_cache.exists())

            cached_response = await service.random_quote(FakeEvent())
            self.assertEqual(cached_response.kind, "image_path")
            self.assertEqual(Path(cached_response.path), tag_cache)
            await service.shutdown()

    async def test_startup_pre_render_backfills_existing_default_and_tag_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            await repository.create_binding("123456", "10001", "名言")
            await repository.create_binding("654321", "10001", "其他群标签")
            quote = Quote(
                id="q_startup",
                qq="10001",
                name="原昵称",
                text="启动预渲染测试",
                created_by="20002",
                created_at=10.0,
                group="123456",
                content_fingerprint="startup-render-fingerprint",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
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
            )

            service.schedule_startup_pre_render()
            startup_task = service._startup_pre_render_task
            self.assertIsNotNone(startup_task)
            await startup_task

            store = repository.get_store("123456")
            self.assertFalse(store.cache_path("q_startup").exists())
            self.assertTrue(store.cache_path("q_startup", "名言").exists())
            self.assertFalse(store.cache_path("q_startup", "其他群标签").exists())
            self.assertEqual(renderer.signatures, ["名言"])
            await service.shutdown()

    async def test_clear_render_cache_is_session_scoped_and_cancels_rendering(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "render-source.png"
            source.write_bytes(b"fake-png")
            repository = QuoteRepository(root)
            quote = Quote(
                id="q_clear_cache",
                qq="10001",
                name="测试用户",
                text="缓存清理测试",
                created_by="20002",
                created_at=10.0,
                group="123456",
                content_fingerprint="clear-cache-fingerprint",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text="缓存清理测试")],
            )
            current_store = repository.get_store("123456")
            current_store.cache_path(quote.id).write_bytes(b"cached")
            (current_store.cache_dir / "orphan.png.tmp").write_bytes(b"temporary")
            other_store = repository.get_store("654321")
            other_cache = other_store.cache_path("q_other")
            other_cache.write_bytes(b"keep")

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
            )
            service.schedule_pre_render(FakeEvent(), quote, signature_override="inflight")
            await asyncio.wait_for(renderer.started.wait(), timeout=1.0)

            removed, failed = await service.clear_render_cache("123456")

            self.assertEqual((removed, failed), (2, 0))
            self.assertEqual(list(current_store.cache_dir.iterdir()), [])
            self.assertTrue(other_cache.exists())
            self.assertFalse(service._render_tasks)
            await service.shutdown()


if __name__ == "__main__":
    unittest.main()

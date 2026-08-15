from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image as PillowImage

from constants import DATABASE_FILENAME, DATABASE_SCHEMA_VERSION
from models import (
    ImageSignature,
    PendingForwardNode,
    PendingForwardSegment,
    PendingQuoteSegment,
    PreparedMedia,
    Quote,
)
from sqlite_store import QuoteRepository
from utils import prepare_image


class SQLiteQuoteRepositoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    async def test_crud_random_and_sent_index(self) -> None:
        repository = QuoteRepository(self.root)
        quote = Quote(
            id="q_new",
            qq="10001",
            name="测试用户",
            text="测试语录",
            created_by="20002",
            created_at=100.0,
            group="123456",
            content_fingerprint="fingerprint-1",
        )
        result = await repository.create_quote_with_segments(
            "123456",
            quote,
            [PendingQuoteSegment(type="text", text="测试语录")],
        )

        self.assertIsNotNone(result.quote)
        self.assertTrue((self.root / DATABASE_FILENAME).exists())
        self.assertTrue(repository.has_content_fingerprint("123456", "10001", "fingerprint-1"))
        selected = await repository.random_quote("123456", qq="10001")
        self.assertIsNotNone(selected)
        self.assertEqual(selected.id, "q_new")

        await repository.record_sent_quote(
            "123456",
            quote_id="q_new",
            fingerprint="sent-fingerprint",
            sent_at=200.0,
            image_signatures=[ImageSignature(sha256="abc")],
        )
        self.assertEqual(
            repository.find_sent_quote_id("123456", fingerprint="sent-fingerprint"),
            "q_new",
        )
        self.assertTrue(await repository.delete_quote("q_new"))
        self.assertIsNone(await repository.random_quote("123456"))
        connection = sqlite3.connect(self.root / DATABASE_FILENAME)
        try:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                DATABASE_SCHEMA_VERSION,
            )
        finally:
            connection.close()

    async def test_lists_quotes_by_newest_page(self) -> None:
        repository = QuoteRepository(self.root)
        for index in range(12):
            quote = Quote(
                id=f"q_page_{index:02d}",
                qq="10001",
                name="测试用户",
                text=f"第 {index} 条",
                created_by="20002",
                created_at=float(index),
                group="123456",
                content_fingerprint=f"page-fingerprint-{index}",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )

        total, quotes = repository.list_quotes_page(
            "123456",
            limit=5,
            offset=5,
        )
        self.assertEqual(total, 12)
        self.assertEqual(
            [quote.id for quote in quotes],
            ["q_page_06", "q_page_05", "q_page_04", "q_page_03", "q_page_02"],
        )

    async def test_random_quote_strictly_avoids_consecutive_repeats(self) -> None:
        repository = QuoteRepository(self.root)
        for index in range(3):
            quote = Quote(
                id=f"q_random_{index}",
                qq="10001",
                name="测试用户",
                text=f"随机语录 {index}",
                created_by="20002",
                created_at=float(index),
                group="123456",
                content_fingerprint=f"random-fingerprint-{index}",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )

        selected_ids = []
        for _ in range(30):
            selected = await repository.random_quote(
                "123456",
                history_session_key="123456",
            )
            self.assertIsNotNone(selected)
            selected_ids.append(selected.id)

        self.assertTrue(
            all(previous != current for previous, current in zip(selected_ids, selected_ids[1:]))
        )

    async def test_random_quote_state_persists_and_is_shared_across_filters(self) -> None:
        repository = QuoteRepository(self.root)
        for quote_id, qq in (("q_owner_1", "10001"), ("q_owner_2", "10002")):
            quote = Quote(
                id=quote_id,
                qq=qq,
                name=qq,
                text=quote_id,
                created_by="20002",
                created_at=1.0,
                group="123456",
                content_fingerprint=f"fingerprint-{quote_id}",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )

        only_owner = await repository.random_quote(
            "123456",
            qq="10001",
            history_session_key="123456",
        )
        self.assertEqual(only_owner.id, "q_owner_1")

        restarted = QuoteRepository(self.root)
        unfiltered = await restarted.random_quote(
            "123456",
            history_session_key="123456",
        )
        self.assertEqual(unfiltered.id, "q_owner_2")

        single_candidate = await restarted.random_quote(
            "123456",
            qq="10002",
            history_session_key="123456",
        )
        self.assertEqual(single_candidate.id, "q_owner_2")

    async def test_concurrent_random_quotes_do_not_repeat(self) -> None:
        repository = QuoteRepository(self.root)
        for index in range(2):
            quote = Quote(
                id=f"q_concurrent_{index}",
                qq="10001",
                name="测试用户",
                text=f"并发语录 {index}",
                created_by="20002",
                created_at=float(index),
                group="123456",
                content_fingerprint=f"concurrent-fingerprint-{index}",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )

        selected = await asyncio.gather(
            *[
                repository.random_quote(
                    "123456",
                    history_session_key="123456",
                )
                for _ in range(20)
            ]
        )
        selected_ids = [quote.id for quote in selected]
        self.assertTrue(
            all(previous != current for previous, current in zip(selected_ids, selected_ids[1:]))
        )

        second_repository = QuoteRepository(self.root)
        for index in range(10):
            first, second = await asyncio.gather(
                repository.random_quote(
                    "123456",
                    history_session_key=f"concurrent-session-{index}",
                ),
                second_repository.random_quote(
                    "123456",
                    history_session_key=f"concurrent-session-{index}",
                ),
            )
            self.assertNotEqual(first.id, second.id)

    async def test_migrates_session_json_and_keeps_backup(self) -> None:
        session_dir = self.root / "groups" / "778899"
        session_dir.mkdir(parents=True)
        images_dir = session_dir / "images"
        images_dir.mkdir()
        (images_dir / "old.png").write_bytes(b"old-image")
        (session_dir / "quotes.json").write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "session_key": "778899",
                    "quotes": [
                        {
                            "id": "q_json",
                            "qq": "10001",
                            "name": "旧用户",
                            "text": "旧语录",
                            "created_by": "20002",
                            "created_at": 10.0,
                            "group": "778899",
                            "kind": "standard",
                            "image_ids": ["img_old"],
                            "media_ids": [],
                            "segments": [
                                {"type": "text", "text": "旧语录", "asset_id": ""},
                                {"type": "image", "text": "", "asset_id": "img_old"}
                            ],
                            "forward_nodes": [],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (session_dir / "image_index.json").write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "session_key": "778899",
                    "images": [
                        {
                            "asset_id": "img_old",
                            "file_name": "old.png",
                            "rel_path": "groups/778899/images/old.png",
                            "sha256": "old-sha",
                            "dhash": "0000000000000000",
                            "width": 100,
                            "height": 100,
                            "ref_count": 1,
                            "created_at": 10.0,
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (session_dir / "sent_index.json").write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "session_key": "778899",
                    "sent": [
                        {
                            "quote_id": "q_json",
                            "fingerprint": "old-sent",
                            "sent_at": 20.0,
                            "image_signatures": [],
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        repository = QuoteRepository(self.root)
        self.assertTrue(await repository.migrate_legacy_data())
        migrated = repository.get_quote("778899", "q_json")
        self.assertIsNotNone(migrated)
        self.assertEqual(migrated.text, "旧语录")
        self.assertEqual(repository.find_asset("778899", "img_old").sha256, "old-sha")
        self.assertEqual(
            repository.find_sent_quote_id("778899", fingerprint="old-sent"),
            "q_json",
        )
        self.assertTrue((session_dir / "quotes.json.migrated.bak").exists())
        self.assertTrue((session_dir / "image_index.json.migrated.bak").exists())
        self.assertTrue((session_dir / "sent_index.json.migrated.bak").exists())
        self.assertFalse(await repository.migrate_legacy_data())
        self.assertEqual(len(repository.list_quotes("778899")), 1)

    async def test_image_and_forward_media_lifecycle(self) -> None:
        repository = QuoteRepository(self.root)
        source = BytesIO()
        PillowImage.new("RGBA", (100, 80), (10, 20, 30, 128)).save(
            source,
            format="PNG",
        )
        prepared_image = prepare_image(source.getvalue(), source="upload.png")
        image_quote = Quote(
            id="q_image",
            qq="10001",
            name="图片用户",
            text="",
            created_by="20002",
            created_at=30.0,
            group="123456",
            content_fingerprint="image-fingerprint",
        )
        image_result = await repository.create_quote_with_segments(
            "123456",
            image_quote,
            [PendingQuoteSegment(type="image", image=prepared_image)],
        )
        self.assertIsNotNone(image_result.quote)
        image_asset = repository.find_asset("123456", image_quote.image_ids[0])
        self.assertIsNotNone(image_asset)
        image_path = self.root / image_asset.rel_path
        self.assertTrue(image_path.exists())
        self.assertEqual(image_asset.file_name, f"{image_asset.asset_id}.jpg")
        self.assertEqual(image_path.read_bytes(), prepared_image.content)

        duplicate_result = await repository.create_quote_with_segments(
            "123456",
            Quote(
                id="q_image_duplicate",
                qq="10002",
                name="重复图片用户",
                text="",
                created_by="20002",
                created_at=31.0,
                group="123456",
                content_fingerprint="another-fingerprint",
            ),
            [PendingQuoteSegment(type="image", image=prepared_image)],
        )
        self.assertFalse(duplicate_result.duplicate)
        self.assertEqual(image_quote.image_ids, duplicate_result.quote.image_ids)
        self.assertEqual(
            repository.find_asset("123456", image_quote.image_ids[0]).ref_count,
            2,
        )

        forward_image_quote = Quote(
            id="q_forward_image",
            qq="10004",
            name="聊天记录图片用户",
            text="",
            created_by="20002",
            created_at=32.0,
            group="123456",
            kind="forward",
            content_fingerprint="forward-image-fingerprint",
        )
        forward_image_result = await repository.create_quote_with_forward_nodes(
            "123456",
            forward_image_quote,
            [
                PendingForwardNode(
                    sender_uin="10004",
                    sender_name="聊天记录图片用户",
                    segments=[PendingForwardSegment(type="image", image=prepared_image)],
                )
            ],
        )
        self.assertIsNotNone(forward_image_result.quote)
        self.assertEqual(forward_image_quote.image_ids, image_quote.image_ids)
        self.assertEqual(
            repository.find_asset("123456", image_quote.image_ids[0]).ref_count,
            3,
        )

        media = PreparedMedia(
            content=b"audio-content",
            extension=".wav",
            media_type="record",
            display_name="record.wav",
        )
        forward_quote = Quote(
            id="q_forward",
            qq="10003",
            name="转发用户",
            text="",
            created_by="20002",
            created_at=40.0,
            group="654321",
            kind="forward",
            content_fingerprint="forward-fingerprint",
        )
        forward_result = await repository.create_quote_with_forward_nodes(
            "654321",
            forward_quote,
            [
                PendingForwardNode(
                    sender_uin="10003",
                    sender_name="转发用户",
                    segments=[PendingForwardSegment(type="record", media=media)],
                )
            ],
        )
        self.assertIsNotNone(forward_result.quote)
        loaded_forward = repository.get_quote("654321", "q_forward")
        self.assertEqual(loaded_forward.kind, "forward")
        media_asset = repository.find_media_asset("654321", loaded_forward.media_ids[0])
        self.assertIsNotNone(media_asset)
        media_path = self.root / media_asset.rel_path
        self.assertTrue(media_path.exists())

        self.assertTrue(await repository.delete_quote("q_image"))
        self.assertTrue(image_path.exists())
        self.assertTrue(await repository.delete_quote("q_image_duplicate"))
        self.assertTrue(image_path.exists())
        self.assertTrue(await repository.delete_quote("q_forward_image"))
        self.assertFalse(image_path.exists())
        self.assertTrue(await repository.delete_quote("q_forward"))
        self.assertFalse(media_path.exists())

    async def test_binding_lifecycle_and_session_uniqueness(self) -> None:
        repository = QuoteRepository(self.root)

        self.assertEqual(
            await repository.create_binding("123456", "10001", "tag"),
            ("created", "tag"),
        )
        self.assertEqual(
            await repository.create_binding("123456", "10001", "tag"),
            ("unchanged", "tag"),
        )
        self.assertEqual(
            await repository.create_binding("123456", "10001", "tag2"),
            ("qq_exists", "tag"),
        )
        self.assertEqual(
            await repository.create_binding("123456", "10002", "tag"),
            ("tag_exists", "10001"),
        )
        self.assertEqual(
            await repository.create_binding("123456", "10002", "other"),
            ("created", "other"),
        )
        self.assertEqual(
            await repository.create_binding("654321", "10003", "tag"),
            ("created", "tag"),
        )

        self.assertEqual(repository.get_binding_by_tag("123456", "tag").qq, "10001")
        self.assertEqual(repository.get_binding_for_qq("123456", "10002").tag, "other")
        self.assertEqual([item.tag for item in repository.list_bindings("123456")], ["other", "tag"])
        self.assertEqual(
            [item.session_key for item in repository.list_bindings_for_qq_global("10001")],
            ["123456"],
        )

        self.assertEqual(
            await repository.rebind("123456", "10001", "other"),
            ("tag_exists", "10002"),
        )
        self.assertEqual(
            await repository.rebind("123456", "10001", "tag2"),
            ("updated", "tag"),
        )
        self.assertIsNone(repository.get_binding_by_tag("123456", "tag"))
        self.assertEqual(repository.get_binding_by_tag("123456", "tag2").qq, "10001")
        self.assertEqual(
            await repository.rebind("123456", "10001"),
            ("removed", "tag2"),
        )
        self.assertIsNone(repository.get_binding_for_qq("123456", "10001"))
        self.assertEqual(
            await repository.rebind("123456", "10001", "new"),
            ("not_found", ""),
        )

    async def test_gallery_reuses_assets_and_requires_exact_keyword(self) -> None:
        repository = QuoteRepository(self.root)
        source = BytesIO()
        PillowImage.new("RGB", (48, 32), (20, 40, 60)).save(source, format="PNG")
        prepared_image = prepare_image(source.getvalue(), source="gallery.png")

        self.assertEqual(
            await repository.add_gallery_images("123456", "猫", [prepared_image]),
            (1, 0),
        )
        self.assertEqual(
            await repository.add_gallery_images("123456", "猫", [prepared_image]),
            (0, 1),
        )
        self.assertEqual(
            await repository.add_gallery_images("123456", "猫猫", [prepared_image]),
            (1, 0),
        )
        self.assertEqual(
            await repository.create_binding("123456", "10001", "猫"),
            ("gallery_exists", "猫"),
        )

        selected = await repository.random_gallery_image("123456", "  猫猫  ")
        self.assertIsNotNone(selected)
        keyword, asset = selected
        self.assertEqual(keyword, "猫猫")
        self.assertEqual(asset.file_name, f"{asset.asset_id}.jpg")
        self.assertTrue((self.root / asset.rel_path).exists())
        self.assertEqual(asset.ref_count, 2)
        self.assertEqual(len(repository.find_assets("123456", [asset.asset_id])), 1)
        self.assertIsNone(await repository.random_gallery_image("123456", "今天想看看猫猫"))
        self.assertIsNone(await repository.random_gallery_image("123456", "没有命中"))
        self.assertEqual(repository.list_quotes("123456"), [])
        total, first_page = repository.list_galleries_page(
            "123456",
            limit=1,
            offset=0,
        )
        self.assertEqual(total, 2)
        self.assertEqual(first_page, [("猫", 1)])

        gallery_asset = selected[1]
        quote = Quote(
            id="q_reuse_gallery_asset",
            qq="10002",
            name="复用用户",
            text="",
            created_by="20002",
            created_at=1.0,
            group="123456",
            content_fingerprint="reuse-gallery-asset",
        )
        result = await repository.create_quote_with_segments(
            "123456",
            quote,
            [PendingQuoteSegment(type="image", image=prepared_image)],
        )
        self.assertIsNotNone(result.quote)
        self.assertEqual(quote.image_ids, [gallery_asset.asset_id])
        self.assertEqual(
            repository.find_asset("123456", gallery_asset.asset_id).ref_count,
            3,
        )
        self.assertTrue(await repository.delete_quote(quote.id))
        self.assertEqual(
            repository.find_asset("123456", gallery_asset.asset_id).ref_count,
            2,
        )

    async def test_storage_audit_reports_missing_mismatched_and_orphan_files(self) -> None:
        repository = QuoteRepository(self.root)
        source = BytesIO()
        PillowImage.new("RGB", (48, 32), (15, 30, 45)).save(source, format="PNG")
        prepared_image = prepare_image(source.getvalue(), source="audit.png")
        quote = Quote(
            id="q_audit",
            qq="10001",
            name="检查用户",
            text="",
            created_by="20002",
            created_at=1.0,
            group="123456",
            content_fingerprint="audit-fingerprint",
        )
        await repository.create_quote_with_segments(
            "123456",
            quote,
            [PendingQuoteSegment(type="image", image=prepared_image)],
        )
        await repository.add_gallery_images("123456", "检查图库", [prepared_image])

        healthy = await repository.audit_storage("123456")
        self.assertTrue(healthy.healthy)
        self.assertEqual(healthy.quote_count, 1)
        self.assertEqual(healthy.gallery_count, 1)
        self.assertEqual(healthy.image_asset_count, 1)
        self.assertEqual(healthy.image_references, 2)

        asset = repository.find_asset("123456", quote.image_ids[0])
        (self.root / asset.rel_path).unlink()
        orphan_path = repository.get_store("123456").images_dir / "orphan.jpg"
        orphan_path.write_bytes(b"orphan")
        connection = sqlite3.connect(self.root / DATABASE_FILENAME)
        try:
            connection.execute(
                "UPDATE image_assets SET ref_count = 99 WHERE asset_id = ?",
                (asset.asset_id,),
            )
            connection.commit()
        finally:
            connection.close()

        unhealthy = await repository.audit_storage("123456")
        self.assertFalse(unhealthy.healthy)
        self.assertEqual(unhealthy.missing_image_files, 1)
        self.assertEqual(unhealthy.image_ref_count_mismatches, 1)
        self.assertEqual(unhealthy.orphan_image_files, 1)

    async def test_gallery_random_avoids_recent_images(self) -> None:
        repository = QuoteRepository(self.root)
        prepared_images = []
        for pattern_index in range(3):
            source = BytesIO()
            image = PillowImage.new("L", (48, 32))
            for y in range(32):
                for x in range(48):
                    if pattern_index == 0:
                        value = max(0, 255 - x * 5)
                    elif pattern_index == 1:
                        value = min(255, y * 8)
                    else:
                        value = 255 if (x + y) % 2 else 0
                    image.putpixel((x, y), value)
            image.save(source, format="PNG")
            prepared_images.append(prepare_image(source.getvalue(), source="gallery.png"))
        self.assertEqual(
            await repository.add_gallery_images("123456", "轮播", prepared_images),
            (3, 0),
        )

        concurrent_selections = await asyncio.gather(
            *[
                repository.random_gallery_image("123456", "轮播")
                for _ in range(3)
            ]
        )
        self.assertTrue(all(selected is not None for selected in concurrent_selections))
        selected_ids = [selected[1].asset_id for selected in concurrent_selections]
        fourth = await repository.random_gallery_image("123456", "轮播")
        self.assertIsNotNone(fourth)
        selected_ids.append(fourth[1].asset_id)

        self.assertEqual(len(set(selected_ids[:3])), 3)
        self.assertEqual(selected_ids[3], selected_ids[0])
        self.assertNotEqual(selected_ids[3], selected_ids[2])

    async def test_delete_gallery_preserves_shared_assets_and_removes_gif(self) -> None:
        repository = QuoteRepository(self.root)

        static_source = BytesIO()
        static_image = PillowImage.new("L", (48, 32))
        for y in range(32):
            for x in range(48):
                static_image.putpixel((x, y), 255 if (x + y) % 2 else 0)
        static_image.save(static_source, format="PNG")
        shared_image = prepare_image(static_source.getvalue(), source="shared.png")

        gif_source = BytesIO()
        first = PillowImage.new("RGBA", (80, 32), (255, 0, 0, 255))
        second = PillowImage.new("RGBA", (80, 32), (0, 0, 255, 255))
        first.save(
            gif_source,
            format="GIF",
            save_all=True,
            append_images=[second],
            duration=[80, 120],
            loop=0,
        )
        original_gif = gif_source.getvalue()
        animated_image = prepare_image(original_gif, source="animated.gif")

        quote = Quote(
            id="q_shared_gallery",
            qq="10001",
            name="共享用户",
            text="",
            created_by="20002",
            created_at=1.0,
            group="123456",
            content_fingerprint="shared-gallery-image",
        )
        result = await repository.create_quote_with_segments(
            "123456",
            quote,
            [PendingQuoteSegment(type="image", image=shared_image)],
        )
        self.assertIsNotNone(result.quote)
        shared_asset = repository.find_asset("123456", quote.image_ids[0])
        shared_path = self.root / shared_asset.rel_path

        self.assertEqual(
            await repository.add_gallery_images(
                "123456",
                "待删除",
                [shared_image, animated_image],
            ),
            (2, 0),
        )
        self.assertEqual(
            await repository.add_gallery_images("123456", "保留", [shared_image]),
            (1, 0),
        )
        await repository.random_gallery_image("123456", "待删除")

        connection = sqlite3.connect(self.root / DATABASE_FILENAME)
        connection.row_factory = sqlite3.Row
        try:
            gif_row = connection.execute(
                """
                SELECT image_assets.* FROM gallery_images
                JOIN image_assets USING (asset_id)
                WHERE gallery_images.session_key = ?
                  AND gallery_images.keyword = ?
                  AND image_assets.file_name LIKE '%.gif'
                """,
                ("123456", "待删除"),
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNotNone(gif_row)
        gif_path = self.root / str(gif_row["rel_path"])
        self.assertEqual(gif_path.suffix, ".gif")
        self.assertEqual(gif_path.read_bytes(), original_gif)

        self.assertEqual(
            await repository.delete_gallery_image(
                "123456",
                "待删除",
                animated_image,
            ),
            ("deleted", True),
        )
        self.assertFalse(gif_path.exists())
        self.assertEqual(repository.gallery_image_count("123456", "待删除"), 1)
        self.assertEqual(
            await repository.delete_gallery_image(
                "123456",
                "待删除",
                animated_image,
            ),
            ("image_not_found", False),
        )
        self.assertEqual(
            await repository.delete_gallery_image(
                "123456",
                "不存在",
                animated_image,
            ),
            ("gallery_not_found", False),
        )

        self.assertEqual(await repository.delete_gallery("123456", "待删除"), (1, 0))
        self.assertTrue(shared_path.exists())
        self.assertIsNone(await repository.random_gallery_image("123456", "待删除"))
        self.assertIsNotNone(await repository.random_gallery_image("123456", "保留"))
        self.assertEqual(await repository.delete_gallery("123456", "不存在"), (0, 0))

        self.assertEqual(await repository.delete_gallery("123456", "保留"), (1, 0))
        self.assertTrue(shared_path.exists())
        self.assertTrue(await repository.delete_quote(quote.id))
        self.assertFalse(shared_path.exists())

    async def test_quote_rankings_use_bound_tag(self) -> None:
        repository = QuoteRepository(self.root)
        for index, (qq, name) in enumerate(
            [("10001", "张三"), ("10002", "李四"), ("10001", "张三新昵称")]
        ):
            quote = Quote(
                id=f"q_rank_{index}",
                qq=qq,
                name=name,
                text=f"排名语录 {index}",
                created_by="20002",
                created_at=float(index),
                group="123456",
                content_fingerprint=f"rank-fingerprint-{index}",
            )
            await repository.create_quote_with_segments(
                "123456",
                quote,
                [PendingQuoteSegment(type="text", text=quote.text)],
            )
        await repository.create_binding("123456", "10001", "名言")

        self.assertEqual(
            repository.quote_rankings("123456"),
            [("10001", "名言", 2), ("10002", "李四", 1)],
        )

    async def test_upgrades_sqlite_schema_v1_to_v5(self) -> None:
        repository = QuoteRepository(self.root)
        legacy_quote = Quote(
            id="q_before_binding_schema",
            qq="10001",
            name="旧用户",
            text="升级前数据",
            created_by="20002",
            created_at=100.0,
            group="123456",
            content_fingerprint="before-binding-schema",
        )
        await repository.create_quote_with_segments(
            "123456",
            legacy_quote,
            [PendingQuoteSegment(type="text", text="升级前数据")],
        )

        database_path = self.root / DATABASE_FILENAME
        connection = sqlite3.connect(database_path)
        try:
            connection.execute("DROP TABLE quote_bindings")
            connection.execute("DROP TABLE gallery_sent_records")
            connection.execute("DROP TABLE gallery_images")
            connection.execute("PRAGMA user_version = 1")
            connection.commit()
        finally:
            connection.close()

        repository = QuoteRepository(self.root)
        self.assertEqual(
            repository.get_quote("123456", "q_before_binding_schema").text,
            "升级前数据",
        )
        self.assertEqual(
            await repository.create_binding("123456", "10001", "tag"),
            ("created", "tag"),
        )
        connection = sqlite3.connect(database_path)
        try:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                DATABASE_SCHEMA_VERSION,
            )
            table = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'quote_bindings'"
            ).fetchone()
            self.assertIsNotNone(table)
            gallery_table = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'gallery_images'"
            ).fetchone()
            self.assertIsNotNone(gallery_table)
            history_table = connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'gallery_sent_records'"
            ).fetchone()
            self.assertIsNotNone(history_table)
            random_state_table = connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'quote_random_state'"
            ).fetchone()
            self.assertIsNotNone(random_state_table)
        finally:
            connection.close()

    async def test_upgrades_v3_gallery_without_data_loss(self) -> None:
        repository = QuoteRepository(self.root)
        source = BytesIO()
        PillowImage.new("RGB", (48, 32), (30, 60, 90)).save(source, format="PNG")
        prepared_image = prepare_image(source.getvalue(), source="legacy-gallery.png")
        self.assertEqual(
            await repository.add_gallery_images("123456", "旧图库", [prepared_image]),
            (1, 0),
        )

        database_path = self.root / DATABASE_FILENAME
        connection = sqlite3.connect(database_path)
        try:
            connection.execute("DROP TABLE gallery_sent_records")
            connection.execute("PRAGMA user_version = 3")
            connection.commit()
        finally:
            connection.close()

        upgraded = QuoteRepository(self.root)
        selected = await upgraded.random_gallery_image("123456", "旧图库")
        self.assertIsNotNone(selected)
        self.assertEqual(selected[0], "旧图库")
        self.assertTrue((self.root / selected[1].rel_path).exists())
        connection = sqlite3.connect(database_path)
        try:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                DATABASE_SCHEMA_VERSION,
            )
            history_count = connection.execute(
                "SELECT COUNT(*) FROM gallery_sent_records"
            ).fetchone()[0]
            self.assertEqual(history_count, 1)
        finally:
            connection.close()

    async def test_migrates_root_legacy_json(self) -> None:
        (self.root / "quotes.json").write_text(
            json.dumps(
                {
                    "quotes": [
                        {
                            "id": "q_root",
                            "qq": "10003",
                            "name": "更旧用户",
                            "text": "根目录旧语录",
                            "created_by": "20004",
                            "created_at": 5.0,
                            "group": "445566",
                            "images": [],
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        repository = QuoteRepository(self.root)
        self.assertTrue(await repository.migrate_legacy_data())
        migrated = repository.get_quote("445566", "q_root")
        self.assertIsNotNone(migrated)
        self.assertEqual(migrated.text, "根目录旧语录")
        self.assertTrue((self.root / "quotes.json.bak").exists())

    async def test_corrupt_json_is_left_untouched(self) -> None:
        session_dir = self.root / "groups" / "broken"
        session_dir.mkdir(parents=True)
        broken_file = session_dir / "quotes.json"
        broken_file.write_text("{broken", encoding="utf-8")

        repository = QuoteRepository(self.root)
        self.assertFalse(await repository.migrate_legacy_data())
        self.assertTrue(broken_file.exists())
        self.assertFalse((session_dir / "quotes.json.migrated.bak").exists())

    async def test_git_backup_snapshot_blocks_database_writes_until_staged(self) -> None:
        repository = QuoteRepository(self.root)

        async with repository.git_backup_snapshot():
            pending_write = asyncio.create_task(
                repository.create_binding("123456", "10001", "测试标签")
            )
            await asyncio.sleep(0)
            self.assertFalse(pending_write.done())

        status, detail = await pending_write
        self.assertEqual(status, "created")
        self.assertEqual(detail, "测试标签")


if __name__ == "__main__":
    unittest.main()

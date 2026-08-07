from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from constants import DATABASE_FILENAME, DATABASE_SCHEMA_VERSION
from models import (
    ImageSignature,
    PendingForwardNode,
    PendingForwardSegment,
    PendingQuoteSegment,
    PreparedImage,
    PreparedMedia,
    Quote,
)
from sqlite_store import QuoteRepository


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
        prepared_image = PreparedImage(
            content=b"image-content",
            extension=".png",
            sha256="image-sha",
            dhash="0000000000000000",
            width=100,
            height=100,
        )
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
        self.assertTrue(duplicate_result.duplicate)

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
        self.assertFalse(image_path.exists())
        self.assertTrue(await repository.delete_quote("q_forward"))
        self.assertFalse(media_path.exists())

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


if __name__ == "__main__":
    unittest.main()

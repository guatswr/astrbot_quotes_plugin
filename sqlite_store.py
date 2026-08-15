from __future__ import annotations

import asyncio
import json
import secrets
import sqlite3
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from time import time
from typing import Any, Iterable

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

try:
    from .constants import (
        DATABASE_FILENAME,
        DATABASE_SCHEMA_VERSION,
        DUPLICATE_IMAGE_MESSAGE,
        DUPLICATE_QUOTE_MESSAGE,
        GALLERY_RECENT_WINDOW,
        IMAGE_INDEX_FILENAME,
        MAX_GALLERY_SENT_RECORDS,
        MAX_SENT_RECORDS,
        MEDIA_INDEX_FILENAME,
        QUOTES_FILENAME,
        SENT_INDEX_FILENAME,
    )
    from .models import (
        ImageAsset,
        ImageSignature,
        MediaAsset,
        PendingForwardNode,
        PendingQuoteSegment,
        PreparedImage,
        Quote,
        QuoteBinding,
        QuoteSegment,
        SentQuoteRecord,
        StorageAuditResult,
    )
    from .store import CreateQuoteResult, QuoteRepository as JsonQuoteRepository
    from .utils import is_near_duplicate
except ImportError:  # pragma: no cover
    from constants import (
        DATABASE_FILENAME,
        DATABASE_SCHEMA_VERSION,
        DUPLICATE_IMAGE_MESSAGE,
        DUPLICATE_QUOTE_MESSAGE,
        GALLERY_RECENT_WINDOW,
        IMAGE_INDEX_FILENAME,
        MAX_GALLERY_SENT_RECORDS,
        MAX_SENT_RECORDS,
        MEDIA_INDEX_FILENAME,
        QUOTES_FILENAME,
        SENT_INDEX_FILENAME,
    )
    from models import (
        ImageAsset,
        ImageSignature,
        MediaAsset,
        PendingForwardNode,
        PendingQuoteSegment,
        PreparedImage,
        Quote,
        QuoteBinding,
        QuoteSegment,
        SentQuoteRecord,
        StorageAuditResult,
    )
    from store import CreateQuoteResult, QuoteRepository as JsonQuoteRepository
    from utils import is_near_duplicate


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _random_key() -> int:
    return secrets.randbits(63)


class SQLiteQuoteRepository(JsonQuoteRepository):
    """SQLite-backed repository with automatic migration from JSON storage."""

    def __init__(self, plugin_root: Path):
        super().__init__(plugin_root)
        self.db_path = self.root / DATABASE_FILENAME
        self._db_write_lock = asyncio.Lock()
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _initialize_database(self) -> None:
        with self._connection() as connection:
            current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current_version > DATABASE_SCHEMA_VERSION:
                raise RuntimeError(
                    f"语录数据库版本过新: {current_version} > {DATABASE_SCHEMA_VERSION}"
                )
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            if current_version == 0:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS quotes (
                        id TEXT PRIMARY KEY,
                        session_key TEXT NOT NULL,
                        qq TEXT NOT NULL DEFAULT '',
                        name TEXT NOT NULL DEFAULT '',
                        text TEXT NOT NULL DEFAULT '',
                        created_by TEXT NOT NULL DEFAULT '',
                        created_at REAL NOT NULL DEFAULT 0,
                        kind TEXT NOT NULL DEFAULT 'standard',
                        image_ids_json TEXT NOT NULL DEFAULT '[]',
                        media_ids_json TEXT NOT NULL DEFAULT '[]',
                        segments_json TEXT NOT NULL DEFAULT '[]',
                        forward_nodes_json TEXT NOT NULL DEFAULT '[]',
                        content_fingerprint TEXT NOT NULL DEFAULT '',
                        random_key INTEGER NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_quotes_session_random
                    ON quotes(session_key, random_key);
                    CREATE INDEX IF NOT EXISTS idx_quotes_session_qq_random
                    ON quotes(session_key, qq, random_key);
                    CREATE INDEX IF NOT EXISTS idx_quotes_qq_random
                    ON quotes(qq, random_key);
                    CREATE INDEX IF NOT EXISTS idx_quotes_random
                    ON quotes(random_key);
                    CREATE INDEX IF NOT EXISTS idx_quotes_fingerprint
                    ON quotes(session_key, qq, content_fingerprint);
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_quotes_fingerprint
                    ON quotes(session_key, qq, content_fingerprint)
                    WHERE content_fingerprint <> '';

                    CREATE TABLE IF NOT EXISTS image_assets (
                        asset_id TEXT PRIMARY KEY,
                        session_key TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        rel_path TEXT NOT NULL,
                        sha256 TEXT NOT NULL DEFAULT '',
                        dhash TEXT NOT NULL DEFAULT '',
                        width INTEGER NOT NULL DEFAULT 0,
                        height INTEGER NOT NULL DEFAULT 0,
                        ref_count INTEGER NOT NULL DEFAULT 0,
                        created_at REAL NOT NULL DEFAULT 0
                    );
                    CREATE INDEX IF NOT EXISTS idx_images_session_sha
                    ON image_assets(session_key, sha256);
                    CREATE INDEX IF NOT EXISTS idx_images_session
                    ON image_assets(session_key);

                    CREATE TABLE IF NOT EXISTS media_assets (
                        asset_id TEXT PRIMARY KEY,
                        session_key TEXT NOT NULL,
                        media_type TEXT NOT NULL DEFAULT '',
                        file_name TEXT NOT NULL,
                        rel_path TEXT NOT NULL,
                        display_name TEXT NOT NULL DEFAULT '',
                        ref_count INTEGER NOT NULL DEFAULT 0,
                        created_at REAL NOT NULL DEFAULT 0
                    );
                    CREATE INDEX IF NOT EXISTS idx_media_session
                    ON media_assets(session_key);

                    CREATE TABLE IF NOT EXISTS sent_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_key TEXT NOT NULL,
                        quote_id TEXT NOT NULL,
                        fingerprint TEXT NOT NULL,
                        sent_at REAL NOT NULL DEFAULT 0,
                        image_signatures_json TEXT NOT NULL DEFAULT '[]',
                        FOREIGN KEY(quote_id) REFERENCES quotes(id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_sent_lookup
                    ON sent_records(session_key, fingerprint, sent_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_sent_quote
                    ON sent_records(quote_id);
                    """
                )
                current_version = 1
                connection.execute("PRAGMA user_version = 1")
            if current_version < 2:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS quote_bindings (
                        session_key TEXT NOT NULL,
                        qq TEXT NOT NULL,
                        tag TEXT NOT NULL,
                        created_at REAL NOT NULL DEFAULT 0,
                        updated_at REAL NOT NULL DEFAULT 0,
                        PRIMARY KEY(session_key, qq),
                        UNIQUE(session_key, tag)
                    );
                    CREATE INDEX IF NOT EXISTS idx_bindings_tag
                    ON quote_bindings(session_key, tag);
                    """
                )
                current_version = 2
                connection.execute("PRAGMA user_version = 2")
            if current_version < 3:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS gallery_images (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_key TEXT NOT NULL,
                        keyword TEXT NOT NULL,
                        asset_id TEXT NOT NULL,
                        created_at REAL NOT NULL DEFAULT 0,
                        random_key INTEGER NOT NULL,
                        UNIQUE(session_key, keyword, asset_id),
                        FOREIGN KEY(asset_id) REFERENCES image_assets(asset_id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_gallery_keyword_random
                    ON gallery_images(session_key, keyword, random_key);
                    CREATE INDEX IF NOT EXISTS idx_gallery_asset
                    ON gallery_images(asset_id);
                    """
                )
                current_version = 3
                connection.execute("PRAGMA user_version = 3")
            if current_version < 4:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS gallery_sent_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_key TEXT NOT NULL,
                        keyword TEXT NOT NULL,
                        asset_id TEXT NOT NULL,
                        sent_at REAL NOT NULL DEFAULT 0,
                        FOREIGN KEY(asset_id) REFERENCES image_assets(asset_id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_gallery_sent_recent
                    ON gallery_sent_records(session_key, keyword, id DESC);
                    CREATE INDEX IF NOT EXISTS idx_gallery_sent_asset
                    ON gallery_sent_records(asset_id);
                    """
                )
                current_version = 4
                connection.execute("PRAGMA user_version = 4")
            if current_version < 5:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS quote_random_state (
                        session_key TEXT PRIMARY KEY,
                        quote_id TEXT NOT NULL,
                        selected_at REAL NOT NULL DEFAULT 0,
                        FOREIGN KEY(quote_id) REFERENCES quotes(id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_quote_random_state_quote
                    ON quote_random_state(quote_id);
                    """
                )
                current_version = 5
                connection.execute("PRAGMA user_version = 5")
            connection.commit()

    def session_keys(self) -> list[str]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT DISTINCT session_key FROM quotes ORDER BY session_key"
            ).fetchall()
        return [str(row["session_key"]) for row in rows]

    def _row_to_binding(self, row: sqlite3.Row) -> QuoteBinding:
        return QuoteBinding(
            session_key=str(row["session_key"]),
            qq=str(row["qq"]),
            tag=str(row["tag"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )

    def get_binding_by_tag(self, session_key: str, tag: str) -> QuoteBinding | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM quote_bindings
                WHERE session_key = ? AND tag = ?
                LIMIT 1
                """,
                (session_key, tag),
            ).fetchone()
        return self._row_to_binding(row) if row is not None else None

    def get_binding_for_qq(self, session_key: str, qq: str) -> QuoteBinding | None:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM quote_bindings
                WHERE session_key = ? AND qq = ?
                LIMIT 1
                """,
                (session_key, str(qq)),
            ).fetchone()
        return self._row_to_binding(row) if row is not None else None

    def list_bindings(self, session_key: str) -> list[QuoteBinding]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM quote_bindings
                WHERE session_key = ?
                ORDER BY tag, qq
                """,
                (session_key,),
            ).fetchall()
        return [self._row_to_binding(row) for row in rows]

    def list_bindings_for_qq_global(self, qq: str) -> list[QuoteBinding]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM quote_bindings
                WHERE qq = ?
                ORDER BY session_key, tag
                """,
                (str(qq),),
            ).fetchall()
        return [self._row_to_binding(row) for row in rows]

    def list_all_bindings(self) -> list[QuoteBinding]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM quote_bindings ORDER BY session_key, qq, tag"
            ).fetchall()
        return [self._row_to_binding(row) for row in rows]

    async def create_binding(self, session_key: str, qq: str, tag: str) -> tuple[str, str]:
        if not session_key or not qq or not str(tag).strip():
            return "invalid", ""
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._create_binding_sync,
                session_key,
                str(qq),
                str(tag).strip(),
            )

    def _create_binding_sync(self, session_key: str, qq: str, tag: str) -> tuple[str, str]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM quote_bindings WHERE session_key = ? AND qq = ?",
                (session_key, qq),
            ).fetchone()
            if existing is not None:
                connection.rollback()
                existing_tag = str(existing["tag"])
                return ("unchanged", existing_tag) if existing_tag == tag else ("qq_exists", existing_tag)
            tag_owner = connection.execute(
                "SELECT qq FROM quote_bindings WHERE session_key = ? AND tag = ?",
                (session_key, tag),
            ).fetchone()
            if tag_owner is not None:
                connection.rollback()
                return "tag_exists", str(tag_owner["qq"])
            gallery = connection.execute(
                """
                SELECT 1 FROM gallery_images
                WHERE session_key = ? AND keyword = ?
                LIMIT 1
                """,
                (session_key, tag),
            ).fetchone()
            if gallery is not None:
                connection.rollback()
                return "gallery_exists", tag
            now = time()
            connection.execute(
                """
                INSERT INTO quote_bindings (
                    session_key, qq, tag, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (session_key, qq, tag, now, now),
            )
            connection.commit()
            return "created", tag
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def rebind(self, session_key: str, qq: str, tag: str = "") -> tuple[str, str]:
        if not session_key or not qq:
            return "invalid", ""
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._rebind_sync,
                session_key,
                str(qq),
                str(tag).strip(),
            )

    def _rebind_sync(self, session_key: str, qq: str, tag: str) -> tuple[str, str]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM quote_bindings WHERE session_key = ? AND qq = ?",
                (session_key, qq),
            ).fetchone()
            if existing is None:
                connection.rollback()
                return "not_found", ""
            old_tag = str(existing["tag"])
            if not tag:
                connection.execute(
                    "DELETE FROM quote_bindings WHERE session_key = ? AND qq = ?",
                    (session_key, qq),
                )
                connection.commit()
                return "removed", old_tag
            if tag == old_tag:
                connection.rollback()
                return "unchanged", old_tag
            tag_owner = connection.execute(
                "SELECT qq FROM quote_bindings WHERE session_key = ? AND tag = ?",
                (session_key, tag),
            ).fetchone()
            if tag_owner is not None:
                connection.rollback()
                return "tag_exists", str(tag_owner["qq"])
            gallery = connection.execute(
                """
                SELECT 1 FROM gallery_images
                WHERE session_key = ? AND keyword = ?
                LIMIT 1
                """,
                (session_key, tag),
            ).fetchone()
            if gallery is not None:
                connection.rollback()
                return "gallery_exists", tag
            connection.execute(
                """
                UPDATE quote_bindings
                SET tag = ?, updated_at = ?
                WHERE session_key = ? AND qq = ?
                """,
                (tag, time(), session_key, qq),
            )
            connection.commit()
            return "updated", old_tag
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _row_to_quote(self, row: sqlite3.Row) -> Quote:
        return Quote.from_dict(
            {
                "id": row["id"],
                "qq": row["qq"],
                "name": row["name"],
                "text": row["text"],
                "created_by": row["created_by"],
                "created_at": row["created_at"],
                "group": row["session_key"],
                "kind": row["kind"],
                "image_ids": _json_loads(row["image_ids_json"], []),
                "media_ids": _json_loads(row["media_ids_json"], []),
                "segments": _json_loads(row["segments_json"], []),
                "forward_nodes": _json_loads(row["forward_nodes_json"], []),
                "content_fingerprint": row["content_fingerprint"],
            }
        )

    def _quote_values(self, quote: Quote) -> tuple[Any, ...]:
        payload = quote.to_dict()
        return (
            quote.id,
            quote.group,
            quote.qq,
            quote.name,
            quote.text,
            quote.created_by,
            quote.created_at,
            quote.kind,
            _json_dumps(payload["image_ids"]),
            _json_dumps(payload["media_ids"]),
            _json_dumps(payload["segments"]),
            _json_dumps(payload["forward_nodes"]),
            quote.content_fingerprint,
            _random_key(),
        )

    def _insert_quote(self, connection: sqlite3.Connection, quote: Quote, *, ignore: bool = False) -> None:
        clause = "OR IGNORE " if ignore else ""
        connection.execute(
            f"""
            INSERT {clause}INTO quotes (
                id, session_key, qq, name, text, created_by, created_at, kind,
                image_ids_json, media_ids_json, segments_json, forward_nodes_json,
                content_fingerprint, random_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._quote_values(quote),
        )

    def list_quotes(self, session_key: str) -> list[Quote]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM quotes WHERE session_key = ? ORDER BY created_at, id",
                (session_key,),
            ).fetchall()
        return [self._row_to_quote(row) for row in rows]

    def list_all_quotes(self) -> list[Quote]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM quotes ORDER BY created_at DESC, id DESC"
            ).fetchall()
        return [self._row_to_quote(row) for row in rows]

    def list_quotes_page(
        self,
        session_key: str,
        *,
        limit: int,
        offset: int,
    ) -> tuple[int, list[Quote]]:
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        with self._connection() as connection:
            total = int(
                connection.execute(
                    "SELECT COUNT(*) FROM quotes WHERE session_key = ?",
                    (session_key,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                """
                SELECT * FROM quotes
                WHERE session_key = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (session_key, safe_limit, safe_offset),
            ).fetchall()
        return total, [self._row_to_quote(row) for row in rows]

    def quote_rankings(self, session_key: str) -> list[tuple[str, str, int]]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    q.qq AS qq,
                    COALESCE(
                        NULLIF(b.tag, ''),
                        NULLIF((
                            SELECT q2.name
                            FROM quotes AS q2
                            WHERE q2.session_key = q.session_key
                              AND (
                                  (q.qq <> '' AND q2.qq = q.qq)
                                  OR (
                                      q.qq = '' AND q2.qq = ''
                                      AND q2.name = q.name
                                  )
                              )
                            ORDER BY q2.created_at DESC, q2.id DESC
                            LIMIT 1
                        ), ''),
                        NULLIF(q.qq, ''),
                        '未知用户'
                    ) AS display_name,
                    COUNT(*) AS quote_count
                FROM quotes AS q
                LEFT JOIN quote_bindings AS b
                  ON b.session_key = q.session_key AND b.qq = q.qq
                WHERE q.session_key = ?
                GROUP BY q.qq, CASE WHEN q.qq = '' THEN q.name ELSE '' END, b.tag
                ORDER BY quote_count DESC, display_name, q.qq
                """,
                (session_key,),
            ).fetchall()
        return [
            (str(row["qq"]), str(row["display_name"]), int(row["quote_count"]))
            for row in rows
        ]

    def list_quotes_for_owner(self, session_key: str, qq: str) -> list[Quote]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM quotes WHERE session_key = ? AND qq = ? ORDER BY created_at, id",
                (session_key, str(qq)),
            ).fetchall()
        return [self._row_to_quote(row) for row in rows]

    def list_quotes_for_owner_global(self, qq: str) -> list[Quote]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM quotes WHERE qq = ? ORDER BY created_at, id",
                (str(qq),),
            ).fetchall()
        return [self._row_to_quote(row) for row in rows]

    def get_quote(self, session_key: str, quote_id: str) -> Quote | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM quotes WHERE session_key = ? AND id = ?",
                (session_key, quote_id),
            ).fetchone()
        return self._row_to_quote(row) if row is not None else None

    def has_content_fingerprint(self, session_key: str, qq: str, fingerprint: str) -> bool:
        if not fingerprint:
            return False
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM quotes
                WHERE session_key = ? AND qq = ? AND content_fingerprint = ?
                LIMIT 1
                """,
                (session_key, str(qq), fingerprint),
            ).fetchone()
        return row is not None

    def update_content_fingerprint(self, quote_id: str, fingerprint: str) -> None:
        if not quote_id or not fingerprint:
            return
        try:
            with self._connection() as connection:
                connection.execute(
                    "UPDATE OR IGNORE quotes SET content_fingerprint = ? WHERE id = ?",
                    (fingerprint, quote_id),
                )
                connection.commit()
        except sqlite3.Error as exc:
            logger.info(f"更新语录内容指纹失败: quote_id={quote_id}, error={exc}")

    def _image_from_row(self, row: sqlite3.Row) -> ImageAsset:
        return ImageAsset(
            asset_id=str(row["asset_id"]),
            file_name=str(row["file_name"]),
            rel_path=str(row["rel_path"]),
            sha256=str(row["sha256"]),
            dhash=str(row["dhash"]),
            width=int(row["width"]),
            height=int(row["height"]),
            ref_count=int(row["ref_count"]),
            created_at=float(row["created_at"]),
        )

    def _media_from_row(self, row: sqlite3.Row) -> MediaAsset:
        return MediaAsset(
            asset_id=str(row["asset_id"]),
            media_type=str(row["media_type"]),
            file_name=str(row["file_name"]),
            rel_path=str(row["rel_path"]),
            display_name=str(row["display_name"]),
            ref_count=int(row["ref_count"]),
            created_at=float(row["created_at"]),
        )

    def _select_in_chunks(
        self,
        connection: sqlite3.Connection,
        table: str,
        session_key: str,
        ids: Iterable[str],
    ) -> list[sqlite3.Row]:
        values = list(dict.fromkeys(str(item) for item in ids if str(item)))
        rows: list[sqlite3.Row] = []
        for offset in range(0, len(values), 500):
            chunk = values[offset : offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(
                connection.execute(
                    f"SELECT * FROM {table} WHERE session_key = ? AND asset_id IN ({placeholders})",
                    (session_key, *chunk),
                ).fetchall()
            )
        return rows

    def find_assets(self, session_key: str, asset_ids: Iterable[str]) -> dict[str, ImageAsset]:
        with self._connection() as connection:
            rows = self._select_in_chunks(connection, "image_assets", session_key, asset_ids)
        assets = [self._image_from_row(row) for row in rows]
        return {asset.asset_id: asset for asset in assets}

    def find_media_assets(self, session_key: str, asset_ids: Iterable[str]) -> dict[str, MediaAsset]:
        with self._connection() as connection:
            rows = self._select_in_chunks(connection, "media_assets", session_key, asset_ids)
        assets = [self._media_from_row(row) for row in rows]
        return {asset.asset_id: asset for asset in assets}

    def find_asset(self, session_key: str, asset_id: str) -> ImageAsset | None:
        return self.find_assets(session_key, [asset_id]).get(asset_id)

    def find_media_asset(self, session_key: str, asset_id: str) -> MediaAsset | None:
        return self.find_media_assets(session_key, [asset_id]).get(asset_id)

    def _list_image_assets(self, connection: sqlite3.Connection, session_key: str) -> list[ImageAsset]:
        rows = connection.execute(
            "SELECT * FROM image_assets WHERE session_key = ?",
            (session_key,),
        ).fetchall()
        return [self._image_from_row(row) for row in rows]

    def _insert_image_asset(self, connection: sqlite3.Connection, session_key: str, asset: ImageAsset, *, ignore: bool = False) -> None:
        clause = "OR IGNORE " if ignore else ""
        connection.execute(
            f"""
            INSERT {clause}INTO image_assets (
                asset_id, session_key, file_name, rel_path, sha256, dhash,
                width, height, ref_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset.asset_id,
                session_key,
                asset.file_name,
                asset.rel_path,
                asset.sha256,
                asset.dhash,
                asset.width,
                asset.height,
                asset.ref_count,
                asset.created_at,
            ),
        )

    async def add_gallery_images(
        self,
        session_key: str,
        keyword: str,
        images: list[PreparedImage],
    ) -> tuple[int, int]:
        normalized_keyword = str(keyword or "").strip()
        valid_images = [image for image in images if image is not None and image.content]
        if not session_key or not normalized_keyword or not valid_images:
            return 0, 0
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._add_gallery_images_sync,
                session_key,
                normalized_keyword,
                valid_images,
            )

    def _add_gallery_images_sync(
        self,
        session_key: str,
        keyword: str,
        images: list[PreparedImage],
    ) -> tuple[int, int]:
        store = self.get_store(session_key)
        created_files: list[Path] = []
        connection = self._connect()
        added = 0
        skipped = 0
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing_assets = self._list_image_assets(connection, session_key)
            created_at = time()
            for image in images:
                matched_asset = next(
                    (
                        asset
                        for asset in existing_assets
                        if is_near_duplicate(
                            image,
                            asset.sha256,
                            asset.dhash,
                            asset.width,
                            asset.height,
                        )
                    ),
                    None,
                )
                if matched_asset is not None:
                    existing_mapping = connection.execute(
                        """
                        SELECT 1 FROM gallery_images
                        WHERE session_key = ? AND keyword = ? AND asset_id = ?
                        LIMIT 1
                        """,
                        (session_key, keyword, matched_asset.asset_id),
                    ).fetchone()
                    if existing_mapping is not None:
                        skipped += 1
                        continue
                    connection.execute(
                        "UPDATE image_assets SET ref_count = ref_count + 1 WHERE asset_id = ?",
                        (matched_asset.asset_id,),
                    )
                    asset = matched_asset
                else:
                    asset = self._persist_image_asset(
                        store,
                        image,
                        created_at=created_at,
                        created_files=created_files,
                    )
                    self._insert_image_asset(connection, session_key, asset)
                    existing_assets.append(asset)

                connection.execute(
                    """
                    INSERT INTO gallery_images (
                        session_key, keyword, asset_id, created_at, random_key
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (session_key, keyword, asset.asset_id, created_at, _random_key()),
                )
                added += 1
            connection.commit()
            return added, skipped
        except Exception:
            connection.rollback()
            for path in created_files:
                path.unlink(missing_ok=True)
            raise
        finally:
            connection.close()

    async def delete_gallery(self, session_key: str, keyword: str) -> tuple[int, int]:
        normalized_keyword = str(keyword or "").strip()
        if not session_key or not normalized_keyword:
            return 0, 0
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._delete_gallery_sync,
                session_key,
                normalized_keyword,
            )

    def _delete_gallery_sync(self, session_key: str, keyword: str) -> tuple[int, int]:
        connection = self._connect()
        files_to_remove: list[Path] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT gallery_images.asset_id
                FROM gallery_images
                WHERE session_key = ? AND keyword = ?
                """,
                (session_key, keyword),
            ).fetchall()
            if not rows:
                connection.rollback()
                return 0, 0

            asset_counts = Counter(str(row["asset_id"]) for row in rows)
            for asset_id, count in asset_counts.items():
                connection.execute(
                    """
                    UPDATE image_assets
                    SET ref_count = MAX(0, ref_count - ?)
                    WHERE session_key = ? AND asset_id = ?
                    """,
                    (count, session_key, asset_id),
                )

            connection.execute(
                "DELETE FROM gallery_sent_records WHERE session_key = ? AND keyword = ?",
                (session_key, keyword),
            )
            connection.execute(
                "DELETE FROM gallery_images WHERE session_key = ? AND keyword = ?",
                (session_key, keyword),
            )
            orphan_rows = connection.execute(
                """
                SELECT rel_path FROM image_assets
                WHERE session_key = ? AND ref_count <= 0
                """,
                (session_key,),
            ).fetchall()
            files_to_remove = [self.root / str(row["rel_path"]) for row in orphan_rows]
            connection.execute(
                "DELETE FROM image_assets WHERE session_key = ? AND ref_count <= 0",
                (session_key,),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        removed_files = 0
        for path in files_to_remove:
            try:
                path.unlink(missing_ok=True)
                removed_files += 1
            except OSError as exc:
                logger.info(f"删除图库资源文件失败: path={path}, error={exc}")
        return len(rows), removed_files

    def gallery_image_count(self, session_key: str, keyword: str) -> int:
        normalized_keyword = str(keyword or "").strip()
        if not session_key or not normalized_keyword:
            return 0
        with self._connection() as connection:
            return int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM gallery_images
                    WHERE session_key = ? AND keyword = ?
                    """,
                    (session_key, normalized_keyword),
                ).fetchone()[0]
            )

    def list_galleries_page(
        self,
        session_key: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[int, list[tuple[str, int]]]:
        safe_limit = max(1, min(100, int(limit)))
        safe_offset = max(0, int(offset))
        with self._connection() as connection:
            total = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT keyword) FROM gallery_images WHERE session_key = ?",
                    (session_key,),
                ).fetchone()[0]
            )
            rows = connection.execute(
                """
                SELECT keyword, COUNT(*) AS image_count
                FROM gallery_images
                WHERE session_key = ?
                GROUP BY keyword
                ORDER BY keyword
                LIMIT ? OFFSET ?
                """,
                (session_key, safe_limit, safe_offset),
            ).fetchall()
        return total, [
            (str(row["keyword"]), int(row["image_count"]))
            for row in rows
        ]

    async def audit_storage(self, session_key: str) -> StorageAuditResult:
        async with self._db_write_lock:
            return await asyncio.to_thread(self._audit_storage_sync, session_key)

    def _audit_storage_sync(self, session_key: str) -> StorageAuditResult:
        with self._connection() as connection:
            quote_rows = connection.execute(
                """
                SELECT image_ids_json, media_ids_json
                FROM quotes WHERE session_key = ?
                """,
                (session_key,),
            ).fetchall()
            gallery_rows = connection.execute(
                "SELECT keyword, asset_id FROM gallery_images WHERE session_key = ?",
                (session_key,),
            ).fetchall()
            image_rows = connection.execute(
                "SELECT asset_id, rel_path, ref_count FROM image_assets WHERE session_key = ?",
                (session_key,),
            ).fetchall()
            media_rows = connection.execute(
                "SELECT asset_id, rel_path, ref_count FROM media_assets WHERE session_key = ?",
                (session_key,),
            ).fetchall()
            binding_rows = connection.execute(
                "SELECT tag FROM quote_bindings WHERE session_key = ?",
                (session_key,),
            ).fetchall()

        image_references: Counter[str] = Counter()
        media_references: Counter[str] = Counter()
        for row in quote_rows:
            image_references.update(
                str(asset_id)
                for asset_id in _json_loads(row["image_ids_json"], [])
                if str(asset_id)
            )
            media_references.update(
                str(asset_id)
                for asset_id in _json_loads(row["media_ids_json"], [])
                if str(asset_id)
            )

        gallery_references = Counter(str(row["asset_id"]) for row in gallery_rows)
        expected_image_references = image_references + gallery_references
        image_assets = {str(row["asset_id"]): row for row in image_rows}
        media_assets = {str(row["asset_id"]): row for row in media_rows}

        tracked_image_paths = {
            (self.root / str(row["rel_path"])).resolve()
            for row in image_rows
        }
        tracked_media_paths = {
            (self.root / str(row["rel_path"])).resolve()
            for row in media_rows
        }
        session_root = self.groups_dir / session_key
        images_dir = session_root / "images"
        media_dir = session_root / "media"
        physical_image_paths = {
            path.resolve()
            for path in images_dir.rglob("*")
            if path.is_file()
        }
        physical_media_paths = {
            path.resolve()
            for path in media_dir.rglob("*")
            if path.is_file()
        }

        return StorageAuditResult(
            session_key=session_key,
            quote_count=len(quote_rows),
            gallery_count=len({str(row["keyword"]) for row in gallery_rows}),
            gallery_image_references=sum(gallery_references.values()),
            image_asset_count=len(image_rows),
            image_references=sum(expected_image_references.values()),
            media_asset_count=len(media_rows),
            media_references=sum(media_references.values()),
            missing_image_files=sum(not path.is_file() for path in tracked_image_paths),
            missing_media_files=sum(not path.is_file() for path in tracked_media_paths),
            missing_image_references=sum(
                count
                for asset_id, count in expected_image_references.items()
                if asset_id not in image_assets
            ),
            missing_media_references=sum(
                count
                for asset_id, count in media_references.items()
                if asset_id not in media_assets
            ),
            image_ref_count_mismatches=sum(
                int(row["ref_count"]) != expected_image_references.get(asset_id, 0)
                for asset_id, row in image_assets.items()
            ),
            media_ref_count_mismatches=sum(
                int(row["ref_count"]) != media_references.get(asset_id, 0)
                for asset_id, row in media_assets.items()
            ),
            orphan_image_files=len(physical_image_paths - tracked_image_paths),
            orphan_media_files=len(physical_media_paths - tracked_media_paths),
            tag_gallery_name_collisions=len(
                {str(row["tag"]) for row in binding_rows}
                & {str(row["keyword"]) for row in gallery_rows}
            ),
        )

    async def delete_gallery_image(
        self,
        session_key: str,
        keyword: str,
        image: PreparedImage,
    ) -> tuple[str, bool]:
        normalized_keyword = str(keyword or "").strip()
        if not session_key or not normalized_keyword or image is None:
            return "invalid", False
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._delete_gallery_image_sync,
                session_key,
                normalized_keyword,
                image,
            )

    def _delete_gallery_image_sync(
        self,
        session_key: str,
        keyword: str,
        image: PreparedImage,
    ) -> tuple[str, bool]:
        connection = self._connect()
        file_to_remove: Path | None = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT image_assets.*
                FROM gallery_images
                JOIN image_assets USING (asset_id)
                WHERE gallery_images.session_key = ?
                  AND gallery_images.keyword = ?
                """,
                (session_key, keyword),
            ).fetchall()
            if not rows:
                connection.rollback()
                return "gallery_not_found", False

            matched = next(
                (
                    row
                    for row in rows
                    if is_near_duplicate(
                        image,
                        str(row["sha256"]),
                        str(row["dhash"]),
                        int(row["width"]),
                        int(row["height"]),
                    )
                ),
                None,
            )
            if matched is None:
                connection.rollback()
                return "image_not_found", False

            asset_id = str(matched["asset_id"])
            connection.execute(
                """
                DELETE FROM gallery_sent_records
                WHERE session_key = ? AND keyword = ? AND asset_id = ?
                """,
                (session_key, keyword, asset_id),
            )
            connection.execute(
                """
                DELETE FROM gallery_images
                WHERE session_key = ? AND keyword = ? AND asset_id = ?
                """,
                (session_key, keyword, asset_id),
            )
            connection.execute(
                """
                UPDATE image_assets
                SET ref_count = MAX(0, ref_count - 1)
                WHERE session_key = ? AND asset_id = ?
                """,
                (session_key, asset_id),
            )
            current = connection.execute(
                """
                SELECT rel_path, ref_count FROM image_assets
                WHERE session_key = ? AND asset_id = ?
                """,
                (session_key, asset_id),
            ).fetchone()
            if current is not None and int(current["ref_count"]) <= 0:
                file_to_remove = self.root / str(current["rel_path"])
                connection.execute(
                    "DELETE FROM image_assets WHERE session_key = ? AND asset_id = ?",
                    (session_key, asset_id),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        removed_file = False
        if file_to_remove is not None:
            try:
                file_to_remove.unlink(missing_ok=True)
                removed_file = True
            except OSError as exc:
                logger.info(f"删除单张图库资源失败: path={file_to_remove}, error={exc}")
        return "deleted", removed_file

    async def random_gallery_image(
        self,
        session_key: str,
        message_text: str,
    ) -> tuple[str, ImageAsset] | None:
        if not session_key or not str(message_text or ""):
            return None
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._random_gallery_image_sync,
                session_key,
                str(message_text),
            )

    def _random_gallery_image_sync(
        self,
        session_key: str,
        message_text: str,
    ) -> tuple[str, ImageAsset] | None:
        text = str(message_text or "").strip()
        if not session_key or not text:
            return None
        connection = self._connect()
        try:
            # Serialize gallery selection and recent-history replacement across instances.
            connection.execute("BEGIN IMMEDIATE")
            keyword = text
            if connection.execute(
                """
                SELECT 1 FROM gallery_images
                WHERE session_key = ? AND keyword = ?
                LIMIT 1
                """,
                (session_key, keyword),
            ).fetchone() is None:
                connection.rollback()
                return None

            total = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM gallery_images
                    WHERE session_key = ? AND keyword = ?
                    """,
                    (session_key, keyword),
                ).fetchone()[0]
            )
            if total <= 0:
                connection.rollback()
                return None

            recent_limit = min(max(0, total - 1), GALLERY_RECENT_WINDOW)
            recent_ids: list[str] = []
            if recent_limit:
                recent_ids = [
                    str(row["asset_id"])
                    for row in connection.execute(
                        """
                        SELECT asset_id, MAX(id) AS latest_id
                        FROM gallery_sent_records
                        WHERE session_key = ? AND keyword = ?
                        GROUP BY asset_id
                        ORDER BY latest_id DESC
                        LIMIT ?
                        """,
                        (session_key, keyword, recent_limit),
                    ).fetchall()
                ]

            exclusion_sql = ""
            candidate_values: list[Any] = [session_key, keyword]
            if recent_ids:
                placeholders = ",".join("?" for _ in recent_ids)
                exclusion_sql = f" AND gallery_images.asset_id NOT IN ({placeholders})"
                candidate_values.extend(recent_ids)

            candidate_count = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM gallery_images
                    WHERE session_key = ? AND keyword = ?{exclusion_sql}
                    """,
                    candidate_values,
                ).fetchone()[0]
            )
            if candidate_count <= 0:
                connection.rollback()
                return None

            offset = secrets.randbelow(candidate_count)
            row = connection.execute(
                f"""
                SELECT image_assets.*
                FROM gallery_images
                JOIN image_assets USING (asset_id)
                WHERE gallery_images.session_key = ?
                  AND gallery_images.keyword = ?{exclusion_sql}
                ORDER BY gallery_images.random_key
                LIMIT 1 OFFSET ?
                """,
                (*candidate_values, offset),
            ).fetchone()
            if row is None:
                connection.rollback()
                return None

            asset_id = str(row["asset_id"])
            connection.execute(
                """
                INSERT INTO gallery_sent_records (
                    session_key, keyword, asset_id, sent_at
                ) VALUES (?, ?, ?, ?)
                """,
                (session_key, keyword, asset_id, time()),
            )
            connection.execute(
                """
                DELETE FROM gallery_sent_records
                WHERE session_key = ? AND keyword = ? AND id NOT IN (
                    SELECT id FROM gallery_sent_records
                    WHERE session_key = ? AND keyword = ?
                    ORDER BY id DESC
                    LIMIT ?
                )
                """,
                (
                    session_key,
                    keyword,
                    session_key,
                    keyword,
                    MAX_GALLERY_SENT_RECORDS,
                ),
            )
            connection.commit()
            return keyword, self._image_from_row(row)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _insert_media_asset(self, connection: sqlite3.Connection, session_key: str, asset: MediaAsset, *, ignore: bool = False) -> None:
        clause = "OR IGNORE " if ignore else ""
        connection.execute(
            f"""
            INSERT {clause}INTO media_assets (
                asset_id, session_key, media_type, file_name, rel_path,
                display_name, ref_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asset.asset_id,
                session_key,
                asset.media_type,
                asset.file_name,
                asset.rel_path,
                asset.display_name,
                asset.ref_count,
                asset.created_at,
            ),
        )

    async def create_quote_with_segments(
        self,
        session_key: str,
        quote: Quote,
        segments: list[PendingQuoteSegment],
    ) -> CreateQuoteResult:
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._create_quote_with_segments_sync,
                session_key,
                quote,
                segments,
            )

    def _create_quote_with_segments_sync(
        self,
        session_key: str,
        quote: Quote,
        segments: list[PendingQuoteSegment],
    ) -> CreateQuoteResult:
        store = self.get_store(session_key)
        created_files: list[Path] = []
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            images = [
                segment.image
                for segment in segments
                if segment.type == "image" and segment.image is not None
            ]
            if self._has_duplicate(images, []):
                connection.rollback()
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_IMAGE_MESSAGE)

            persisted_segments: list[QuoteSegment] = []
            created_assets: list[ImageAsset] = []
            referenced_assets: list[ImageAsset] = []
            existing_assets = self._list_image_assets(connection, session_key)
            created_at = time()
            for segment in segments:
                if segment.type == "text":
                    text = str(segment.text or "").strip()
                    if text:
                        persisted_segments.append(QuoteSegment(type="text", text=text))
                    continue
                if segment.type != "image" or segment.image is None:
                    continue
                asset = self._find_matching_image_asset(segment.image, existing_assets)
                if asset is not None:
                    connection.execute(
                        "UPDATE image_assets SET ref_count = ref_count + 1 WHERE asset_id = ?",
                        (asset.asset_id,),
                    )
                else:
                    asset = self._persist_image_asset(
                        store,
                        segment.image,
                        created_at=created_at,
                        created_files=created_files,
                    )
                    created_assets.append(asset)
                    existing_assets.append(asset)
                referenced_assets.append(asset)
                persisted_segments.append(QuoteSegment(type="image", asset_id=asset.asset_id))

            quote.kind = "standard"
            quote.group = session_key
            quote.forward_nodes = []
            quote.segments = persisted_segments
            quote.image_ids = [asset.asset_id for asset in referenced_assets]
            quote.media_ids = []
            if not quote.text:
                quote.text = " ".join(
                    segment.text for segment in persisted_segments if segment.type == "text"
                ).strip()

            for asset in created_assets:
                self._insert_image_asset(connection, session_key, asset)
            self._insert_quote(connection, quote)
            connection.commit()
            return CreateQuoteResult(quote=quote)
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            for path in created_files:
                path.unlink(missing_ok=True)
            if quote.content_fingerprint and "quotes.session_key, quotes.qq, quotes.content_fingerprint" in str(exc):
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_QUOTE_MESSAGE)
            raise
        except Exception:
            connection.rollback()
            for path in created_files:
                path.unlink(missing_ok=True)
            raise
        finally:
            connection.close()

    async def create_quote_with_forward_nodes(
        self,
        session_key: str,
        quote: Quote,
        nodes: list[PendingForwardNode],
    ) -> CreateQuoteResult:
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._create_quote_with_forward_nodes_sync,
                session_key,
                quote,
                nodes,
            )

    def _create_quote_with_forward_nodes_sync(
        self,
        session_key: str,
        quote: Quote,
        nodes: list[PendingForwardNode],
    ) -> CreateQuoteResult:
        store = self.get_store(session_key)
        created_files: list[Path] = []
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            images = self._collect_pending_forward_images(nodes)
            if self._has_duplicate(images, []):
                connection.rollback()
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_IMAGE_MESSAGE)

            created_image_assets: list[ImageAsset] = []
            created_media_assets: list[MediaAsset] = []
            existing_assets = self._list_image_assets(connection, session_key)

            def reuse_image(image: PreparedImage) -> ImageAsset | None:
                asset = self._find_matching_image_asset(image, existing_assets)
                if asset is not None:
                    connection.execute(
                        "UPDATE image_assets SET ref_count = ref_count + 1 WHERE asset_id = ?",
                        (asset.asset_id,),
                    )
                return asset

            persisted_nodes, image_ids, media_ids = self._persist_forward_nodes(
                store,
                nodes,
                created_image_assets=created_image_assets,
                created_media_assets=created_media_assets,
                created_files=created_files,
                created_at=time(),
                reuse_image=reuse_image,
            )
            quote.kind = "forward"
            quote.group = session_key
            quote.segments = []
            quote.forward_nodes = persisted_nodes
            quote.image_ids = image_ids
            quote.media_ids = media_ids
            if not quote.text:
                quote.text = self._flatten_forward_nodes(persisted_nodes)

            for asset in created_image_assets:
                self._insert_image_asset(connection, session_key, asset)
            for asset in created_media_assets:
                self._insert_media_asset(connection, session_key, asset)
            self._insert_quote(connection, quote)
            connection.commit()
            return CreateQuoteResult(quote=quote)
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            for path in created_files:
                path.unlink(missing_ok=True)
            if quote.content_fingerprint and "quotes.session_key, quotes.qq, quotes.content_fingerprint" in str(exc):
                return CreateQuoteResult(duplicate=True, message=DUPLICATE_QUOTE_MESSAGE)
            raise
        except Exception:
            connection.rollback()
            for path in created_files:
                path.unlink(missing_ok=True)
            raise
        finally:
            connection.close()

    async def random_quote(
        self,
        session_key: str | None = None,
        qq: str | None = None,
        *,
        history_session_key: str | None = None,
    ) -> Quote | None:
        history_key = history_session_key or session_key or "__global__"
        async with self._db_write_lock:
            return await asyncio.to_thread(
                self._random_quote_sync,
                session_key,
                qq,
                history_key,
            )

    def _random_quote_sync(
        self,
        session_key: str | None,
        qq: str | None,
        history_session_key: str,
    ) -> Quote | None:
        conditions: list[str] = []
        values: list[Any] = []
        if session_key is not None:
            conditions.append("session_key = ?")
            values.append(session_key)
        if qq:
            conditions.append("qq = ?")
            values.append(str(qq))
        base_where = f" WHERE {' AND '.join(conditions)}" if conditions else ""

        connection = self._connect()
        try:
            # Serialize quote selection and state replacement across repository instances.
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute(
                "SELECT quote_id FROM quote_random_state WHERE session_key = ?",
                (history_session_key,),
            ).fetchone()
            last_quote_id = str(state["quote_id"]) if state is not None else ""

            candidate_where = base_where
            candidate_values = list(values)
            if last_quote_id:
                candidate_where = (
                    f"{base_where}{' AND' if base_where else ' WHERE'} id <> ?"
                )
                candidate_values.append(last_quote_id)

            candidate_count = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM quotes{candidate_where}",
                    candidate_values,
                ).fetchone()[0]
            )
            if candidate_count == 0 and last_quote_id:
                candidate_where = base_where
                candidate_values = list(values)
                candidate_count = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM quotes{candidate_where}",
                        candidate_values,
                    ).fetchone()[0]
                )
            if candidate_count == 0:
                connection.rollback()
                return None

            offset = secrets.randbelow(candidate_count)
            row = connection.execute(
                f"""
                SELECT * FROM quotes{candidate_where}
                ORDER BY random_key, id
                LIMIT 1 OFFSET ?
                """,
                (*candidate_values, offset),
            ).fetchone()
            if row is None:
                connection.rollback()
                return None

            connection.execute(
                """
                INSERT INTO quote_random_state (session_key, quote_id, selected_at)
                VALUES (?, ?, ?)
                ON CONFLICT(session_key) DO UPDATE SET
                    quote_id = excluded.quote_id,
                    selected_at = excluded.selected_at
                """,
                (history_session_key, str(row["id"]), time()),
            )
            connection.commit()
            return self._row_to_quote(row)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    async def record_sent_quote(
        self,
        session_key: str,
        *,
        quote_id: str,
        fingerprint: str,
        sent_at: float,
        image_signatures: list[ImageSignature] | None = None,
    ) -> None:
        if not quote_id or not fingerprint:
            return
        async with self._db_write_lock:
            await asyncio.to_thread(
                self._record_sent_quote_sync,
                session_key,
                quote_id,
                fingerprint,
                sent_at,
                image_signatures or [],
            )

    def _record_sent_quote_sync(
        self,
        session_key: str,
        quote_id: str,
        fingerprint: str,
        sent_at: float,
        image_signatures: list[ImageSignature],
    ) -> None:
        signatures = [
            item.to_dict() if isinstance(item, ImageSignature) else dict(item)
            for item in image_signatures
            if isinstance(item, (ImageSignature, dict))
        ]
        with self._connection() as connection:
            if connection.execute("SELECT 1 FROM quotes WHERE id = ?", (quote_id,)).fetchone() is None:
                return
            connection.execute(
                """
                INSERT INTO sent_records (
                    session_key, quote_id, fingerprint, sent_at, image_signatures_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (session_key, quote_id, fingerprint, sent_at, _json_dumps(signatures)),
            )
            connection.execute(
                """
                DELETE FROM sent_records
                WHERE session_key = ? AND id NOT IN (
                    SELECT id FROM sent_records
                    WHERE session_key = ?
                    ORDER BY sent_at DESC, id DESC
                    LIMIT ?
                )
                """,
                (session_key, session_key, MAX_SENT_RECORDS),
            )
            connection.commit()

    def find_sent_quote_id(
        self,
        session_key: str,
        *,
        fingerprint: str,
        replied_at: float = 0.0,
    ) -> str | None:
        if not fingerprint:
            return None
        with self._connection() as connection:
            row = None
            if replied_at > 0:
                row = connection.execute(
                    """
                    SELECT quote_id FROM sent_records
                    WHERE session_key = ? AND fingerprint = ? AND sent_at <= ?
                    ORDER BY sent_at DESC, id DESC LIMIT 1
                    """,
                    (session_key, fingerprint, replied_at),
                ).fetchone()
            if row is None:
                row = connection.execute(
                    """
                    SELECT quote_id FROM sent_records
                    WHERE session_key = ? AND fingerprint = ?
                    ORDER BY sent_at DESC, id DESC LIMIT 1
                    """,
                    (session_key, fingerprint),
                ).fetchone()
        return str(row["quote_id"]) if row is not None else None

    def find_sent_quote_id_by_image_signature(
        self,
        session_key: str,
        *,
        image: PreparedImage,
        replied_at: float = 0.0,
    ) -> str | None:
        if image is None:
            return None
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT quote_id, sent_at, image_signatures_json
                FROM sent_records WHERE session_key = ?
                ORDER BY sent_at DESC, id DESC
                """,
                (session_key,),
            ).fetchall()
        matches: list[tuple[str, float]] = []
        for row in rows:
            raw_signatures = _json_loads(row["image_signatures_json"], [])
            if len(raw_signatures) != 1:
                continue
            signature = ImageSignature.from_dict(raw_signatures[0])
            if is_near_duplicate(
                image,
                signature.sha256,
                signature.dhash,
                signature.width,
                signature.height,
            ):
                matches.append((str(row["quote_id"]), float(row["sent_at"])))
        if not matches:
            return None
        if replied_at > 0:
            bounded = [item for item in matches if item[1] <= replied_at]
            if bounded:
                matches = bounded
        matches.sort(key=lambda item: item[1], reverse=True)
        return matches[0][0]

    async def delete_quote(self, quote_id: str) -> bool:
        async with self._db_write_lock:
            return await asyncio.to_thread(self._delete_quote_sync, quote_id)

    def _delete_quote_sync(self, quote_id: str) -> bool:
        connection = self._connect()
        files_to_remove: list[Path] = []
        cache_paths: list[Path] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM quotes WHERE id = ?", (quote_id,)).fetchone()
            if row is None:
                connection.rollback()
                return False
            quote = self._row_to_quote(row)
            cache_paths = self.get_store(quote.group).cache_paths(quote_id)

            for asset_id, count in Counter(quote.image_ids).items():
                connection.execute(
                    "UPDATE image_assets SET ref_count = MAX(0, ref_count - ?) WHERE asset_id = ?",
                    (count, asset_id),
                )
            for asset_id, count in Counter(quote.media_ids).items():
                connection.execute(
                    "UPDATE media_assets SET ref_count = MAX(0, ref_count - ?) WHERE asset_id = ?",
                    (count, asset_id),
                )

            image_rows = connection.execute(
                "SELECT rel_path FROM image_assets WHERE session_key = ? AND ref_count <= 0",
                (quote.group,),
            ).fetchall()
            media_rows = connection.execute(
                "SELECT rel_path FROM media_assets WHERE session_key = ? AND ref_count <= 0",
                (quote.group,),
            ).fetchall()
            files_to_remove.extend(self.root / str(item["rel_path"]) for item in image_rows)
            files_to_remove.extend(self.root / str(item["rel_path"]) for item in media_rows)
            connection.execute(
                "DELETE FROM image_assets WHERE session_key = ? AND ref_count <= 0",
                (quote.group,),
            )
            connection.execute(
                "DELETE FROM media_assets WHERE session_key = ? AND ref_count <= 0",
                (quote.group,),
            )
            connection.execute("DELETE FROM quotes WHERE id = ?", (quote_id,))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        for path in files_to_remove:
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                logger.info(f"删除语录资源文件失败: path={path}, error={exc}")
        for cache_path in cache_paths:
            try:
                cache_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.info(f"删除语录渲染缓存失败: path={cache_path}, error={exc}")
        return True

    async def migrate_legacy_data(self) -> bool:
        migrated_root = False
        legacy_root_file = self.root / QUOTES_FILENAME
        if legacy_root_file.exists():
            try:
                json.loads(legacy_root_file.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.error(f"旧语录 JSON 无法解析，已保留原文件: {legacy_root_file}, error={exc}")
            else:
                migrated_root = await super().migrate_legacy_data()

        async with self._db_write_lock:
            imported = await asyncio.to_thread(self._import_session_json_files)
        return migrated_root or imported > 0

    def _import_session_json_files(self) -> int:
        if not self.groups_dir.exists():
            return 0
        imported_quotes = 0
        for session_dir in sorted(path for path in self.groups_dir.iterdir() if path.is_dir()):
            source_files = [
                session_dir / QUOTES_FILENAME,
                session_dir / IMAGE_INDEX_FILENAME,
                session_dir / MEDIA_INDEX_FILENAME,
                session_dir / SENT_INDEX_FILENAME,
            ]
            existing_files = [path for path in source_files if path.exists()]
            if not existing_files:
                continue
            payloads: dict[str, dict[str, Any]] = {}
            try:
                for path in existing_files:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    if not isinstance(payload, dict):
                        raise ValueError("顶层 JSON 必须是对象")
                    payloads[path.name] = payload
            except Exception as exc:
                logger.error(f"跳过损坏的会话 JSON，原文件未改动: session={session_dir.name}, error={exc}")
                continue

            quotes = [
                Quote.from_dict(item)
                for item in (payloads.get(QUOTES_FILENAME, {}).get("quotes") or [])
                if isinstance(item, dict)
            ]
            images = [
                ImageAsset.from_dict(item)
                for item in (payloads.get(IMAGE_INDEX_FILENAME, {}).get("images") or [])
                if isinstance(item, dict)
            ]
            media = [
                MediaAsset.from_dict(item)
                for item in (payloads.get(MEDIA_INDEX_FILENAME, {}).get("media") or [])
                if isinstance(item, dict)
            ]
            sent = [
                SentQuoteRecord.from_dict(item)
                for item in (payloads.get(SENT_INDEX_FILENAME, {}).get("sent") or [])
                if isinstance(item, dict)
            ]

            session_key = session_dir.name
            session_imported_quotes = 0
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                for asset in images:
                    self._insert_image_asset(connection, session_key, asset, ignore=True)
                for asset in media:
                    self._insert_media_asset(connection, session_key, asset, ignore=True)
                for quote in quotes:
                    quote.group = session_key
                    before = connection.total_changes
                    self._insert_quote(connection, quote, ignore=True)
                    if connection.total_changes > before:
                        session_imported_quotes += 1
                valid_image_ids = {
                    str(row["asset_id"])
                    for row in connection.execute(
                        "SELECT asset_id FROM image_assets WHERE session_key = ?",
                        (session_key,),
                    ).fetchall()
                }
                valid_media_ids = {
                    str(row["asset_id"])
                    for row in connection.execute(
                        "SELECT asset_id FROM media_assets WHERE session_key = ?",
                        (session_key,),
                    ).fetchall()
                }
                valid_quote_ids = {
                    str(row["id"])
                    for row in connection.execute(
                        "SELECT id FROM quotes WHERE session_key = ?",
                        (session_key,),
                    ).fetchall()
                }
                missing_quotes = {quote.id for quote in quotes} - valid_quote_ids
                missing_images = {asset.asset_id for asset in images} - valid_image_ids
                missing_media = {asset.asset_id for asset in media} - valid_media_ids
                if missing_quotes or missing_images or missing_media:
                    raise RuntimeError(
                        "迁移校验失败: "
                        f"quotes={len(missing_quotes)}, images={len(missing_images)}, "
                        f"media={len(missing_media)}"
                    )
                for record in sent:
                    if record.quote_id not in valid_quote_ids or not record.fingerprint:
                        continue
                    exists = connection.execute(
                        """
                        SELECT 1 FROM sent_records
                        WHERE session_key = ? AND quote_id = ? AND fingerprint = ? AND sent_at = ?
                        LIMIT 1
                        """,
                        (session_key, record.quote_id, record.fingerprint, record.sent_at),
                    ).fetchone()
                    if exists is not None:
                        continue
                    connection.execute(
                        """
                        INSERT INTO sent_records (
                            session_key, quote_id, fingerprint, sent_at, image_signatures_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            session_key,
                            record.quote_id,
                            record.fingerprint,
                            record.sent_at,
                            _json_dumps([item.to_dict() for item in record.image_signatures]),
                        ),
                    )
                connection.execute(
                    """
                    DELETE FROM sent_records
                    WHERE session_key = ? AND id NOT IN (
                        SELECT id FROM sent_records
                        WHERE session_key = ?
                        ORDER BY sent_at DESC, id DESC
                        LIMIT ?
                    )
                    """,
                    (session_key, session_key, MAX_SENT_RECORDS),
                )
                connection.commit()
                imported_quotes += session_imported_quotes
            except Exception as exc:
                connection.rollback()
                logger.error(f"迁移会话 JSON 失败，原文件未改动: session={session_key}, error={exc}")
                continue
            finally:
                connection.close()

            for path in existing_files:
                self._backup_migrated_json(path)
            logger.info(
                f"已迁移会话 JSON 到 SQLite: session={session_key}, quotes={len(quotes)}, "
                f"images={len(images)}, media={len(media)}, sent={len(sent)}"
            )
        return imported_quotes

    def _backup_migrated_json(self, path: Path) -> None:
        target = path.with_name(path.name + ".migrated.bak")
        if target.exists():
            target = path.with_name(path.name + f".migrated.{int(time() * 1000)}.bak")
        try:
            path.replace(target)
        except OSError as exc:
            logger.warning(f"迁移成功但备份 JSON 重命名失败: path={path}, error={exc}")


# Main entry points can use the concise historical name without changing service APIs.
QuoteRepository = SQLiteQuoteRepository

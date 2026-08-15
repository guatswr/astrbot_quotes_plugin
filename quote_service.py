from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path
from typing import Any

try:
    from astrbot.api import logger
    import astrbot.api.message_components as Comp
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)
    Comp = None  # type: ignore

try:
    from .constants import DUPLICATE_IMAGE_MESSAGE, DUPLICATE_QUOTE_MESSAGE, UPLOAD_SUCCESS_PROMPT
    from .models import (
        CommandResponse,
        ForwardNode,
        ForwardSegment,
        ImageSignature,
        Quote,
        collect_forward_asset_ids,
    )
    from .napcat_service import NapcatService
    from .renderer import QuoteRenderer
    from .sqlite_store import QuoteRepository
    from .utils import (
        is_valid_qq,
        make_session_key,
        normalize_image_file_for_send,
        normalize_quote_text,
        random_id,
        sha256_bytes,
    )
except ImportError:  # pragma: no cover
    from constants import DUPLICATE_IMAGE_MESSAGE, DUPLICATE_QUOTE_MESSAGE, UPLOAD_SUCCESS_PROMPT
    from models import (
        CommandResponse,
        ForwardNode,
        ForwardSegment,
        ImageSignature,
        Quote,
        collect_forward_asset_ids,
    )
    from napcat_service import NapcatService
    from renderer import QuoteRenderer
    from sqlite_store import QuoteRepository
    from utils import (
        is_valid_qq,
        make_session_key,
        normalize_image_file_for_send,
        normalize_quote_text,
        random_id,
        sha256_bytes,
    )


class QuoteService:
    def __init__(
        self,
        repository: QuoteRepository,
        image_service: Any,
        napcat_service: NapcatService,
        renderer: QuoteRenderer,
        http_client: Any | None,
        *,
        global_mode: bool,
        text_mode: bool,
        render_cache: bool,
        image_signature_use_group: bool,
        blacklist: set[str],
        render_wait_timeout: float = 0.8,
    ):
        self.repository = repository
        self.image_service = image_service
        self.napcat_service = napcat_service
        self.renderer = renderer
        self.http_client = http_client
        self.global_mode = global_mode
        self.text_mode = text_mode
        self.render_cache = render_cache
        self.render_wait_timeout = max(0.0, min(5.0, float(render_wait_timeout)))
        self.image_signature_use_group = image_signature_use_group
        self.blacklist = blacklist
        self._render_semaphore = asyncio.Semaphore(1)
        self._render_tasks: set[asyncio.Task[bool]] = set()
        self._render_inflight: dict[tuple[str, str], asyncio.Task[bool]] = {}
        self._startup_pre_render_task: asyncio.Task[None] | None = None
        self._skip_startup_prewarm_sessions: set[str] = set()

    async def add_quote(self, event: Any, uid: str = "") -> CommandResponse:
        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        reply_message_id = self.get_reply_message_id(event)
        reply_payload = await self.napcat_service.fetch_onebot_message(event, reply_message_id)
        reply_sender = reply_payload.get("sender") or {}
        reply_qq = str(reply_sender.get("user_id") or reply_sender.get("qq") or "")
        mention_qq = self.extract_at_qq(event) or ""
        raw_target = str(uid or "").strip()
        explicit_qq = raw_target if is_valid_qq(raw_target) else ""
        gallery_keyword = raw_target if raw_target and not explicit_qq and not mention_qq else ""

        if gallery_keyword:
            return await self._add_gallery_images(
                event,
                session_key=session_key,
                keyword=gallery_keyword,
                reply_message=reply_payload.get("message"),
            )

        forward_id, forward_payload = self.napcat_service.extract_forward_reference(reply_payload.get("message"))
        if forward_id or forward_payload:
            return await self._add_forward_quote(
                event,
                session_key=session_key,
                reply_qq=reply_qq,
                explicit_qq=explicit_qq,
                mention_qq=mention_qq,
                forward_id=forward_id,
                forward_payload=forward_payload,
            )

        reply_segments = await self.image_service.build_reply_segments(event, reply_payload.get("message"))
        current_segments = await self.image_service.build_current_segments(
            event,
            command_name="上传",
            explicit_qq=explicit_qq,
        )
        all_segments = self._normalize_pending_segments([*reply_segments, *current_segments])
        if not all_segments:
            logger.info(f"上传语录失败: 未获取到可收录内容, session={session_key}, sender={event.get_sender_id()}")
            return CommandResponse(kind="plain", text="未获取到被回复消息内容或图片，请确认已正确回复对方的消息或附带图片。")

        if explicit_qq:
            target_qq = explicit_qq
        elif mention_qq:
            target_qq = mention_qq
        elif reply_qq:
            target_qq = reply_qq
        elif current_segments:
            target_qq = str(event.get_sender_id())
        else:
            target_qq = ""

        if target_qq and target_qq in self.blacklist:
            logger.info(f"上传语录已忽略: 目标用户在黑名单中, session={session_key}, target_qq={target_qq}")
            return CommandResponse(kind="plain", text="该用户在语录黑名单中，本次语录已忽略。")

        target_name = await self.napcat_service.resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time

        duplicate_fingerprint = self._fingerprint_pending_standard_segments(all_segments)
        if target_qq and duplicate_fingerprint and self._has_duplicate_quote(
            session_key,
            target_qq=target_qq,
            fingerprint=duplicate_fingerprint,
        ):
            logger.info(f"上传语录被拒绝: 内容重复, session={session_key}, target_qq={target_qq}")
            return CommandResponse(kind="plain", text=DUPLICATE_QUOTE_MESSAGE)

        quote = Quote(
            id=random_id("q_"),
            qq=str(target_qq or ""),
            name=str(target_name),
            text=self._plain_text_from_pending_segments(all_segments),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            group=session_key,
            content_fingerprint=duplicate_fingerprint,
        )
        result = await self.repository.create_quote_with_segments(session_key, quote, all_segments)
        if result.duplicate:
            logger.info(
                f"上传语录被拒绝: {result.message or '内容重复'}, "
                f"session={session_key}, target_qq={target_qq}"
            )
            return CommandResponse(kind="plain", text=result.message or DUPLICATE_IMAGE_MESSAGE)

        bindings = []
        if target_qq:
            bindings = (
                self.repository.list_bindings_for_qq_global(target_qq)
                if self.global_mode
                else [self.repository.get_binding_for_qq(session_key, target_qq)]
            )
        bindings = [binding for binding in bindings if binding is not None]
        for binding in bindings:
            self.schedule_pre_render(event, quote, signature_override=binding.tag)
        if self.global_mode or not bindings:
            self.schedule_pre_render(event, quote)

        image_count = len([segment for segment in all_segments if segment.type == "image"])
        logger.info(
            "上传语录成功: "
            f"quote_id={quote.id}, session={session_key}, target_qq={target_qq}, "
            f"segments={len(all_segments)}, images={image_count}"
        )
        return CommandResponse(
            kind="plain",
            text=self._upload_success_text(),
        )

    async def _add_gallery_images(
        self,
        event: Any,
        *,
        session_key: str,
        keyword: str,
        reply_message: Any,
    ) -> CommandResponse:
        error = self._gallery_keyword_error(keyword)
        if error:
            return CommandResponse(kind="plain", text=error)

        reply_segments = await self.image_service.build_reply_segments(event, reply_message)
        current_segments = await self.image_service.build_current_segments(
            event,
            command_name="上传",
            explicit_qq=keyword,
        )
        images = [
            segment.image
            for segment in [*reply_segments, *current_segments]
            if segment.type == "image" and segment.image is not None
        ]
        if not images:
            return CommandResponse(
                kind="plain",
                text="请回复或附带图片后使用：/上传 图库关键词",
            )

        added, skipped = await self.repository.add_gallery_images(
            session_key,
            keyword,
            images,
        )
        if added == 0:
            return CommandResponse(kind="plain", text="这个图库里已经有这些图片啦。")
        logger.info(
            "图库图片上传成功: "
            f"session={session_key}, keyword={keyword}, added={added}, skipped={skipped}"
        )
        return CommandResponse(kind="plain", text=UPLOAD_SUCCESS_PROMPT)

    async def _add_forward_quote(
        self,
        event: Any,
        *,
        session_key: str,
        reply_qq: str,
        explicit_qq: str,
        mention_qq: str,
        forward_id: str | None,
        forward_payload: dict[str, Any] | None,
    ) -> CommandResponse:
        nodes = await self.image_service.build_forward_nodes(
            event,
            forward_id=forward_id,
            forward_payload=forward_payload,
            forward_loader=self.napcat_service.fetch_forward_messages,
        )
        if not nodes:
            logger.info(f"上传聊天记录语录失败: 未获取到 forward 节点, session={session_key}, sender={event.get_sender_id()}")
            return CommandResponse(kind="plain", text="未获取到可用的聊天记录内容，请确认回复的是 QQ 合并转发消息。")

        if explicit_qq:
            target_qq = explicit_qq
        elif mention_qq:
            target_qq = mention_qq
        elif reply_qq:
            target_qq = reply_qq
        else:
            target_qq = str(event.get_sender_id())

        if target_qq and target_qq in self.blacklist:
            logger.info(f"上传聊天记录语录已忽略: 目标用户在黑名单中, session={session_key}, target_qq={target_qq}")
            return CommandResponse(kind="plain", text="该用户在语录黑名单中，本次语录已忽略。")

        target_name = await self.napcat_service.resolve_user_name(event, target_qq) if target_qq else ""
        if not target_name:
            target_name = target_qq or "未知用户"

        from time import time

        duplicate_fingerprint = self._fingerprint_pending_forward_nodes(nodes)
        if target_qq and duplicate_fingerprint and self._has_duplicate_quote(
            session_key,
            target_qq=target_qq,
            fingerprint=duplicate_fingerprint,
        ):
            logger.info(f"上传聊天记录语录被拒绝: 内容重复, session={session_key}, target_qq={target_qq}")
            return CommandResponse(kind="plain", text=DUPLICATE_QUOTE_MESSAGE)

        quote = Quote(
            id=random_id("q_"),
            qq=str(target_qq or ""),
            name=str(target_name),
            text=self._flatten_forward_nodes(nodes),
            created_by=str(event.get_sender_id()),
            created_at=time(),
            group=session_key,
            kind="forward",
            content_fingerprint=duplicate_fingerprint,
        )
        result = await self.repository.create_quote_with_forward_nodes(session_key, quote, nodes)
        if result.duplicate:
            logger.info(
                f"上传聊天记录语录被拒绝: {result.message or '内容重复'}, "
                f"session={session_key}, target_qq={target_qq}"
            )
            return CommandResponse(kind="plain", text=result.message or DUPLICATE_IMAGE_MESSAGE)

        message_count = self._count_forward_messages(nodes)
        logger.info(
            "上传聊天记录语录成功: "
            f"quote_id={quote.id}, session={session_key}, target_qq={target_qq}, messages={message_count}"
        )
        return CommandResponse(
            kind="plain",
            text=self._upload_success_text(),
        )

    async def random_quote(
        self,
        event: Any,
        uid: str = "",
        silent_if_empty: bool = False,
        signature_override: str = "",
    ) -> CommandResponse | None:
        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        target_session = None if self.global_mode else session_key
        explicit_qq = uid.strip() if is_valid_qq(uid) else ""
        only_qq = explicit_qq or (self.extract_at_qq(event) or "")
        quote = await self.repository.random_quote(
            target_session,
            qq=only_qq or None,
            history_session_key=session_key,
        )
        if quote is None:
            if not silent_if_empty:
                logger.info(
                    "随机语录未命中: "
                    f"session={target_session or 'global'}, target_qq={only_qq or '*'}"
                )
            if silent_if_empty:
                return None
            if only_qq:
                text = (
                    "我的记忆库里还没有这位的语录。再教我一点吧！"
                    if self.global_mode
                    else "这个会话的记忆库里还没有这位的语录。再教我一点吧！"
                )
            else:
                text = (
                    "我的记忆库还是空的，先用 /上传 教教我吧！"
                    if self.global_mode
                    else "这个会话的记忆库还是空的，先用 /上传 教教我吧！"
                )
            return CommandResponse(kind="plain", text=text)

        effective_signature = signature_override
        if not effective_signature and quote.qq:
            binding = self.repository.get_binding_for_qq(session_key, quote.qq)
            if binding is not None:
                effective_signature = binding.tag

        logger.info(f"随机语录命中: quote_id={quote.id}, session={quote.group}, target_qq={only_qq or '*'}")
        chain = await self.build_quote_chain(quote, effective_signature)
        if chain:
            return CommandResponse(
                kind="chain",
                chain=chain,
                quote_id=quote.id,
                delete_fingerprint=await self.build_delete_fingerprint(quote, chain=chain),
                delete_image_signatures=self.build_delete_image_signatures(quote),
            )

        if self.text_mode or quote.kind == "forward" or self.renderer.should_fallback_to_plain(quote):
            return self._plain_quote_response(quote, effective_signature)

        store = self.repository.get_store(quote.group)
        cache_path = store.cache_path(quote.id, effective_signature)
        if self.render_cache and cache_path.exists():
            return CommandResponse(
                kind="image_path",
                path=str(cache_path),
                quote_id=quote.id,
                delete_fingerprint=await self._fingerprint_image_path(cache_path),
            )

        if not self.render_cache:
            signature = effective_signature or await self.napcat_service.resolve_signature_name(
                event, quote, use_group_signature=self.image_signature_use_group
            )
            rendered_url = await self.renderer.render_quote_image(quote, signature)
            return CommandResponse(
                kind="image_url",
                url=rendered_url,
                quote_id=quote.id,
                delete_fingerprint=await self._fingerprint_image_url(rendered_url),
            )

        render_task = self._get_or_create_render_task(
            event,
            quote,
            signature_override=effective_signature,
        )
        try:
            cached = await asyncio.wait_for(
                asyncio.shield(render_task),
                timeout=self.render_wait_timeout,
            )
        except asyncio.TimeoutError:
            logger.info(
                f"语录冷渲染超过等待预算，回退头像文本消息: quote_id={quote.id}, "
                f"timeout={self.render_wait_timeout}s"
            )
            return self._plain_quote_response(quote, effective_signature)
        if cached:
            return CommandResponse(
                kind="image_path",
                path=str(cache_path),
                quote_id=quote.id,
                delete_fingerprint=await self._fingerprint_image_path(cache_path),
            )
        return self._plain_quote_response(quote, effective_signature)

    def _upload_success_text(self) -> str:
        return UPLOAD_SUCCESS_PROMPT

    def _gallery_keyword_error(self, keyword: str) -> str:
        if not str(keyword or "").strip():
            return "图库关键词不能为空。"
        if "\n" in keyword or "\r" in keyword:
            return "图库关键词不能包含换行。"
        if len(keyword) > 64:
            return "图库关键词不能超过 64 个字符。"
        if keyword.startswith("/"):
            return "图库关键词不能以 / 开头。"
        return ""

    async def delete_quote(self, quote_id: str) -> bool:
        tasks = [
            task
            for (task_quote_id, _), task in self._render_inflight.items()
            if task_quote_id == quote_id and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return await self.repository.delete_quote(quote_id)

    def schedule_pre_render(
        self,
        event: Any,
        quote: Quote,
        *,
        signature_override: str = "",
        resolved_signature: str = "",
    ) -> None:
        if not self._should_render_quote(quote):
            return
        self._get_or_create_render_task(
            event,
            quote,
            signature_override=signature_override,
            resolved_signature=resolved_signature,
        )

    def schedule_startup_pre_render(self) -> None:
        if self.text_mode or not self.render_cache:
            return
        existing = self._startup_pre_render_task
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(
            self._pre_render_existing_quotes(),
            name="quotes-startup-pre-render",
        )
        self._startup_pre_render_task = task
        task.add_done_callback(self._finish_startup_pre_render)

    def _finish_startup_pre_render(self, task: asyncio.Task[None]) -> None:
        if self._startup_pre_render_task is task:
            self._startup_pre_render_task = None
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            logger.warning(f"启动期语录预渲染失败: {exc}")

    async def _pre_render_existing_quotes(self) -> None:
        quotes, bindings = await asyncio.gather(
            asyncio.to_thread(self.repository.list_all_quotes),
            asyncio.to_thread(self.repository.list_all_bindings),
        )
        tags_by_qq: dict[str, set[str]] = {}
        tags_by_session_qq: dict[tuple[str, str], set[str]] = {}
        for binding in bindings:
            tags_by_qq.setdefault(binding.qq, set()).add(binding.tag)
            tags_by_session_qq.setdefault((binding.session_key, binding.qq), set()).add(
                binding.tag
            )

        generated = 0
        for quote in quotes:
            if quote.group in self._skip_startup_prewarm_sessions:
                continue
            if not self._should_render_quote(quote):
                continue

            tags = (
                tags_by_qq.get(quote.qq, set())
                if self.global_mode
                else tags_by_session_qq.get((quote.group, quote.qq), set())
            )
            variants = [(tag, tag) for tag in sorted(tags)]
            if not self.image_signature_use_group and (self.global_mode or not tags):
                variants.append(("", quote.name))
            for cache_variant, resolved_signature in variants:
                if quote.group in self._skip_startup_prewarm_sessions:
                    break
                cache_path = self.repository.get_store(quote.group).cache_path(
                    quote.id,
                    cache_variant,
                )
                if cache_path.exists():
                    continue
                task = self._get_or_create_render_task(
                    None,
                    quote,
                    signature_override=cache_variant,
                    resolved_signature=resolved_signature,
                )
                try:
                    cached = await task
                except Exception as exc:
                    logger.info(
                        f"启动期单条语录预渲染失败: quote_id={quote.id}, error={exc}"
                    )
                    continue
                if cached:
                    generated += 1
                await asyncio.sleep(0)
        logger.info(f"启动期语录预渲染完成: generated={generated}")

    def schedule_binding_pre_render(
        self,
        event: Any,
        session_key: str,
        qq: str,
        tag: str,
    ) -> None:
        quotes = (
            self.repository.list_quotes_for_owner_global(qq)
            if self.global_mode
            else self.repository.list_quotes_for_owner(session_key, qq)
        )
        for quote in quotes:
            self.schedule_pre_render(event, quote, signature_override=tag)

    def schedule_owner_default_pre_render(
        self,
        event: Any,
        session_key: str,
        qq: str,
    ) -> None:
        quotes = (
            self.repository.list_quotes_for_owner_global(qq)
            if self.global_mode
            else self.repository.list_quotes_for_owner(session_key, qq)
        )
        for quote in quotes:
            self.schedule_pre_render(event, quote)

    def _should_render_quote(self, quote: Quote) -> bool:
        if self.text_mode or not self.render_cache or quote.kind != "standard":
            return False
        if self.renderer.should_fallback_to_plain(quote):
            return False
        return not any(
            segment.type == "image" and segment.asset_id
            for segment in quote.segments
        )

    def _get_or_create_render_task(
        self,
        event: Any,
        quote: Quote,
        *,
        signature_override: str = "",
        resolved_signature: str = "",
    ) -> asyncio.Task[bool]:
        task_key = (quote.id, signature_override)
        existing = self._render_inflight.get(task_key)
        if existing is not None and not existing.done():
            return existing
        task = asyncio.create_task(
            self._render_quote_to_cache(
                event,
                quote,
                signature_override=signature_override,
                resolved_signature=resolved_signature,
            ),
            name=f"quotes-pre-render-{quote.id}",
        )
        self._render_inflight[task_key] = task
        self._render_tasks.add(task)
        task.add_done_callback(lambda done, key=task_key: self._finish_render_task(key, done))
        return task

    def _finish_render_task(self, task_key: tuple[str, str], task: asyncio.Task[bool]) -> None:
        self._render_tasks.discard(task)
        if self._render_inflight.get(task_key) is task:
            self._render_inflight.pop(task_key, None)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            logger.warning(f"语录后台预渲染失败: quote_id={task_key[0]}, error={exc}")

    async def _render_quote_to_cache(
        self,
        event: Any,
        quote: Quote,
        *,
        signature_override: str = "",
        resolved_signature: str = "",
    ) -> bool:
        store = self.repository.get_store(quote.group)
        cache_path = store.cache_path(quote.id, signature_override)
        if cache_path.exists():
            return True
        async with self._render_semaphore:
            if cache_path.exists():
                return True
            signature = resolved_signature or signature_override
            if not signature:
                signature = await self.napcat_service.resolve_signature_name(
                    event, quote, use_group_signature=self.image_signature_use_group
                )
            rendered_url = await self.renderer.render_quote_image(quote, signature)
            cached = await self.cache_rendered_result(rendered_url, cache_path)
            if cached:
                logger.info(f"语录后台预渲染完成: quote_id={quote.id}, cache={cache_path}")
            return cached

    async def remove_signature_cache(self, session_key: str, qq: str, signature: str) -> None:
        if not signature:
            return
        quotes = (
            self.repository.list_quotes_for_owner_global(qq)
            if self.global_mode
            else self.repository.list_quotes_for_owner(session_key, qq)
        )
        tasks: list[asyncio.Task[bool]] = []
        for quote in quotes:
            task = self._render_inflight.get((quote.id, signature))
            if task is not None and not task.done():
                task.cancel()
                tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        paths = [
            self.repository.get_store(quote.group).cache_path(quote.id, signature)
            for quote in quotes
        ]

        def remove_paths() -> None:
            for path in paths:
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.info(f"删除旧标签渲染缓存失败: path={path}, error={exc}")

        await asyncio.to_thread(remove_paths)

    async def clear_render_cache(self, session_key: str) -> tuple[int, int]:
        self._skip_startup_prewarm_sessions.add(session_key)
        quote_ids = {quote.id for quote in self.repository.list_quotes(session_key)}
        tasks = [
            task
            for (quote_id, _), task in self._render_inflight.items()
            if quote_id in quote_ids and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        store = self.repository.get_store(session_key)
        return await asyncio.to_thread(store.clear_cache_files)

    async def build_quote_list_text(
        self,
        session_key: str,
        page: int = 1,
        page_size: int = 10,
    ) -> str:
        safe_page = max(1, int(page))
        safe_page_size = max(1, min(50, int(page_size)))
        offset = (safe_page - 1) * safe_page_size
        total, quotes = await asyncio.to_thread(
            self.repository.list_quotes_page,
            session_key,
            limit=safe_page_size,
            offset=offset,
        )
        if total == 0:
            return "这个会话的记忆库还是空的，先用 /上传 教教我吧！"

        total_pages = (total + safe_page_size - 1) // safe_page_size
        if safe_page > total_pages:
            return f"页码超出范围：当前共有 {total_pages} 页、{total} 条语录。"

        lines = [f"当前会话语录（第 {safe_page}/{total_pages} 页，共 {total} 条）："]
        for index, quote in enumerate(quotes, start=offset + 1):
            owner = quote.name or quote.qq or "未知用户"
            owner_text = f"{owner}（{quote.qq}）" if quote.qq and quote.qq != owner else owner
            lines.append(f"{index}. {owner_text}：{self._quote_list_summary(quote)}")
        if safe_page < total_pages:
            lines.append(f"发送 /语录列表 {safe_page + 1} 查看下一页。")
        return "\n".join(lines)

    async def build_quote_ranking_text(self, session_key: str) -> str:
        rankings = await asyncio.to_thread(self.repository.quote_rankings, session_key)
        if not rankings:
            return "当前会话还没有收录语录。"
        lines = ["当前会话语录排名："]
        lines.extend(
            f"{index}. {display_name}：{quote_count} 条"
            for index, (_, display_name, quote_count) in enumerate(rankings, start=1)
        )
        return "\n".join(lines)

    async def random_gallery_response(
        self,
        session_key: str,
        message_text: str,
    ) -> CommandResponse | None:
        selected = await self.repository.random_gallery_image(
            session_key,
            message_text,
        )
        if selected is None:
            return None
        keyword, asset = selected
        abs_path = self.repository.root / asset.rel_path
        try:
            image_bytes = await asyncio.to_thread(
                normalize_image_file_for_send,
                abs_path,
            )
            if Comp is None:
                return None
            return CommandResponse(
                kind="chain",
                chain=[Comp.Image.fromBytes(image_bytes)],
            )
        except Exception as exc:
            logger.warning(
                "图库图片规范化失败: "
                f"session={session_key}, keyword={keyword}, path={abs_path}, error={exc}"
            )
            return CommandResponse(kind="plain", text="[图库图片暂时无法发送]")

    async def delete_gallery(self, session_key: str, keyword: str) -> str:
        normalized_keyword = str(keyword or "").strip()
        error = self._gallery_keyword_error(normalized_keyword)
        if error:
            return "请使用：/图库删除 关键词"
        deleted, removed_files = await self.repository.delete_gallery(
            session_key,
            normalized_keyword,
        )
        if deleted == 0:
            return f'当前会话没有名为“{normalized_keyword}”的图库。'
        logger.info(
            "图库删除成功: "
            f"session={session_key}, keyword={normalized_keyword}, "
            f"images={deleted}, removed_files={removed_files}"
        )
        return f'已删除图库“{normalized_keyword}”，共移除 {deleted} 张图片。'

    async def delete_gallery_image(self, event: Any, keyword: str) -> str:
        normalized_keyword = str(keyword or "").strip()
        if self._gallery_keyword_error(normalized_keyword):
            return "请使用：回复机器人发送的图库图片后发送 /图库图片删除 关键词"

        reply_message_id = self.get_reply_message_id(event)
        if not reply_message_id:
            return "请先回复机器人发送的单张图库图片。"
        reply_payload = await self.napcat_service.fetch_onebot_message(
            event,
            reply_message_id,
        )
        if not reply_payload:
            return "未能读取被回复的图库图片，请稍后重试。"

        sender = reply_payload.get("sender") or {}
        sender_id = str(sender.get("user_id") or sender.get("qq") or "")
        self_id = self._self_id_of_event(event)
        if self_id and sender_id and sender_id != self_id:
            return "只能删除机器人发送的图库图片。"

        segments = await self.image_service.build_reply_segments(
            event,
            reply_payload.get("message"),
        )
        images = [
            segment.image
            for segment in segments
            if segment.type == "image" and segment.image is not None
        ]
        if len(images) != 1:
            return "被回复的消息必须只包含一张可识别的图库图片。"

        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        status, _ = await self.repository.delete_gallery_image(
            session_key,
            normalized_keyword,
            images[0],
        )
        if status == "gallery_not_found":
            return f'当前会话没有名为“{normalized_keyword}”的图库。'
        if status == "image_not_found":
            return f'这张图不在图库“{normalized_keyword}”中。'
        if status != "deleted":
            return "图库图片删除失败，请稍后重试。"
        logger.info(
            "单张图库图片删除成功: "
            f"session={session_key}, keyword={normalized_keyword}"
        )
        return f'已从图库“{normalized_keyword}”删除这张图片。'

    def _quote_list_summary(self, quote: Quote, max_length: int = 56) -> str:
        text = " ".join(str(quote.text or "").split())
        if len(text) > max_length:
            text = f"{text[: max_length - 1]}…"
        if quote.kind == "forward":
            count = len(quote.forward_nodes)
            detail = f"聊天记录，共 {count} 个节点"
            return f"[{detail}] {text}" if text else f"[{detail}]"

        image_count = sum(
            1
            for segment in quote.segments
            if segment.type == "image" and segment.asset_id
        )
        if image_count:
            image_label = f"[图片×{image_count}]"
            return f"{text} {image_label}" if text else image_label
        return text or "[无文本内容]"

    async def shutdown(self) -> None:
        startup_task = self._startup_pre_render_task
        if startup_task is not None and not startup_task.done():
            startup_task.cancel()
        tasks = [task for task in self._render_tasks if not task.done()]
        for task in tasks:
            task.cancel()
        pending: list[asyncio.Task[Any]] = list(tasks)
        if startup_task is not None:
            pending.append(startup_task)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def resolve_delete_target(self, event: Any) -> str | None:
        session_key = make_session_key(event.get_group_id(), event.get_sender_id())
        reply_message_id = self.get_reply_message_id(event)
        if not reply_message_id:
            logger.info("删除语录定位失败: 当前消息没有 Reply 段。")
            return None

        reply_payload = await self.napcat_service.fetch_onebot_message(event, reply_message_id)
        if not reply_payload:
            logger.info(f"删除语录定位失败: get_msg 未返回被回复消息, message_id={reply_message_id}")
            return None

        sender = reply_payload.get("sender") or {}
        sender_id = str(sender.get("user_id") or sender.get("qq") or "")
        self_id = self._self_id_of_event(event)
        if self_id and sender_id and sender_id != self_id:
            logger.info(
                "删除语录定位失败: 被回复消息不是机器人发送。"
                f" sender_id={sender_id}, self_id={self_id}, message_id={reply_message_id}"
            )
            return None

        fingerprint, image = await self._fingerprint_from_reply_payload(event, reply_payload)
        if not fingerprint:
            logger.info(f"删除语录定位失败: 无法从被回复消息计算指纹, message_id={reply_message_id}")
            return None

        replied_at = float(reply_payload.get("time") or 0)
        quote_id = self.repository.find_sent_quote_id(
            session_key,
            fingerprint=fingerprint,
            replied_at=replied_at,
        )
        if quote_id:
            logger.info(f"删除语录定位成功: 精确指纹匹配 quote_id={quote_id}, session={session_key}")
            return quote_id
        if image is not None:
            legacy_quote_id = self.repository.find_sent_quote_id(
                session_key,
                fingerprint=self._fingerprint_image_chain_sha(image.sha256),
                replied_at=replied_at,
            )
            if legacy_quote_id:
                logger.info(
                    "删除语录定位成功: 兼容旧版纯图片链式指纹匹配 "
                    f"quote_id={legacy_quote_id}, session={session_key}"
                )
                return legacy_quote_id
            near_quote_id = self.repository.find_sent_quote_id_by_image_signature(
                session_key,
                image=image,
                replied_at=replied_at,
            )
            if near_quote_id:
                logger.info(
                    "删除语录定位成功: 纯图片感知哈希近似匹配 "
                    f"quote_id={near_quote_id}, session={session_key}"
                )
                return near_quote_id
            logger.info(
                "删除语录定位失败: 单图消息未匹配 sent_index。"
                f" session={session_key}, sha256={image.sha256[:12]}, dhash={image.dhash[:12]}"
            )
            return None
        logger.info(f"删除语录定位失败: 指纹未匹配 sent_index, session={session_key}")
        return None

    async def build_quote_chain(self, quote: Quote, sender_id: str = "") -> list[Any]:
        if Comp is None:
            return []
        if quote.kind == "forward":
            return self.build_forward_quote_chain(quote)
        return await self.build_standard_quote_chain(quote, sender_id)

    async def build_standard_quote_chain(self, quote: Quote, sender_id: str = "") -> list[Any]:
        if not quote.segments:
            return []
        has_image = any(segment.type == "image" and segment.asset_id for segment in quote.segments)
        if not has_image:
            return []

        asset_map = self.repository.find_assets(quote.group, quote.image_ids)
        chain: list[Any] = []
        for segment in quote.segments:
            if segment.type == "text":
                text = str(segment.text or "").strip()
                if text:
                    chain.append(Comp.Plain(text))
                continue
            if segment.type != "image" or not segment.asset_id:
                continue
            asset = asset_map.get(segment.asset_id)
            if asset is None:
                continue
            abs_path = self.repository.root / asset.rel_path
            if abs_path.exists():
                try:
                    image_bytes = await asyncio.to_thread(
                        normalize_image_file_for_send,
                        abs_path,
                    )
                    chain.append(Comp.Image.fromBytes(image_bytes))
                except Exception as exc:
                    logger.warning(
                        f"语录图片规范化失败，改发占位文本: "
                        f"quote_id={quote.id}, path={abs_path}, error={exc}"
                    )
                    chain.append(Comp.Plain("[图片暂时无法发送]"))
        if chain:
            identifier = str(sender_id or quote.qq or quote.name).strip()
            if identifier:
                chain.append(Comp.Plain(f"\n— {identifier}"))
        return chain

    def build_forward_quote_chain(self, quote: Quote) -> list[Any]:
        if not quote.forward_nodes:
            return []
        image_ids, media_ids = collect_forward_asset_ids(quote.forward_nodes)
        image_map = self.repository.find_assets(quote.group, image_ids)
        media_map = self.repository.find_media_assets(quote.group, media_ids)
        nodes = [
            self._build_forward_node_component(quote.group, node, image_map, media_map)
            for node in quote.forward_nodes
        ]
        nodes = [node for node in nodes if node is not None]
        if not nodes:
            return []
        return [Comp.Nodes(nodes=nodes)]

    def _build_forward_node_component(
        self,
        session_key: str,
        node: ForwardNode,
        image_map: dict[str, Any],
        media_map: dict[str, Any],
    ) -> Any | None:
        content = self._build_forward_segment_components(
            session_key,
            node.segments,
            image_map,
            media_map,
        )
        if not content:
            content = [Comp.Plain("[空消息]")]
        try:
            return Comp.Node(
                uin=str(node.sender_uin or "0"),
                name=str(node.sender_name or node.sender_uin or "未知用户"),
                content=content,
            )
        except Exception as exc:
            logger.info(f"构造 forward 节点失败: {exc}")
            return None

    def _build_forward_segment_components(
        self,
        session_key: str,
        segments: list[ForwardSegment],
        image_map: dict[str, Any],
        media_map: dict[str, Any],
    ) -> list[Any]:
        content: list[Any] = []
        for segment in segments:
            if segment.type == "text":
                text = str(segment.text or "")
                if text:
                    content.append(Comp.Plain(text))
                continue

            if segment.type == "image" and segment.asset_id:
                asset = image_map.get(segment.asset_id)
                if asset is not None:
                    abs_path = self.repository.root / asset.rel_path
                    if abs_path.exists():
                        content.append(Comp.Image.fromFileSystem(str(abs_path)))
                        continue
                content.append(Comp.Plain("[图片]"))
                continue

            if segment.type in {"record", "video", "file"} and segment.asset_id:
                media_asset = media_map.get(segment.asset_id)
                if media_asset is None:
                    content.append(Comp.Plain(self._placeholder_for_media(segment.type)))
                    continue
                abs_path = self.repository.root / media_asset.rel_path
                if not abs_path.exists():
                    content.append(Comp.Plain(self._placeholder_for_media(segment.type)))
                    continue
                component = self._build_media_component(segment.type, abs_path, media_asset.display_name)
                if component is None:
                    content.append(Comp.Plain(self._placeholder_for_media(segment.type)))
                else:
                    content.append(component)
                continue

            if segment.type == "face" and segment.face_id:
                try:
                    content.append(Comp.Face(id=segment.face_id))
                except Exception:
                    content.append(Comp.Plain("[表情]"))
                continue

            if segment.type == "at" and segment.qq:
                try:
                    content.append(Comp.At(qq=segment.qq, name=segment.name or ""))
                except Exception:
                    content.append(Comp.Plain(f"@{segment.name or segment.qq}"))
                continue

            if segment.type == "nodes":
                nested_nodes = [
                    self._build_forward_node_component(session_key, node, image_map, media_map)
                    for node in segment.nodes
                ]
                nested_nodes = [node for node in nested_nodes if node is not None]
                if nested_nodes:
                    content.append(Comp.Nodes(nodes=nested_nodes))
                else:
                    content.append(Comp.Plain("[聊天记录]"))
                continue

            placeholder = self._placeholder_for_unknown(segment.type)
            if placeholder:
                content.append(Comp.Plain(placeholder))
        return content

    def _build_media_component(self, media_type: str, path: Path, display_name: str) -> Any | None:
        try:
            if media_type == "record":
                return Comp.Record.fromFileSystem(str(path))
            if media_type == "video":
                return Comp.Video.fromFileSystem(str(path))
            if media_type == "file":
                return Comp.File(file=str(path), name=display_name or path.name)
        except Exception as exc:
            logger.info(f"构造媒体组件失败({media_type}): {exc}")
        return None

    async def cache_rendered_result(self, rendered_url: str, cache_path: Path) -> bool:
        if not self.render_cache:
            return False
        try:
            content: bytes | None = None
            if rendered_url.startswith("file://"):
                from urllib.parse import unquote, urlparse
                from urllib.request import url2pathname

                parsed = urlparse(rendered_url)
                raw_path = url2pathname(unquote(parsed.path))
                if parsed.netloc:
                    raw_path = f"//{parsed.netloc}{raw_path}"
                if len(raw_path) >= 3 and raw_path[0] in {"/", "\\"} and raw_path[2] == ":":
                    raw_path = raw_path[1:]
                local_path = Path(raw_path)
                if local_path.exists():
                    content = await asyncio.to_thread(local_path.read_bytes)
            elif rendered_url.startswith("data:image/") and ";base64," in rendered_url:
                content = base64.b64decode(rendered_url.split(",", 1)[1], validate=True)
            elif rendered_url.startswith("http") and self.http_client is not None:
                response = await self.http_client.get(rendered_url)
                if getattr(response, "status_code", 200) < 400:
                    content = bytes(response.content)
            else:
                local_path = Path(str(rendered_url or ""))
                if local_path.exists():
                    content = await asyncio.to_thread(local_path.read_bytes)
            if content:
                await asyncio.to_thread(self._atomic_write_cache, cache_path, content)
                return True
        except Exception as exc:
            logger.info(f"渲染缓存落盘失败: {exc}")
        return False

    def _atomic_write_cache(self, cache_path: Path, content: bytes) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
        tmp_path.write_bytes(content)
        tmp_path.replace(cache_path)

    def extract_at_qq(self, event: Any) -> str | None:
        try:
            for segment in event.get_messages():
                if Comp is not None and isinstance(segment, Comp.At):
                    for field in ("qq", "target", "uin", "user_id", "id"):
                        value = getattr(segment, field, None)
                        if value:
                            return str(value)
        except Exception as exc:
            logger.warning(f"解析 @ 失败: {exc}")
        return None

    def extract_exact_plain_text(self, event: Any) -> str | None:
        try:
            segments = list(event.get_messages())
        except Exception:
            return None
        if Comp is None or not segments:
            return None
        if any(not isinstance(segment, Comp.Plain) for segment in segments):
            return None
        text = "".join(str(getattr(segment, "text", "") or "") for segment in segments).strip()
        return text or None

    def get_reply_message_id(self, event: Any) -> str | None:
        try:
            for segment in event.get_messages():
                if Comp is not None and isinstance(segment, Comp.Reply):
                    value = (
                        getattr(segment, "message_id", None)
                        or getattr(segment, "id", None)
                        or getattr(segment, "reply", None)
                        or getattr(segment, "msgId", None)
                    )
                    if value:
                        return str(value)
        except Exception as exc:
            logger.warning(f"解析 Reply 段失败: {exc}")
        return None

    def _normalize_pending_segments(self, segments: list[Any]) -> list[Any]:
        normalized: list[Any] = []
        for segment in segments:
            if segment.type == "text":
                text = normalize_quote_text(str(segment.text or ""))
                if text:
                    segment.text = text
                    normalized.append(segment)
                continue
            if segment.type == "image" and getattr(segment, "image", None) is not None:
                normalized.append(segment)
        return normalized

    def _plain_text_from_pending_segments(self, segments: list[Any]) -> str:
        parts = [str(segment.text or "") for segment in segments if segment.type == "text" and str(segment.text or "").strip()]
        return " ".join(parts).strip()

    def _flatten_forward_nodes(self, nodes: list[Any]) -> str:
        lines: list[str] = []
        for node in nodes:
            sender = str(getattr(node, "sender_name", "") or getattr(node, "sender_uin", "") or "未知用户")
            parts: list[str] = []
            for segment in getattr(node, "segments", []):
                if segment.type == "text" and segment.text:
                    parts.append(str(segment.text))
                elif segment.type == "image":
                    parts.append("[图片]")
                elif segment.type == "record":
                    parts.append("[语音]")
                elif segment.type == "video":
                    parts.append("[视频]")
                elif segment.type == "file":
                    parts.append("[文件]")
                elif segment.type == "face" and getattr(segment, "face_id", 0):
                    parts.append("[表情]")
                elif segment.type == "at" and getattr(segment, "qq", ""):
                    parts.append(f"@{segment.name or segment.qq}")
                elif segment.type == "nodes":
                    parts.append("[聊天记录]")
            content = "".join(parts).strip()
            if content:
                lines.append(f"{sender}：{content}")
        return "\n".join(lines).strip()

    def _count_forward_messages(self, nodes: list[Any]) -> int:
        total = 0
        for node in nodes:
            total += 1
            for segment in getattr(node, "segments", []):
                if segment.type == "nodes":
                    total += self._count_forward_messages(segment.nodes)
        return total

    def _quote_plain_fallback(self, quote: Quote, signature: str = "") -> str:
        if quote.kind == "forward":
            return quote.text or f"{quote.name} 的聊天记录语录"
        body = normalize_quote_text(str(quote.text or "")) or "……"
        author = " ".join(str(signature or quote.name or quote.qq or "未知用户").split())
        return f"❝  {body}  ❞\n\n—— {author}"

    def _plain_quote_response(self, quote: Quote, signature: str = "") -> CommandResponse:
        text = self._quote_plain_fallback(quote, signature)
        chain: list[Any] = []
        if Comp is not None and quote.kind == "standard":
            qq = str(quote.qq or "").strip()
            if qq:
                try:
                    chain.append(Comp.Image.fromURL(self._quote_avatar_url(qq)))
                except Exception as exc:
                    logger.info(f"构造语录头像组件失败，回退纯文本: qq={qq}, error={exc}")
            chain.append(Comp.Plain(f"\n{text}" if chain else text))
        if chain:
            return CommandResponse(
                kind="chain",
                chain=chain,
                quote_id=quote.id,
                delete_fingerprint=self._fingerprint_plain_text(text),
            )
        return CommandResponse(
            kind="plain",
            text=text,
            quote_id=quote.id,
            delete_fingerprint=self._fingerprint_plain_text(text),
        )

    def _quote_avatar_url(self, qq: str) -> str:
        return f"https://q1.qlogo.cn/g?b=qq&nk={qq}&s=100"

    async def build_delete_fingerprint(self, quote: Quote, *, chain: list[Any] | None = None) -> str:
        if quote.kind == "forward":
            return self._fingerprint_forward_nodes(quote.group, quote.forward_nodes)

        if chain and len(chain) > 1:
            fingerprint = await self._fingerprint_standard_chain(chain)
            if fingerprint:
                return fingerprint

        single_image = self._single_standard_image_signature(quote)
        if single_image is not None:
            logger.info(f"语录删除指纹: 使用纯图片指纹 quote_id={quote.id}, sha256={single_image.sha256[:12]}")
            return self._fingerprint_image_sha(single_image.sha256)

        if chain:
            fingerprint = await self._fingerprint_standard_chain(chain)
            if fingerprint:
                return fingerprint

        return self._fingerprint_standard_quote(quote)

    def build_delete_image_signatures(self, quote: Quote) -> list[ImageSignature]:
        signature = self._single_standard_image_signature(quote)
        return [signature] if signature is not None else []

    async def _fingerprint_from_reply_payload(
        self,
        event: Any,
        reply_payload: dict[str, Any],
    ) -> tuple[str, Any | None]:
        message = reply_payload.get("message")
        forward_id, forward_payload = self.napcat_service.extract_forward_reference(message)
        if forward_id or forward_payload:
            nodes = await self.image_service.build_forward_nodes(
                event,
                forward_id=forward_id,
                forward_payload=forward_payload,
                forward_loader=self.napcat_service.fetch_forward_messages,
            )
            if nodes:
                return self._fingerprint_pending_forward_nodes(nodes), None

        segments = await self.image_service.build_reply_segments(event, message)
        normalized = self._normalize_pending_segments(segments)
        if not normalized:
            return "", None

        if self._is_avatar_text_quote(normalized):
            return self._fingerprint_plain_text(normalized[-1].text), None

        if len(normalized) == 1 and normalized[0].type == "image" and normalized[0].image is not None:
            return self._fingerprint_image_sha(normalized[0].image.sha256), normalized[0].image

        reply_images = [
            segment.image
            for segment in normalized
            if segment.type == "image" and segment.image is not None
        ]
        return (
            self._hash_payload(
                {
                    "kind": "chain",
                    "parts": [
                        self._pending_segment_payload(segment)
                        for segment in normalized
                    ],
                }
            ),
            reply_images[0] if len(reply_images) == 1 else None,
        )

    def _is_avatar_text_quote(self, segments: list[Any]) -> bool:
        if len(segments) != 2:
            return False
        image_segment, text_segment = segments
        if image_segment.type != "image" or text_segment.type != "text":
            return False
        text = normalize_quote_text(str(text_segment.text or ""))
        legacy_style = text.startswith("「") and "」 — " in text
        literary_style = text.startswith("❝") and "❞" in text and "—— " in text
        ascii_style = text.startswith("+--[ QUOTE ]") and "| +--[ " in text and text.endswith("]")
        return legacy_style or literary_style or ascii_style

    def _fingerprint_plain_text(self, text: str) -> str:
        normalized = self._canonical_text(normalize_quote_text(text))
        if not normalized:
            return ""
        return self._hash_payload(
            {"kind": "chain", "parts": [{"type": "text", "text": normalized}]}
        )

    def _fingerprint_standard_quote(self, quote: Quote) -> str:
        if not quote.segments:
            return ""
        parts = [self._stored_standard_segment_payload(quote.group, segment) for segment in quote.segments]
        parts = [item for item in parts if item is not None]
        if not parts:
            return ""
        return self._hash_payload({"kind": "chain", "parts": parts})

    def _fingerprint_pending_standard_segments(self, segments: list[Any]) -> str:
        parts = [self._pending_segment_payload(segment) for segment in segments]
        parts = [item for item in parts if item is not None]
        if not parts:
            return ""
        return self._hash_payload({"kind": "chain", "parts": parts})

    async def _fingerprint_standard_chain(self, chain: list[Any]) -> str:
        parts: list[dict[str, Any]] = []
        for component in chain:
            payload = await self._component_payload(component)
            if payload is not None:
                parts.append(payload)
        if not parts:
            return ""
        return self._hash_payload({"kind": "chain", "parts": parts})

    async def _fingerprint_image_path(self, path: Path) -> str:
        try:
            if path.exists():
                content = await asyncio.to_thread(path.read_bytes)
                return self._fingerprint_image_sha(sha256_bytes(content))
        except Exception as exc:
            logger.info(f"计算语录图片指纹失败: {exc}")
        return ""

    async def _fingerprint_image_url(self, url: str) -> str:
        if not url:
            return ""
        try:
            if url.startswith("file://"):
                from urllib.parse import unquote, urlparse

                parsed = urlparse(url)
                local_path = Path(unquote(parsed.path))
                return await self._fingerprint_image_path(local_path)

            if url.startswith("http") and self.http_client is not None:
                response = await self.http_client.get(url)
                if getattr(response, "status_code", 200) < 400:
                    return self._fingerprint_image_sha(sha256_bytes(bytes(response.content)))
        except Exception as exc:
            logger.info(f"计算语录图片 URL 指纹失败: {exc}")
        return ""

    async def _component_payload(self, component: Any) -> dict[str, Any] | None:
        if Comp is None:
            return None

        if isinstance(component, Comp.Plain):
            text = self._canonical_text(str(getattr(component, "text", "") or ""))
            return {"type": "text", "text": text} if text else None

        if isinstance(component, Comp.Image):
            image_hash = await self._hash_component_file(component)
            return {"type": "image", "sha256": image_hash} if image_hash else None

        if isinstance(component, Comp.Record):
            media_hash = await self._hash_component_file(component)
            return {"type": "record", "sha256": media_hash} if media_hash else {"type": "record", "text": "[语音]"}

        if isinstance(component, Comp.Video):
            media_hash = await self._hash_component_file(component)
            return {"type": "video", "sha256": media_hash} if media_hash else {"type": "video", "text": "[视频]"}

        if isinstance(component, Comp.File):
            media_hash = await self._hash_component_file(component)
            if media_hash:
                return {"type": "file", "sha256": media_hash}
            return {"type": "file", "name": str(getattr(component, "name", "") or "")}

        if isinstance(component, Comp.At):
            return {
                "type": "at",
                "qq": str(getattr(component, "qq", "") or ""),
                "name": str(getattr(component, "name", "") or ""),
            }

        if isinstance(component, Comp.Face):
            return {"type": "face", "face_id": int(getattr(component, "id", 0) or 0)}

        if isinstance(component, Comp.Node):
            nested = await self._fingerprint_node_component(component)
            return nested

        if isinstance(component, Comp.Nodes):
            nested_nodes = []
            for node in list(getattr(component, "nodes", []) or []):
                node_payload = await self._fingerprint_node_component(node)
                if node_payload is not None:
                    nested_nodes.append(node_payload)
            return {"type": "nodes", "nodes": nested_nodes} if nested_nodes else None

        return None

    async def _fingerprint_node_component(self, node: Any) -> dict[str, Any] | None:
        content = []
        for component in list(getattr(node, "content", []) or []):
            payload = await self._component_payload(component)
            if payload is not None:
                content.append(payload)
        return {
            "sender_uin": str(getattr(node, "uin", "") or ""),
            "sender_name": str(getattr(node, "name", "") or ""),
            "segments": content,
        }

    async def _hash_component_file(self, component: Any) -> str:
        for attr in ("path", "file"):
            raw_value = getattr(component, attr, None)
            path_hash = await self._hash_local_or_remote_file(raw_value)
            if path_hash:
                return path_hash
        return ""

    async def _hash_local_or_remote_file(self, raw_value: Any) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        try:
            if value.startswith("base64://"):
                content = base64.b64decode(value[9:], validate=True)
                return sha256_bytes(content) if content else ""
            if value.startswith("data:image/") and ";base64," in value:
                content = base64.b64decode(value.split(",", 1)[1], validate=True)
                return sha256_bytes(content) if content else ""
            if value.startswith("file:///"):
                value = value[8:]
            elif value.startswith("file://"):
                value = value[7:]
            path = Path(value)
            if path.exists():
                content = await asyncio.to_thread(path.read_bytes)
                return sha256_bytes(content)
            if value.startswith("http") and self.http_client is not None:
                response = await self.http_client.get(value)
                if getattr(response, "status_code", 200) < 400:
                    return sha256_bytes(bytes(response.content))
        except Exception as exc:
            logger.info(f"计算媒体文件指纹失败: {exc}")
        return ""

    def _fingerprint_image_sha(self, sha_value: str) -> str:
        return self._hash_payload({"kind": "image", "sha256": str(sha_value or "")})

    def _fingerprint_image_chain_sha(self, sha_value: str) -> str:
        return self._hash_payload(
            {
                "kind": "chain",
                "parts": [{"type": "image", "sha256": str(sha_value or "")}],
            }
        )

    def _single_standard_image_signature(self, quote: Quote) -> ImageSignature | None:
        if quote.kind != "standard":
            return None
        image_segments = [
            segment
            for segment in quote.segments
            if segment.type == "image" and segment.asset_id
        ]
        if len(image_segments) != 1:
            return None
        segment = image_segments[0]
        asset = self.repository.find_asset(quote.group, segment.asset_id)
        if asset is None or not asset.sha256:
            return None
        return ImageSignature(
            sha256=asset.sha256,
            dhash=asset.dhash,
            width=asset.width,
            height=asset.height,
        )

    def _stored_standard_segment_payload(self, session_key: str, segment: Any) -> dict[str, Any] | None:
        if segment.type == "text":
            text = self._canonical_text(str(segment.text or ""))
            return {"type": "text", "text": text} if text else None
        if segment.type == "image" and segment.asset_id:
            asset = self.repository.find_asset(session_key, segment.asset_id)
            if asset is not None and asset.sha256:
                return {"type": "image", "sha256": asset.sha256}
            return {"type": "image", "asset_id": segment.asset_id}
        return None

    def _pending_segment_payload(self, segment: Any) -> dict[str, Any] | None:
        if segment.type == "text":
            text = self._canonical_text(str(segment.text or ""))
            return {"type": "text", "text": text} if text else None
        if segment.type == "image" and getattr(segment, "image", None) is not None:
            return {"type": "image", "sha256": str(segment.image.sha256 or "")}
        return None

    def _fingerprint_forward_nodes(self, session_key: str, nodes: list[ForwardNode]) -> str:
        payload = [self._stored_forward_node_payload(session_key, node) for node in nodes]
        payload = [item for item in payload if item is not None]
        if not payload:
            return ""
        return self._hash_payload({"kind": "forward", "nodes": payload})

    def _fingerprint_pending_forward_nodes(self, nodes: list[Any]) -> str:
        payload = [self._pending_forward_node_payload(node) for node in nodes]
        payload = [item for item in payload if item is not None]
        if not payload:
            return ""
        return self._hash_payload({"kind": "forward", "nodes": payload})

    def _stored_forward_node_payload(self, session_key: str, node: ForwardNode) -> dict[str, Any] | None:
        segments = [self._stored_forward_segment_payload(session_key, segment) for segment in node.segments]
        segments = [item for item in segments if item is not None]
        return {
            "sender_uin": str(node.sender_uin or ""),
            "sender_name": str(node.sender_name or ""),
            "segments": segments,
        }

    def _stored_forward_segment_payload(self, session_key: str, segment: ForwardSegment) -> dict[str, Any] | None:
        if segment.type == "text":
            text = self._canonical_text(str(segment.text or ""))
            return {"type": "text", "text": text} if text else None
        if segment.type == "image" and segment.asset_id:
            asset = self.repository.find_asset(session_key, segment.asset_id)
            if asset is not None and asset.sha256:
                return {"type": "image", "sha256": asset.sha256}
            return {"type": "image", "asset_id": segment.asset_id}
        if segment.type in {"record", "video", "file"} and segment.asset_id:
            asset = self.repository.find_media_asset(session_key, segment.asset_id)
            if asset is not None:
                abs_path = self.repository.root / asset.rel_path
                if abs_path.exists():
                    return {"type": segment.type, "sha256": sha256_bytes(abs_path.read_bytes())}
            return {"type": segment.type, "asset_id": segment.asset_id}
        if segment.type == "face" and segment.face_id:
            return {"type": "face", "face_id": int(segment.face_id)}
        if segment.type == "at" and segment.qq:
            return {"type": "at", "qq": str(segment.qq), "name": str(segment.name or "")}
        if segment.type == "nodes":
            nested = [self._stored_forward_node_payload(session_key, node) for node in segment.nodes]
            nested = [item for item in nested if item is not None]
            return {"type": "nodes", "nodes": nested} if nested else {"type": "text", "text": "[聊天记录]"}
        placeholder = self._placeholder_for_unknown(segment.type)
        return {"type": "text", "text": placeholder} if placeholder else None

    def _pending_forward_node_payload(self, node: Any) -> dict[str, Any] | None:
        segments = [self._pending_forward_segment_payload(segment) for segment in getattr(node, "segments", [])]
        segments = [item for item in segments if item is not None]
        return {
            "sender_uin": str(getattr(node, "sender_uin", "") or ""),
            "sender_name": str(getattr(node, "sender_name", "") or ""),
            "segments": segments,
        }

    def _pending_forward_segment_payload(self, segment: Any) -> dict[str, Any] | None:
        if segment.type == "text":
            text = self._canonical_text(str(segment.text or ""))
            return {"type": "text", "text": text} if text else None
        if segment.type == "image" and getattr(segment, "image", None) is not None:
            return {"type": "image", "sha256": str(segment.image.sha256 or "")}
        if segment.type in {"record", "video", "file"} and getattr(segment, "media", None) is not None:
            return {"type": segment.type, "sha256": sha256_bytes(segment.media.content)}
        if segment.type == "face" and getattr(segment, "face_id", 0):
            return {"type": "face", "face_id": int(segment.face_id)}
        if segment.type == "at" and getattr(segment, "qq", ""):
            return {"type": "at", "qq": str(segment.qq), "name": str(segment.name or "")}
        if segment.type == "nodes":
            nested = [self._pending_forward_node_payload(node) for node in getattr(segment, "nodes", [])]
            nested = [item for item in nested if item is not None]
            return {"type": "nodes", "nodes": nested} if nested else {"type": "text", "text": "[聊天记录]"}
        placeholder = self._placeholder_for_unknown(segment.type)
        return {"type": "text", "text": placeholder} if placeholder else None

    def _canonical_text(self, text: str) -> str:
        return str(text or "").replace("\r\n", "\n").strip()

    def _hash_payload(self, payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return sha256_bytes(canonical.encode("utf-8"))

    def _has_duplicate_quote(self, session_key: str, *, target_qq: str, fingerprint: str) -> bool:
        if not target_qq or not fingerprint:
            return False
        has_fingerprint = getattr(self.repository, "has_content_fingerprint", None)
        if callable(has_fingerprint) and has_fingerprint(session_key, target_qq, fingerprint):
            return True
        list_for_owner = getattr(self.repository, "list_quotes_for_owner", None)
        quotes = (
            list_for_owner(session_key, target_qq)
            if callable(list_for_owner)
            else self.repository.list_quotes(session_key)
        )
        update_fingerprint = getattr(self.repository, "update_content_fingerprint", None)
        for quote in quotes:
            if str(quote.qq or "") != str(target_qq):
                continue
            if quote.content_fingerprint:
                if quote.content_fingerprint == fingerprint:
                    return True
                continue
            existing_fingerprint = self._stored_quote_fingerprint(quote)
            if existing_fingerprint and callable(update_fingerprint):
                update_fingerprint(quote.id, existing_fingerprint)
            if existing_fingerprint and existing_fingerprint == fingerprint:
                return True
        return False

    def _stored_quote_fingerprint(self, quote: Quote) -> str:
        if quote.kind == "forward":
            return self._fingerprint_forward_nodes(quote.group, quote.forward_nodes)
        return self._fingerprint_standard_quote(quote)

    def _self_id_of_event(self, event: Any) -> str:
        for getter in (
            lambda: getattr(event, "get_self_id", lambda: "")(),
            lambda: getattr(getattr(event, "message_obj", None), "self_id", None),
            lambda: getattr(event, "self_id", None),
            lambda: (getattr(event, "raw_event", None) or {}).get("self_id")
            if isinstance(getattr(event, "raw_event", None), dict)
            else None,
        ):
            try:
                value = getter()
            except Exception:
                value = None
            if value:
                return str(value)
        return ""

    def _placeholder_for_media(self, media_type: str) -> str:
        return {
            "record": "[语音]",
            "video": "[视频]",
            "file": "[文件]",
        }.get(media_type, "[附件]")

    def _placeholder_for_unknown(self, seg_type: str) -> str:
        if not seg_type:
            return ""
        return f"[{seg_type}]"

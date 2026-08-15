from __future__ import annotations

from time import time
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
import astrbot.api.message_components as Comp

try:
    from astrbot.api import AstrBotConfig  # type: ignore
except Exception:  # pragma: no cover
    AstrBotConfig = dict  # type: ignore

try:
    from .constants import (
        PLUGIN_NAME,
        QUOTE_EVENT_LIMIT,
        QUOTE_EVENT_WINDOW_SECONDS,
        QUOTE_RATE_LIMIT_MESSAGES,
    )
    from .git_backup import DEFAULT_GIT_BACKUP_MESSAGE, GitBackupService
    from .image_service import ImageService
    from .models import CommandResponse
    from .napcat_service import NapcatService
    from .quote_service import QuoteService
    from .rate_limiter import SlidingWindowRateLimiter
    from .renderer import QuoteRenderer
    from .sqlite_store import QuoteRepository
    from .utils import ensure_plugin_data_dir, is_valid_qq, make_session_key, resolve_wake_prefixes
except ImportError:  # pragma: no cover
    from constants import (
        PLUGIN_NAME,
        QUOTE_EVENT_LIMIT,
        QUOTE_EVENT_WINDOW_SECONDS,
        QUOTE_RATE_LIMIT_MESSAGES,
    )
    from git_backup import DEFAULT_GIT_BACKUP_MESSAGE, GitBackupService
    from image_service import ImageService
    from models import CommandResponse
    from napcat_service import NapcatService
    from quote_service import QuoteService
    from rate_limiter import SlidingWindowRateLimiter
    from renderer import QuoteRenderer
    from sqlite_store import QuoteRepository
    from utils import ensure_plugin_data_dir, is_valid_qq, make_session_key, resolve_wake_prefixes


@register(
    PLUGIN_NAME,
    "Codex",
    "提交语录并生成带头像的语录图片，支持图文混合与聊天记录语录",
    "1.10.0",
    "https://github.com/exynos967/astrbot_quotes_plugin",
)
class QuotesPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self.http_client = self._create_http_client()
        self.data_root = ensure_plugin_data_dir(str(self.config.get("storage") or "").strip(), PLUGIN_NAME)
        self.repository = QuoteRepository(self.data_root)
        git_backup_config = self.config.get("git_backup") or {}
        self.git_backup = GitBackupService(
            self.data_root,
            enabled=bool(git_backup_config.get("enabled", False)),
            interval_seconds=self._parse_bounded_float(
                git_backup_config.get("interval_minutes", 1440),
                default=1440.0,
                minimum=1.0,
                maximum=525600.0,
            )
            * 60.0,
            commit_message=str(
                git_backup_config.get("commit_message") or DEFAULT_GIT_BACKUP_MESSAGE
            ),
            command_timeout_seconds=self._parse_bounded_float(
                git_backup_config.get("command_timeout_seconds", 120),
                default=120.0,
                minimum=10.0,
                maximum=1800.0,
            ),
            stage_guard_factory=self.repository.git_backup_snapshot,
        )
        self.napcat_service = NapcatService()
        self._wake_prefixes = resolve_wake_prefixes(self._resolve_context_config())
        self.image_service = ImageService(
            self.http_client,
            wake_prefixes=self._wake_prefixes,
        )
        self.renderer = QuoteRenderer(self.html_render, self.config.get("image") or {})
        performance_config = self.config.get("performance") or {}
        self.quote_service = QuoteService(
            repository=self.repository,
            image_service=self.image_service,
            napcat_service=self.napcat_service,
            renderer=self.renderer,
            http_client=self.http_client,
            global_mode=bool(self.config.get("global_mode", False)),
            text_mode=bool(performance_config.get("text_mode", False)),
            render_cache=bool(performance_config.get("render_cache", True)),
            render_wait_timeout=self._parse_render_wait_timeout(
                performance_config.get("render_wait_timeout", 0.8)
            ),
            image_signature_use_group=bool(self.config.get("image_signature_use_group", False)),
            blacklist=self._parse_blacklist(),
        )
        self._cfg_poke_enabled = bool(self.config.get("poke_enabled", False))
        self._cfg_poke_probability = self._parse_probability(self.config.get("poke_probability", 20))
        self._cfg_poke_group_whitelist = self._parse_id_set(self.config.get("poke_group_whitelist") or [])
        self._cfg_poke_group_blacklist = self._parse_id_set(self.config.get("poke_group_blacklist") or [])
        self._dangerous_confirmations: dict[tuple[str, str, str, str], float] = {}
        self._quote_event_limiter = SlidingWindowRateLimiter(
            limit=QUOTE_EVENT_LIMIT,
            window_seconds=QUOTE_EVENT_WINDOW_SECONDS,
        )

    async def initialize(self):
        await self.repository.migrate_legacy_data()
        await self.renderer.warmup()
        self.quote_service.schedule_startup_pre_render()
        self.git_backup.start()

    async def terminate(self):
        await self.git_backup.stop()
        await self.quote_service.shutdown()
        if self.http_client is not None:
            try:
                await self.http_client.aclose()
            except Exception:
                return

    @filter.command("上传")
    async def add_quote(self, event: AstrMessageEvent, uid: str = ""):
        response = await self.quote_service.add_quote(event, uid=uid)
        for item in self._emit_response(event, response):
            yield item

    @filter.command("语录")
    async def random_quote(self, event: AstrMessageEvent, uid: str = ""):
        target = self.quote_service.extract_at_qq(event) or str(uid or "").strip()
        allowed, limited_response = self._check_quote_event_rate_limit(
            event,
            target=target or "语录里的某个人",
        )
        if not allowed:
            for item in self._emit_response(event, limited_response):
                yield item
            return
        response = await self.quote_service.random_quote(event, uid=uid, silent_if_empty=False)
        for item in self._emit_response(event, response):
            yield item

    @filter.command("语录列表")
    async def list_quotes(self, event: AstrMessageEvent, page: int = 1):
        text = await self.quote_service.build_quote_list_text(
            self._session_key(event),
            page=page,
        )
        yield event.plain_result(text)

    @filter.command("语录排名")
    async def rank_quotes(self, event: AstrMessageEvent):
        text = await self.quote_service.build_quote_ranking_text(self._session_key(event))
        yield event.plain_result(text)

    @filter.command("图库列表")
    async def list_galleries(self, event: AstrMessageEvent, page: int = 1):
        text = await self.quote_service.build_gallery_list_text(
            self._session_key(event),
            page=page,
        )
        yield event.plain_result(text)

    @filter.command("语录存储检查")
    async def audit_quote_storage(self, event: AstrMessageEvent):
        text = await self.quote_service.build_storage_audit_text(self._session_key(event))
        yield event.plain_result(text)

    @filter.command("语录备份")
    async def backup_quote_data(self, event: AstrMessageEvent):
        if not self._is_bot_admin(event):
            yield event.plain_result("权限不足：手动语录备份仅限 Bot 管理员使用。")
            return
        result = await self.git_backup.backup_once()
        if result.status == "error":
            logger.warning(f"手动 Git 备份失败: {result.message}")
        yield event.plain_result(result.command_message)

    @filter.command("语录缓存清理")
    async def clear_quote_cache(self, event: AstrMessageEvent):
        removed, failed = await self.quote_service.clear_render_cache(self._session_key(event))
        if failed:
            yield event.plain_result(
                f"语录缓存清理完成：已删除 {removed} 个文件，{failed} 个文件清理失败。"
            )
        elif removed:
            yield event.plain_result(
                f"已清理当前会话的语录缓存，共删除 {removed} 个文件；需要时会自动重新渲染。\n"
                "缓存打扫完毕！高性能ですから！"
            )
        else:
            yield event.plain_result("当前会话没有可清理的语录缓存。")

    @filter.command("删除", alias={"删除语录"})
    async def delete_quote(self, event: AstrMessageEvent):
        if not await self._check_delete_permission(event):
            yield event.plain_result("权限不足：你无权使用删除语录指令。")
            return
        if self.quote_service.get_reply_message_id(event) is None:
            yield event.plain_result("请先『回复机器人发送的语录』，再发送 删除。")
            return

        quote_id = await self.quote_service.resolve_delete_target(event)
        if not quote_id:
            yield event.plain_result("未能定位到你回复的那条语录，请确认回复的是机器人发送的语录消息。")
            return

        deleted = await self.quote_service.delete_quote(quote_id)
        if deleted:
            yield event.plain_result("已删除语录。")
        else:
            yield event.plain_result("未找到该语录，可能已被删除。")

    @filter.command("图库删除", alias={"删除图库"})
    async def delete_gallery(self, event: AstrMessageEvent, keyword: str = ""):
        if not await self._check_admin_permission(event):
            yield event.plain_result("权限不足：删除整个图库仅限群管理员、群主或 Bot 管理员。")
            return
        normalized_keyword = str(keyword or "").strip()
        if not normalized_keyword:
            yield event.plain_result("请使用：/图库删除 关键词")
            return
        image_count = self.repository.gallery_image_count(
            self._session_key(event),
            normalized_keyword,
        )
        if image_count == 0:
            yield event.plain_result(f'当前会话没有名为“{normalized_keyword}”的图库。')
            return
        if not self._consume_dangerous_confirmation(
            event,
            action="delete_gallery",
            target=normalized_keyword,
        ):
            yield event.plain_result(
                f'危险操作：将删除图库“{normalized_keyword}”的 {image_count} 张图片。\n'
                f'请在 60 秒内再次发送：/图库删除 {normalized_keyword}'
            )
            return
        text = await self.quote_service.delete_gallery(
            self._session_key(event),
            normalized_keyword,
        )
        yield event.plain_result(text)

    @filter.command("图库图片删除", alias={"删除图库图片"})
    async def delete_gallery_image(self, event: AstrMessageEvent, keyword: str = ""):
        if not await self._check_admin_permission(event):
            yield event.plain_result("权限不足：删除图库图片仅限群管理员、群主或 Bot 管理员。")
            return
        text = await self.quote_service.delete_gallery_image(event, keyword)
        yield event.plain_result(text)

    @filter.command("语录帮助")
    async def help_quote(self, event: AstrMessageEvent):
        help_text = (
            "语录插件帮助\n"
            "- 上传：回复消息后发送，保存为语录；可用“上传 @某人”“上传 QQ号”或已有标签指定归属。\n"
            "- 上传 图库关键词：回复或附带图片建立随机图库，发送完全相同的纯文本时随机发图。\n"
            "- 语录：随机发送语录；可用“语录 @某人”或“语录 QQ号”指定用户。\n"
            "- 语录列表 [页码]：按最新优先查看当前会话的语录。\n"
            "- 语录排名：按已收录语录数量排名，已绑定用户显示其 tag。\n"
            "- 图库列表 [页码]：分页查看当前会话的图库及图片数量。\n"
            "- 删除 / 删除语录：回复机器人发送的语录后删除。\n"
            "- 图库图片删除 关键词：回复机器人发送的图库图片后删除单张。\n"
            "- 图库删除 关键词：管理员二次确认后删除整个图库。\n"
            "- 绑定 @某人 tag：发送纯文本 tag 时随机该用户的语录。\n"
            "- 绑定列表：查看当前会话映射；重新绑定 @某人 [tag]：修改或取消映射。\n"
            "- 频率限制：每人在当前会话 2 分钟内最多触发 4 次语录或图库。\n"
            "- 语录缓存清理：清理当前会话的语录渲染缓存。\n"
            "- 语录存储检查：只读检查资源引用、缺失文件和孤儿文件。\n"
            "- 语录备份：Bot 管理员立即执行一次 Git 数据备份。\n"
            "- 语录帮助：查看本帮助。"
        )
        yield event.plain_result(help_text)

    @filter.command("绑定列表")
    async def list_quote_bindings(self, event: AstrMessageEvent):
        bindings = self.repository.list_bindings(self._session_key(event))
        if not bindings:
            yield event.plain_result("当前会话还没有语录绑定。")
            return
        lines = ["当前语录绑定："]
        lines.extend(f'- “{binding.tag}” → @{binding.qq}' for binding in bindings)
        yield event.plain_result("\n".join(lines))

    @filter.command("绑定")
    async def bind_quote_tag(self, event: AstrMessageEvent, tag: str = ""):
        qq = self.quote_service.extract_at_qq(event) or ""
        if not is_valid_qq(qq):
            yield event.plain_result("请使用：/绑定 @某人 tag")
            return
        resolved_tag = self._extract_binding_tag(event, qq, tag)
        error = self._binding_tag_error(resolved_tag)
        if error:
            yield event.plain_result(error)
            return

        session_key = self._session_key(event)
        status, detail = await self.repository.create_binding(session_key, qq, resolved_tag)
        if status == "created":
            self.quote_service.schedule_binding_pre_render(event, session_key, qq, resolved_tag)
            yield event.plain_result(f'已绑定：“{resolved_tag}” → @{qq}')
        elif status == "unchanged":
            yield event.plain_result(f'该用户已经绑定到“{detail}”。')
        elif status == "qq_exists":
            yield event.plain_result(f'@{qq} 已绑定到“{detail}”，请使用 /重新绑定 修改。')
        elif status == "tag_exists":
            yield event.plain_result(f'标签“{resolved_tag}”已绑定到 @{detail}。')
        elif status == "gallery_exists":
            yield event.plain_result(
                f'当前会话已存在同名图库“{detail}”，请更换标签或先删除该图库。'
            )
        else:
            yield event.plain_result("绑定失败，请稍后重试。")

    @filter.command("重新绑定")
    async def rebind_quote_tag(self, event: AstrMessageEvent, tag: str = ""):
        qq = self.quote_service.extract_at_qq(event) or ""
        if not is_valid_qq(qq):
            yield event.plain_result("请使用：/重新绑定 @某人 tag；省略 tag 可取消绑定。")
            return
        resolved_tag = self._extract_binding_tag(event, qq, tag)
        if resolved_tag:
            error = self._binding_tag_error(resolved_tag)
            if error:
                yield event.plain_result(error)
                return

        session_key = self._session_key(event)
        status, detail = await self.repository.rebind(session_key, qq, resolved_tag)
        if status == "updated":
            await self.quote_service.remove_signature_cache(session_key, qq, detail)
            self.quote_service.schedule_binding_pre_render(event, session_key, qq, resolved_tag)
            yield event.plain_result(f'已重新绑定：“{resolved_tag}” → @{qq}')
        elif status == "removed":
            await self.quote_service.remove_signature_cache(session_key, qq, detail)
            self.quote_service.schedule_owner_default_pre_render(event, session_key, qq)
            yield event.plain_result(f'已取消 @{qq} 的绑定“{detail}”。')
        elif status == "unchanged":
            yield event.plain_result(f'绑定未变化：@{qq} 仍绑定到“{detail}”。')
        elif status == "not_found":
            yield event.plain_result(f'@{qq} 尚未绑定，请先使用 /绑定。')
        elif status == "tag_exists":
            yield event.plain_result(f'标签“{resolved_tag}”已绑定到 @{detail}。')
        elif status == "gallery_exists":
            yield event.plain_result(
                f'当前会话已存在同名图库“{detail}”，请更换标签或先删除该图库。'
            )
        else:
            yield event.plain_result("重新绑定失败，请稍后重试。")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_quote_on_binding(self, event: AstrMessageEvent):
        self_id = self._get_self_id(event)
        if self_id and str(event.get_sender_id()) == self_id:
            return
        tag = self.quote_service.extract_exact_plain_text(event)
        if not tag:
            return
        binding = self.repository.get_binding_by_tag(self._session_key(event), tag)
        if binding is None:
            return
        allowed, limited_response = self._check_quote_event_rate_limit(
            event,
            target=binding.tag,
        )
        if not allowed:
            for item in self._emit_response(event, limited_response):
                yield item
            return
        response = await self.quote_service.random_quote(
            event,
            uid=binding.qq,
            silent_if_empty=False,
            signature_override=binding.tag,
        )
        for item in self._emit_response(event, response):
            yield item

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_image_on_gallery_keyword(self, event: AstrMessageEvent):
        self_id = self._get_self_id(event)
        if self_id and str(event.get_sender_id()) == self_id:
            return
        message_text = self.quote_service.extract_exact_plain_text(event)
        if not message_text:
            return
        if message_text.startswith("/") or any(
            prefix and message_text.startswith(prefix)
            for prefix in self._wake_prefixes
        ):
            return

        session_key = self._session_key(event)
        if self.repository.get_binding_by_tag(session_key, message_text) is not None:
            return
        if self.repository.gallery_image_count(session_key, message_text) <= 0:
            return
        allowed, limited_response = self._check_quote_event_rate_limit(
            event,
            target=f'“{message_text}”图库',
        )
        if not allowed:
            for item in self._emit_response(event, limited_response):
                yield item
            return
        response = await self.quote_service.random_gallery_response(
            session_key,
            message_text,
        )
        for item in self._emit_response(event, response):
            yield item

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_quote_on_poke(self, event: AstrMessageEvent):
        if not self._cfg_poke_enabled:
            return
        if not self._is_poke_allowed_in_group(event.get_group_id()):
            return
        self_id = self._get_self_id(event)
        if not self_id:
            return
        try:
            segments = list(event.get_messages())
        except Exception:
            return

        if not self._is_poke_to_bot(event, segments, self_id):
            return

        import secrets

        if self._cfg_poke_probability <= 0:
            return
        if self._cfg_poke_probability < 100 and secrets.randbelow(100) >= self._cfg_poke_probability:
            return

        allowed, limited_response = self._check_quote_event_rate_limit(
            event,
            target="亚托莉",
        )
        if not allowed:
            for item in self._emit_response(event, limited_response):
                yield item
            return

        response = await self.quote_service.random_quote(event, uid="", silent_if_empty=True)
        for item in self._emit_response(event, response):
            yield item

    @filter.after_message_sent()
    async def on_after_message_sent(self, event: AstrMessageEvent):
        try:
            quote_id = str(event.get_extra("_quotes_sent_quote_id", "") or "")
            fingerprint = str(event.get_extra("_quotes_sent_fingerprint", "") or "")
            if quote_id and fingerprint:
                await self.repository.record_sent_quote(
                    self._session_key(event),
                    quote_id=quote_id,
                    fingerprint=fingerprint,
                    sent_at=time(),
                    image_signatures=event.get_extra("_quotes_sent_image_signatures", default=[]),
                )
                logger.info(f"已记录语录发送索引: quote_id={quote_id}, session={self._session_key(event)}")
        except Exception as exc:
            logger.info(f"after_message_sent 记录失败: {exc}")

    def _check_quote_event_rate_limit(
        self,
        event: AstrMessageEvent,
        *,
        target: str,
    ) -> tuple[bool, CommandResponse | None]:
        sender_id = str(event.get_sender_id())
        decision = self._quote_event_limiter.check(
            (self._session_key(event), sender_id)
        )
        if decision.allowed:
            return True, None
        if not decision.notify:
            return False, None

        import secrets

        display_target = str(target or "语录里的某个人").strip()
        template = secrets.choice(QUOTE_RATE_LIMIT_MESSAGES)
        warning = template.format(
            target=display_target,
            seconds=decision.retry_after,
        )
        try:
            response = CommandResponse(
                kind="chain",
                chain=[Comp.At(qq=sender_id), Comp.Plain(f" {warning}")],
            )
        except Exception:
            response = CommandResponse(kind="plain", text=f"@{sender_id} {warning}")
        return False, response

    def _emit_response(self, event: AstrMessageEvent, response: CommandResponse | None):
        if response is None or response.kind == "none":
            return
        if response.quote_id and response.delete_fingerprint:
            event.set_extra("_quotes_sent_quote_id", response.quote_id)
            event.set_extra("_quotes_sent_fingerprint", response.delete_fingerprint)
            event.set_extra("_quotes_sent_image_signatures", response.delete_image_signatures)
        if response.kind == "plain":
            yield event.plain_result(response.text)
            return
        if response.kind == "chain":
            yield event.chain_result(response.chain)
            return
        if response.kind == "image_path":
            yield event.chain_result([Comp.Image.fromFileSystem(response.path)])
            return
        if response.kind == "image_url":
            yield event.image_result(response.url)

    def _session_key(self, event: AstrMessageEvent) -> str:
        return make_session_key(event.get_group_id(), event.get_sender_id())

    def _extract_binding_tag(
        self,
        event: AstrMessageEvent,
        target_qq: str,
        supplied_tag: str = "",
    ) -> str:
        try:
            found_target = False
            plain_parts: list[str] = []
            for segment in event.get_messages():
                if isinstance(segment, Comp.At):
                    segment_qq = str(getattr(segment, "qq", "") or "")
                    if found_target:
                        break
                    found_target = segment_qq == target_qq
                    continue
                if found_target:
                    if not isinstance(segment, Comp.Plain):
                        break
                    text = str(getattr(segment, "text", "") or "").strip()
                    if text:
                        plain_parts.append(text)
        except Exception:
            found_target = False
            plain_parts = []
        if found_target:
            return " ".join(plain_parts).strip()
        return str(supplied_tag or "").strip()

    def _binding_tag_error(self, tag: str) -> str:
        if not tag:
            return "tag 不能为空，请使用：/绑定 @某人 tag"
        if "\n" in tag or "\r" in tag:
            return "tag 不能包含换行。"
        if len(tag) > 64:
            return "tag 不能超过 64 个字符。"
        return ""

    def _create_http_client(self):
        try:
            import httpx  # type: ignore

            return httpx.AsyncClient(timeout=20)
        except Exception:
            return None

    def _resolve_context_config(self):
        getter = getattr(self, "_get_context_config", None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                pass

        getter = getattr(self.context, "get_config", None)
        if callable(getter):
            try:
                return getter()
            except Exception:
                pass
        return getattr(self.context, "_config", None)

    def _parse_blacklist(self) -> set[str]:
        raw = self.config.get("blacklist")
        items: set[str] = set()
        if isinstance(raw, (list, tuple)):
            for item in raw:
                value = str(item).strip()
                if value.isdigit() and len(value) >= 5:
                    items.add(value)
            return items
        for chunk in str(raw or "").replace("；", ";").replace("，", ",").splitlines():
            for item in chunk.replace(";", ",").split(","):
                value = item.strip()
                if value.isdigit() and len(value) >= 5:
                    items.add(value)
        return items

    def _parse_probability(self, value: Any) -> int:
        try:
            return max(0, min(100, int(value)))
        except (TypeError, ValueError):
            return 20

    def _parse_render_wait_timeout(self, value: Any) -> float:
        try:
            return max(0.0, min(5.0, float(value)))
        except (TypeError, ValueError):
            return 0.8

    def _parse_bounded_float(
        self,
        value: Any,
        *,
        default: float,
        minimum: float,
        maximum: float,
    ) -> float:
        try:
            return max(minimum, min(maximum, float(value)))
        except (TypeError, ValueError):
            return default

    def _parse_id_set(self, values: Any) -> set[str]:
        return {str(item).strip() for item in values if str(item).strip()}

    def _is_poke_allowed_in_group(self, group_id: str | None) -> bool:
        if not group_id:
            return True
        gid = str(group_id)
        if self._cfg_poke_group_whitelist:
            return gid in self._cfg_poke_group_whitelist
        if self._cfg_poke_group_blacklist:
            return gid not in self._cfg_poke_group_blacklist
        return True

    def _get_self_id(self, event: AstrMessageEvent) -> str:
        for getter in (
            lambda: getattr(getattr(event, "message_obj", None), "self_id", None),
            lambda: event.get_self_id() if hasattr(event, "get_self_id") else None,
            lambda: getattr(event, "self_id", None),
            lambda: (getattr(event, "raw_event", None) or {}).get("self_id") if isinstance(getattr(event, "raw_event", None), dict) else None,
        ):
            try:
                value = getter()
            except Exception:
                value = None
            if value:
                return str(value)
        return ""

    def _is_poke_to_bot(self, event: AstrMessageEvent, segments: list[Any], self_id: str) -> bool:
        has_unknown_target_poke = False
        for segment in segments:
            try:
                if not isinstance(segment, Comp.Poke):
                    continue
                target = self._extract_poke_target(segment)
                if self._is_same_id(target, self_id):
                    return True
                if not self._looks_like_user_id(target):
                    has_unknown_target_poke = True
            except Exception:
                continue

        has_raw_poke, raw_targets = self._extract_raw_poke_targets(event, self_id)
        if any(self._is_same_id(target, self_id) for target in raw_targets):
            return True
        if has_raw_poke and not raw_targets:
            return True
        return has_unknown_target_poke

    def _extract_poke_target(self, segment: Any) -> str | None:
        for field in ("qq", "target", "target_id", "user_id", "uin", "id"):
            try:
                value = getattr(segment, field, None)
            except Exception:
                value = None
            if value:
                return str(value)
        return None

    def _extract_raw_poke_targets(self, event: AstrMessageEvent, self_id: str) -> tuple[bool, list[str]]:
        raw_event = self._get_raw_event(event)
        if raw_event is None:
            return False, []

        has_poke = False
        targets: list[str] = []
        if str(self._read_raw_value(raw_event, "sub_type") or "").lower() == "poke":
            has_poke = True
            self._append_target(targets, self._read_raw_value(raw_event, "target_id"))
            self._append_target(targets, self._read_raw_value(raw_event, "target"))
            self._append_target(targets, self._read_raw_value(raw_event, "qq"))

        raw_message = self._read_raw_value(raw_event, "message")
        if isinstance(raw_message, list):
            for segment in raw_message:
                if str(self._read_raw_value(segment, "type") or "").lower() != "poke":
                    continue
                has_poke = True
                data = self._read_raw_value(segment, "data") or {}
                for field in ("qq", "target", "target_id", "user_id", "uin"):
                    self._append_target(targets, self._read_raw_value(data, field))
                segment_id = self._read_raw_value(data, "id")
                if self._is_same_id(segment_id, self_id):
                    self._append_target(targets, segment_id)

        return has_poke, targets

    def _get_raw_event(self, event: AstrMessageEvent) -> Any:
        for value in (
            getattr(event, "raw_event", None),
            getattr(getattr(event, "message_obj", None), "raw_message", None),
        ):
            if value is not None:
                return value
        return None

    def _read_raw_value(self, data: Any, field: str) -> Any:
        if isinstance(data, dict):
            return data.get(field)
        getter = getattr(data, "get", None)
        if callable(getter):
            try:
                return getter(field)
            except Exception:
                pass
        try:
            return getattr(data, field, None)
        except Exception:
            return None

    def _append_target(self, targets: list[str], value: Any) -> None:
        if self._looks_like_user_id(value):
            text = str(value).strip()
            if text not in targets:
                targets.append(text)

    def _is_same_id(self, value: Any, expected: str) -> bool:
        return bool(value is not None and str(value).strip() == str(expected).strip())

    def _looks_like_user_id(self, value: Any) -> bool:
        if value is None:
            return False
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "undefined"} or text == "0":
            return False
        return not text.isdigit() or len(text) >= 5

    def _consume_dangerous_confirmation(
        self,
        event: AstrMessageEvent,
        *,
        action: str,
        target: str,
        ttl_seconds: float = 60.0,
    ) -> bool:
        now = time()
        self._dangerous_confirmations = {
            key: expires_at
            for key, expires_at in self._dangerous_confirmations.items()
            if expires_at >= now
        }
        key = (
            self._session_key(event),
            str(event.get_sender_id()),
            str(action),
            str(target),
        )
        expires_at = self._dangerous_confirmations.pop(key, 0.0)
        if expires_at >= now:
            return True
        self._dangerous_confirmations[key] = now + max(1.0, float(ttl_seconds))
        return False

    async def _check_admin_permission(self, event: AstrMessageEvent) -> bool:
        if self._is_bot_admin(event):
            return True

        group_id = event.get_group_id()
        if not group_id:
            return False
        try:
            group = await (event.get_group() if hasattr(event, "get_group") else None)
        except Exception as exc:
            logger.info(f"查询群信息失败: {exc}")
            group = None
        if group is None:
            return False

        sender_id = str(event.get_sender_id())
        owner_id = str(getattr(group, "group_owner", "") or "")
        admin_ids = {str(item) for item in getattr(group, "group_admins", [])}
        return bool(
            (owner_id and sender_id == owner_id)
            or sender_id in admin_ids
        )

    def _is_bot_admin(self, event: AstrMessageEvent) -> bool:
        try:
            return bool(getattr(event, "is_admin", None) and event.is_admin())
        except Exception:
            return False

    async def _check_delete_permission(self, event: AstrMessageEvent) -> bool:
        level = str(self.config.get("delete_permission") or "管理员").strip().replace(" ", "")
        if level in {"群员", "member", "普通成员"}:
            return True

        is_bot_admin = self._is_bot_admin(event)

        if level in {"Bot管理员", "bot管理员", "BOT管理员", "bot_admin", "BotAdmin"}:
            return is_bot_admin

        group_id = event.get_group_id()
        if not group_id:
            return is_bot_admin

        is_group_owner = False
        is_group_admin = False
        try:
            group = await (event.get_group() if hasattr(event, "get_group") else None)
        except Exception as exc:
            logger.info(f"查询群信息失败: {exc}")
            group = None
        if group is not None:
            sender_id = str(event.get_sender_id())
            owner_id = str(getattr(group, "group_owner", "") or "")
            admin_ids = [str(item) for item in getattr(group, "group_admins", [])]
            is_group_owner = bool(owner_id and sender_id == owner_id)
            is_group_admin = sender_id in admin_ids

        if level in {"管理员", "admin"}:
            return is_group_admin or is_group_owner or is_bot_admin
        if level in {"群主", "owner"}:
            return is_group_owner or is_bot_admin
        return is_bot_admin

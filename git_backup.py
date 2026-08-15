from __future__ import annotations

import asyncio
import os
import subprocess
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import AsyncContextManager, Callable

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)


DEFAULT_GIT_BACKUP_MESSAGE = "chore: automatic quotes backup ({timestamp})"


@dataclass(frozen=True)
class GitCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class GitBackupResult:
    status: str
    message: str
    committed: bool = False
    pushed: bool = False

    @property
    def command_message(self) -> str:
        if self.status == "pushed":
            if self.committed:
                return "语录备份完成：已提交并推送数据变更。"
            return "语录备份完成：已推送此前未同步的提交。"
        if self.status == "unchanged":
            return "语录备份完成：数据目录没有需要备份的变更。"
        if self.status == "disabled":
            return "语录备份未执行：请先在插件配置中启用 Git 自动备份。"
        if self.status == "error":
            return f"语录备份失败：{self.message}"
        return self.message


StageGuardFactory = Callable[[], AsyncContextManager[None]]


@asynccontextmanager
async def _unlocked_stage_guard():
    yield


class GitBackupService:
    """Periodically commit and push an independently versioned data directory."""

    def __init__(
        self,
        data_root: Path,
        *,
        enabled: bool = False,
        interval_seconds: float = 86400.0,
        commit_message: str = DEFAULT_GIT_BACKUP_MESSAGE,
        command_timeout_seconds: float = 120.0,
        stage_guard_factory: StageGuardFactory | None = None,
    ):
        self.data_root = Path(data_root).resolve()
        self.enabled = bool(enabled)
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.commit_message = str(commit_message or "").strip() or DEFAULT_GIT_BACKUP_MESSAGE
        self.command_timeout_seconds = max(1.0, float(command_timeout_seconds))
        self._stage_guard_factory = stage_guard_factory or _unlocked_stage_guard
        self._backup_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if not self.enabled or (self._task is not None and not self._task.done()):
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Git 自动备份已启动: "
            f"path={self.data_root}, interval={self.interval_seconds:g}s"
        )

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None or task.done():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def backup_once(self) -> GitBackupResult:
        if not self.enabled:
            return GitBackupResult("disabled", "Git 自动备份未启用。")
        async with self._backup_lock:
            return await self._backup_once_locked()

    async def _backup_once_locked(self) -> GitBackupResult:
        logger.info(f"Git 备份开始检查: path={self.data_root}")
        repository_check = await self._run_git("rev-parse", "--show-toplevel")
        if repository_check.returncode != 0:
            return self._error_result(
                "数据目录不是可用的 Git 仓库",
                repository_check,
            )

        try:
            repository_root = Path(repository_check.stdout.strip()).resolve()
        except (OSError, ValueError):
            return GitBackupResult("error", "Git 返回了无效的仓库根目录。")
        if os.path.normcase(str(repository_root)) != os.path.normcase(str(self.data_root)):
            return GitBackupResult(
                "error",
                "数据目录必须是独立 Git 仓库，不能直接使用其父目录中的仓库。",
            )

        upstream = await self._run_git(
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        )
        if upstream.returncode != 0 or not upstream.stdout.strip():
            return self._error_result(
                "当前分支未配置上游分支，请先手动执行一次 git push -u",
                upstream,
            )
        upstream_name = upstream.stdout.strip()
        logger.info(f"Git 备份仓库检查完成: upstream={upstream_name}")

        commit_needed = False
        try:
            async with self._stage_guard_factory():
                status = await self._run_git("status", "--porcelain", "--untracked-files=all")
                if status.returncode != 0:
                    return self._error_result("检查 Git 变更失败", status)

                if status.stdout.strip():
                    logger.info("Git 备份检测到数据变更，执行 git add .")
                    added = await self._run_git("add", ".")
                    if added.returncode != 0:
                        return self._error_result("git add . 失败", added)
                    logger.info("Git 备份 git add . 完成")

                    staged = await self._run_git("diff", "--cached", "--quiet")
                    if staged.returncode == 1:
                        commit_needed = True
                    elif staged.returncode != 0:
                        return self._error_result("检查暂存区失败", staged)
        except Exception as exc:
            return GitBackupResult("error", f"准备一致性备份快照失败：{exc}")

        committed = False
        if commit_needed:
            message = self._render_commit_message()
            logger.info("Git 备份执行 git commit")
            commit = await self._run_git("commit", "-m", message)
            if commit.returncode != 0:
                return self._error_result("git commit 失败", commit)
            committed = True
            logger.info("Git 备份 git commit 完成")

        if not committed:
            ahead = await self._run_git(
                "rev-list",
                "--count",
                f"{upstream_name}..HEAD",
            )
            if ahead.returncode != 0:
                return self._error_result("检查待推送提交失败", ahead)
            try:
                ahead_count = int(ahead.stdout.strip() or "0")
            except ValueError:
                return GitBackupResult("error", "Git 返回了无效的待推送提交数量。")
            logger.info(f"Git 备份待推送提交检查完成: ahead={ahead_count}")
            if ahead_count <= 0:
                return GitBackupResult("unchanged", "数据目录没有需要备份的变更。")

        logger.info(f"Git 备份执行 git push: upstream={upstream_name}")
        pushed = await self._run_git("push")
        if pushed.returncode != 0:
            result = self._error_result("git push 失败", pushed)
            return GitBackupResult(
                result.status,
                result.message,
                committed=committed,
                pushed=False,
            )
        logger.info(f"Git 备份 git push 完成: upstream={upstream_name}")
        return GitBackupResult(
            "pushed",
            "Git 自动备份已提交并推送。" if committed else "待推送的备份提交已推送。",
            committed=committed,
            pushed=True,
        )

    async def _run_loop(self) -> None:
        while True:
            await self._run_once_and_log()
            await asyncio.sleep(self.interval_seconds)

    async def _run_once_and_log(self) -> None:
        try:
            result = await self.backup_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - 后台任务最终兜底
            logger.warning(f"Git 自动备份异常: {exc}")
            return
        if result.status == "pushed":
            logger.info(result.message)
        elif result.status == "error":
            logger.warning(f"Git 自动备份跳过: {result.message}")

    async def _run_git(self, *args: str) -> GitCommandResult:
        process = None
        kwargs: dict[str, object] = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
        }
        git_environment = os.environ.copy()
        git_environment["GIT_TERMINAL_PROMPT"] = "0"
        git_environment["GCM_INTERACTIVE"] = "Never"
        kwargs["env"] = git_environment
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        try:
            process = await asyncio.create_subprocess_exec(
                "git",
                "-C",
                str(self.data_root),
                *args,
                **kwargs,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.command_timeout_seconds,
            )
            return GitCommandResult(
                process.returncode or 0,
                stdout.decode("utf-8", errors="replace").strip(),
                stderr.decode("utf-8", errors="replace").strip(),
            )
        except asyncio.TimeoutError:
            if process is not None:
                process.kill()
                await process.communicate()
            return GitCommandResult(124, stderr="Git 命令执行超时。")
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                process.kill()
                await process.communicate()
            raise
        except (OSError, ValueError) as exc:
            return GitCommandResult(127, stderr=str(exc))

    def _render_commit_message(self) -> str:
        timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
        return self.commit_message.replace("{timestamp}", timestamp)

    def _error_result(
        self,
        prefix: str,
        command: GitCommandResult,
    ) -> GitBackupResult:
        detail = command.stderr.strip() or command.stdout.strip()
        if len(detail) > 500:
            detail = f"{detail[:497]}..."
        message = f"{prefix}：{detail}" if detail else prefix
        return GitBackupResult("error", message)

from __future__ import annotations

import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

from git_backup import GitBackupService, GitCommandResult


class GitBackupServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name).resolve()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _service(self, **kwargs: object) -> GitBackupService:
        return GitBackupService(self.root, enabled=True, **kwargs)

    async def test_disabled_backup_does_not_invoke_git(self) -> None:
        service = GitBackupService(self.root, enabled=False)
        service._run_git = AsyncMock()  # type: ignore[method-assign]

        result = await service.backup_once()

        self.assertEqual(result.status, "disabled")
        service._run_git.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_requires_data_directory_to_be_repository_root(self) -> None:
        service = self._service()
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            return_value=GitCommandResult(0, str(self.root.parent))
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "error")
        self.assertIn("独立 Git 仓库", result.message)
        service._run_git.assert_awaited_once_with(  # type: ignore[attr-defined]
            "rev-parse", "--show-toplevel"
        )

    async def test_unchanged_repository_does_not_commit_or_push(self) -> None:
        service = self._service()
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                GitCommandResult(0, str(self.root)),
                GitCommandResult(0, "origin/main"),
                GitCommandResult(0, ""),
                GitCommandResult(0, "0"),
            ]
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "unchanged")
        self.assertFalse(result.committed)
        self.assertFalse(result.pushed)
        calls = [call.args for call in service._run_git.await_args_list]  # type: ignore[attr-defined]
        self.assertFalse(any(call and call[0] == "commit" for call in calls))
        self.assertFalse(any(call and call[0] == "push" for call in calls))

    async def test_changed_repository_stages_commits_and_pushes_inside_guard(self) -> None:
        guard_events: list[str] = []

        @asynccontextmanager
        async def stage_guard():
            guard_events.append("enter")
            yield
            guard_events.append("exit")

        service = self._service(
            commit_message="backup {timestamp}",
            stage_guard_factory=stage_guard,
        )
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                GitCommandResult(0, str(self.root)),
                GitCommandResult(0, "origin/main"),
                GitCommandResult(0, " M quotes.sqlite3"),
                GitCommandResult(0),
                GitCommandResult(1),
                GitCommandResult(0, "committed"),
                GitCommandResult(0, "pushed"),
            ]
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "pushed")
        self.assertTrue(result.committed)
        self.assertTrue(result.pushed)
        self.assertEqual(guard_events, ["enter", "exit"])
        calls = [call.args for call in service._run_git.await_args_list]  # type: ignore[attr-defined]
        self.assertIn(("add", "."), calls)
        commit_call = next(call for call in calls if call[:2] == ("commit", "-m"))
        self.assertNotIn("{timestamp}", commit_call[2])
        self.assertTrue(commit_call[2].startswith("backup "))
        self.assertEqual(calls[-1], ("push",))

    async def test_unpushed_commit_is_retried_without_new_changes(self) -> None:
        service = self._service()
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                GitCommandResult(0, str(self.root)),
                GitCommandResult(0, "origin/main"),
                GitCommandResult(0, ""),
                GitCommandResult(0, "2"),
                GitCommandResult(0, "pushed"),
            ]
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "pushed")
        self.assertFalse(result.committed)
        self.assertTrue(result.pushed)
        service._run_git.assert_awaited_with("push")  # type: ignore[attr-defined]

    async def test_push_failure_reports_local_commit_for_next_retry(self) -> None:
        service = self._service()
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                GitCommandResult(0, str(self.root)),
                GitCommandResult(0, "origin/main"),
                GitCommandResult(0, "?? quotes.sqlite3"),
                GitCommandResult(0),
                GitCommandResult(1),
                GitCommandResult(0),
                GitCommandResult(1, stderr="remote rejected"),
            ]
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "error")
        self.assertTrue(result.committed)
        self.assertFalse(result.pushed)
        self.assertIn("remote rejected", result.message)

    async def test_stage_guard_failure_is_reported_without_git_mutation(self) -> None:
        @asynccontextmanager
        async def failing_guard():
            raise RuntimeError("checkpoint busy")
            yield

        service = self._service(stage_guard_factory=failing_guard)
        service._run_git = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                GitCommandResult(0, str(self.root)),
                GitCommandResult(0, "origin/main"),
            ]
        )

        result = await service.backup_once()

        self.assertEqual(result.status, "error")
        self.assertIn("checkpoint busy", result.message)
        self.assertEqual(service._run_git.await_count, 2)  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()

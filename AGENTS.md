# Repository Guidelines

## Project Structure & Module Organization

This repository is a flat Python package for an AstrBot quotes plugin. `main.py` registers the plugin and message handlers; keep business workflows in `quote_service.py`. SQLite persistence and migrations belong in `sqlite_store.py`; `store.py` retains legacy JSON migration helpers. Serialized domain objects belong in `models.py`. Platform calls are isolated in `napcat_service.py`, image acquisition and hashing in `image_service.py`, and quote-card generation in `renderer.py`. Shared helpers and fixed values live in `utils.py` and `constants.py`. Tests live in `tests/`. Update `_conf_schema.json` for user-facing configuration changes. Runtime quote data is stored outside the repository under `data/plugin_data/quotes/`.

## Build, Test, and Development Commands

Create an isolated environment and install runtime dependencies:

```powershell
py -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
```

Run a fast syntax/import compilation check before submitting changes:

```powershell
.\.venv\Scripts\python -m compileall .
.\.venv\Scripts\python -m unittest discover -s tests -v
```

There is no standalone executable or build step. For integration checks, load this directory as a plugin in a development AstrBot instance, restart/reload the plugin, and exercise the affected QQ/OneBot command flow.

## Coding Style & Naming Conventions

Follow existing Python style: four-space indentation, type annotations, `snake_case` functions and variables, `PascalCase` classes, and `UPPER_SNAKE_CASE` constants. Preserve `from __future__ import annotations`. Prefer small async service methods for I/O, dataclasses for persisted structures, and explicit fallbacks around optional AstrBot APIs. Keep platform-specific logic out of storage and rendering modules. No formatter or linter is configured, so match nearby code and keep imports grouped consistently.

## Testing Guidelines

Tests use the standard `unittest` framework; no coverage threshold is enforced. Follow `test_<module>.py` / `test_<behavior>` naming. Mock AstrBot, NapCat, HTTP, and filesystem boundaries, and use temporary directories for repository tests. For storage changes, cover new-database CRUD plus both root-level and per-session JSON migration. Also manually verify upload, random retrieval, precise deletion, rendering, and forwarded-message behavior in AstrBot.

## Commit & Pull Request Guidelines

Recent history favors concise Conventional Commit subjects such as `fix: ...`, `feat: ...`, `docs: ...`, and `chore: ...`; use an imperative summary and keep metadata-only bumps separate. Pull requests should explain user-visible behavior, configuration or storage-format impact, and manual verification performed. Link relevant issues and include screenshots when quote-card rendering changes. Never commit runtime quotes, downloaded media, cache files, tokens, or user identifiers.

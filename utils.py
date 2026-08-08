from __future__ import annotations

import hashlib
import json
import re
import secrets
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover
    import logging

    logger = logging.getLogger(__name__)

try:
    from astrbot.core.star.star_tools import StarTools
except Exception:  # pragma: no cover
    StarTools = None  # type: ignore

try:
    from .constants import (
        DEFAULT_ASPECT_RATIO_TOLERANCE,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        IMAGE_POOL_JPEG_QUALITY,
        IMAGE_POOL_MAX_BYTES,
        IMAGE_POOL_MAX_EDGE,
        MEDIA_DIRNAME,
        PLUGIN_NAME,
    )
    from .models import PreparedImage, PreparedMedia
except ImportError:  # pragma: no cover
    from constants import (
        DEFAULT_ASPECT_RATIO_TOLERANCE,
        DEFAULT_DHASH_SIZE,
        DEFAULT_DHASH_THRESHOLD,
        GROUPS_DIRNAME,
        IMAGE_POOL_JPEG_QUALITY,
        IMAGE_POOL_MAX_BYTES,
        IMAGE_POOL_MAX_EDGE,
        MEDIA_DIRNAME,
        PLUGIN_NAME,
    )
    from models import PreparedImage, PreparedMedia


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp_path.replace(path)


def read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - 仅兜底日志
        logger.error(f"读取 JSON 失败 {path}: {exc}")
        return default


def ensure_plugin_data_dir(storage: str | None = None, plugin_name: str = PLUGIN_NAME) -> Path:
    if storage and storage.strip():
        root = Path(storage).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        return root.resolve()

    if StarTools is not None:
        try:
            return StarTools.get_data_dir(plugin_name)
        except Exception as exc:  # pragma: no cover - 兼容旧版 AstrBot
            logger.info(f"StarTools.get_data_dir 回退到 cwd 方案: {exc}")

    root = Path.cwd() / "data" / "plugin_data" / plugin_name
    root.mkdir(parents=True, exist_ok=True)
    return root.resolve()


def resolve_wake_prefixes(config_obj: Any) -> tuple[str, ...]:
    default_prefixes = ("/", "#", "=", "!", "／", "！")
    if config_obj is None:
        return default_prefixes

    wake_prefix: Any = None
    if hasattr(config_obj, "get"):
        try:
            wake_prefix = config_obj.get("wake_prefix")
        except Exception:
            wake_prefix = None

    if wake_prefix is None and isinstance(config_obj, dict):
        wake_prefix = config_obj.get("wake_prefix")

    if wake_prefix is None:
        return default_prefixes

    if isinstance(wake_prefix, str):
        prefixes = [wake_prefix]
    elif isinstance(wake_prefix, (list, tuple, set)):
        prefixes = [str(item) for item in wake_prefix if str(item)]
    else:
        return default_prefixes

    merged: list[str] = []
    seen: set[str] = set()
    for prefix in [*prefixes, *default_prefixes]:
        value = str(prefix or "")
        if not value or value in seen:
            continue
        merged.append(value)
        seen.add(value)
    return tuple(merged)


def make_session_key(group_id: str | None, sender_id: str | None) -> str:
    gid = str(group_id or "").strip()
    if gid:
        return gid
    return f"private_{str(sender_id or '').strip()}"


def is_valid_qq(value: str | None) -> bool:
    value = str(value or "").strip()
    return value.isdigit() and len(value) >= 5


def normalize_quote_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"@[^@\s（）()]+(?:[（(]\d{5,}[）)])?", "", text)
    text = text.replace("@全体成员", "")
    return " ".join(text.split())


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def guess_extension(source_name: str = "", content_type: str = "") -> str:
    lowered_type = (content_type or "").lower()
    if "png" in lowered_type:
        return ".png"
    if "webp" in lowered_type:
        return ".webp"
    if "gif" in lowered_type:
        return ".gif"
    if "bmp" in lowered_type:
        return ".bmp"
    if source_name:
        suffix = Path(source_name).suffix.lower()
        if 0 < len(suffix) <= 10:
            return suffix
    return ".jpg"


def guess_media_extension(source_name: str = "", content_type: str = "", media_type: str = "") -> str:
    if source_name:
        suffix = Path(source_name).suffix.lower()
        if 0 < len(suffix) <= 10:
            return suffix

    lowered_type = (content_type or "").lower()
    mappings = {
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/ogg": ".ogg",
        "audio/amr": ".amr",
        "audio/aac": ".aac",
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "video/x-msvideo": ".avi",
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/json": ".json",
        "text/plain": ".txt",
    }
    for key, extension in mappings.items():
        if key in lowered_type:
            return extension

    default_map = {
        "record": ".wav",
        "video": ".mp4",
        "file": ".bin",
    }
    return default_map.get(str(media_type or "").lower(), ".bin")


def compute_dhash(content: bytes, hash_size: int = DEFAULT_DHASH_SIZE) -> tuple[str, int, int]:
    try:
        from io import BytesIO

        with Image.open(BytesIO(content)) as img:
            width, height = img.size
            if getattr(img, "is_animated", False):
                img.seek(0)
            gray = img.convert("L").resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
            if hasattr(gray, "get_flattened_data"):
                pixels = list(gray.get_flattened_data())
            else:  # Pillow < 14
                pixels = list(gray.getdata())
    except Exception:
        return "", 0, 0

    bits: list[str] = []
    row_width = hash_size + 1
    for row in range(hash_size):
        row_offset = row * row_width
        for col in range(hash_size):
            left = pixels[row_offset + col]
            right = pixels[row_offset + col + 1]
            bits.append("1" if left > right else "0")

    return f"{int(''.join(bits), 2):0{hash_size * hash_size // 4}x}", width, height


def _is_animated_gif(content: bytes) -> bool:
    if not content:
        return False
    try:
        with Image.open(BytesIO(content)) as image:
            return bool(
                image.format == "GIF"
                and getattr(image, "is_animated", False)
                and int(getattr(image, "n_frames", 1) or 1) > 1
            )
    except Exception:
        return False


def prepare_image(content: bytes, *, source: str = "", content_type: str = "") -> PreparedImage:
    animated_gif = _is_animated_gif(content)
    normalized = content if animated_gif else compress_image_for_pool(content)
    dhash, width, height = compute_dhash(normalized)
    return PreparedImage(
        content=normalized,
        extension=".gif" if animated_gif else ".jpg",
        sha256=sha256_bytes(normalized),
        source=source,
        dhash=dhash,
        width=width,
        height=height,
    )


def compress_image_for_pool(
    content: bytes,
    *,
    max_edge: int = IMAGE_POOL_MAX_EDGE,
    max_bytes: int = IMAGE_POOL_MAX_BYTES,
    initial_quality: int = IMAGE_POOL_JPEG_QUALITY,
) -> bytes:
    """Preserve animated GIFs; compress other images into the pool JPEG format."""
    if not content:
        raise ValueError("图片内容为空")
    if _is_animated_gif(content):
        return content

    with Image.open(BytesIO(content)) as source:
        if getattr(source, "is_animated", False):
            source.seek(0)
        source.load()
        frame = ImageOps.exif_transpose(source)
        frame.thumbnail((max_edge, max_edge), Image.Resampling.LANCZOS)

        if frame.mode in {"RGBA", "LA"} or (
            frame.mode == "P" and "transparency" in frame.info
        ):
            rgba = frame.convert("RGBA")
            rgb = Image.new("RGB", rgba.size, "white")
            rgb.paste(rgba, mask=rgba.getchannel("A"))
        else:
            rgb = frame.convert("RGB")

        quality = max(68, min(95, int(initial_quality)))
        for _ in range(8):
            output = BytesIO()
            rgb.save(output, format="JPEG", quality=quality, optimize=True)
            normalized = output.getvalue()
            if len(normalized) <= max_bytes:
                return normalized

            width, height = rgb.size
            if width <= 640 and height <= 640:
                break
            rgb = rgb.resize(
                (max(1, int(width * 0.82)), max(1, int(height * 0.82))),
                Image.Resampling.LANCZOS,
            )
            quality = max(68, quality - 4)

    raise ValueError(f"图片规范化后仍超过 {max_bytes} 字节")


def normalize_image_file_for_send(
    path: Path,
    *,
    max_edge: int = IMAGE_POOL_MAX_EDGE,
    max_bytes: int = IMAGE_POOL_MAX_BYTES,
) -> bytes:
    """Load a pool image, preserving GIFs or upgrading legacy static images."""
    content = Path(path).read_bytes()
    if not content:
        raise ValueError("图片文件为空")
    if _is_animated_gif(content):
        return content

    try:
        with Image.open(BytesIO(content)) as source:
            is_canonical = (
                source.format == "JPEG"
                and not getattr(source, "is_animated", False)
                and max(source.size) <= max_edge
                and len(content) <= max_bytes
            )
            source.verify()
        if is_canonical:
            return content
    except Exception:
        pass

    return compress_image_for_pool(
        content,
        max_edge=max_edge,
        max_bytes=max_bytes,
    )


def prepare_media(
    content: bytes,
    *,
    media_type: str,
    source: str = "",
    content_type: str = "",
    display_name: str = "",
) -> PreparedMedia:
    extension = guess_media_extension(
        source_name=display_name or source,
        content_type=content_type,
        media_type=media_type,
    )
    return PreparedMedia(
        content=content,
        extension=extension,
        media_type=media_type,
        source=source,
        display_name=display_name or Path(source).name or f"{media_type}{extension}",
    )


def hamming_distance(left_hex: str, right_hex: str) -> int:
    if not left_hex or not right_hex:
        return 10**9
    return (int(left_hex, 16) ^ int(right_hex, 16)).bit_count()


def is_near_duplicate(
    left: PreparedImage,
    right_sha256: str,
    right_dhash: str,
    right_width: int,
    right_height: int,
    *,
    dhash_threshold: int = DEFAULT_DHASH_THRESHOLD,
    aspect_ratio_tolerance: float = DEFAULT_ASPECT_RATIO_TOLERANCE,
) -> bool:
    if left.sha256 and left.sha256 == right_sha256:
        return True
    if not left.dhash or not right_dhash:
        return False
    if hamming_distance(left.dhash, right_dhash) > dhash_threshold:
        return False

    left_ratio = left.aspect_ratio
    if not left_ratio or right_width <= 0 or right_height <= 0:
        return False
    right_ratio = right_width / right_height
    if right_ratio <= 0:
        return False
    ratio_diff = abs(left_ratio - right_ratio) / max(left_ratio, right_ratio)
    return ratio_diff <= aspect_ratio_tolerance


def random_id(prefix: str = "") -> str:
    from time import time

    value = f"{int(time() * 1000)}_{secrets.token_hex(4)}"
    return f"{prefix}{value}" if prefix else value


def rel_image_path(session_key: str, file_name: str) -> str:
    return f"{GROUPS_DIRNAME}/{session_key}/images/{file_name}"


def rel_media_path(session_key: str, file_name: str) -> str:
    return f"{GROUPS_DIRNAME}/{session_key}/{MEDIA_DIRNAME}/{file_name}"

from __future__ import annotations

PLUGIN_NAME = "quotes"
SCHEMA_VERSION = 3
DATABASE_SCHEMA_VERSION = 4
DATABASE_FILENAME = "quotes.sqlite3"
DUPLICATE_IMAGE_MESSAGE = "语录图片已存在"
DUPLICATE_QUOTE_MESSAGE = "语录内容已存在"
UPLOAD_SUCCESS_PROMPT = "我学会啦，来问问我吧！高性能ですから~"
GALLERY_RECENT_WINDOW = 20
MAX_GALLERY_SENT_RECORDS = 200
QUOTE_EVENT_LIMIT = 4
QUOTE_EVENT_WINDOW_SECONDS = 600
QUOTE_RATE_LIMIT_MESSAGES = (
    "你是不是暗恋{target}？{seconds} 秒后再试吧。",
    "对{target}这么上心吗？先冷静 {seconds} 秒吧。",
    "语录也需要喘口气，{seconds} 秒后再来找{target}吧。",
    "高性能也顶不住这样连点呀，{seconds} 秒后再试。",
    "{target}都要被你念害羞啦，休息 {seconds} 秒吧。",
    "检测到对{target}的热烈关注，冷却还剩 {seconds} 秒。",
)
GROUPS_DIRNAME = "groups"
IMAGES_DIRNAME = "images"
MEDIA_DIRNAME = "media"
CACHE_DIRNAME = "cache"
IMAGE_INDEX_FILENAME = "image_index.json"
MEDIA_INDEX_FILENAME = "media_index.json"
SENT_INDEX_FILENAME = "sent_index.json"
QUOTES_FILENAME = "quotes.json"
LEGACY_QUOTES_BAK_SUFFIX = ".bak"
DEFAULT_DHASH_THRESHOLD = 4
DEFAULT_DHASH_SIZE = 8
DEFAULT_ASPECT_RATIO_TOLERANCE = 0.08
MAX_SENT_RECORDS = 200
IMAGE_POOL_MAX_EDGE = 2560
IMAGE_POOL_MAX_BYTES = 1536 * 1024
IMAGE_POOL_JPEG_QUALITY = 86

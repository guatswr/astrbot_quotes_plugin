from __future__ import annotations

import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image

from constants import IMAGE_POOL_MAX_BYTES, IMAGE_POOL_MAX_EDGE
from utils import normalize_image_file_for_send, prepare_image, sha256_bytes


class ImageCompressionTests(unittest.TestCase):
    def test_upload_image_is_compressed_into_canonical_pool_format(self) -> None:
        source = BytesIO()
        Image.new("RGBA", (3200, 1800), (30, 90, 180, 128)).save(
            source,
            format="PNG",
        )

        prepared = prepare_image(source.getvalue(), source="large-transparent.png")

        self.assertEqual(prepared.extension, ".jpg")
        self.assertEqual(prepared.sha256, sha256_bytes(prepared.content))
        self.assertLessEqual(len(prepared.content), IMAGE_POOL_MAX_BYTES)
        self.assertLessEqual(max(prepared.width, prepared.height), IMAGE_POOL_MAX_EDGE)
        with Image.open(BytesIO(prepared.content)) as pooled:
            self.assertEqual(pooled.format, "JPEG")
            self.assertEqual(pooled.mode, "RGB")

    def test_send_path_reuses_canonical_pool_bytes(self) -> None:
        source = BytesIO()
        Image.new("RGB", (640, 480), "navy").save(source, format="PNG")
        prepared = prepare_image(source.getvalue(), source="small.png")

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "img_pool.jpg"
            path.write_bytes(prepared.content)

            self.assertEqual(
                normalize_image_file_for_send(path),
                prepared.content,
            )


if __name__ == "__main__":
    unittest.main()

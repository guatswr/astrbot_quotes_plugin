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

    def test_animated_gif_is_preserved_byte_for_byte(self) -> None:
        source = BytesIO()
        first = Image.new("RGBA", (48, 32), (255, 0, 0, 255))
        second = Image.new("RGBA", (48, 32), (0, 0, 255, 255))
        first.save(
            source,
            format="GIF",
            save_all=True,
            append_images=[second],
            duration=[80, 120],
            loop=0,
            disposal=2,
        )
        original = source.getvalue()

        prepared = prepare_image(
            original,
            source="renamed-image.bin",
            content_type="application/octet-stream",
        )

        self.assertEqual(prepared.extension, ".gif")
        self.assertEqual(prepared.content, original)
        self.assertEqual(prepared.sha256, sha256_bytes(original))
        with Image.open(BytesIO(prepared.content)) as animated:
            self.assertEqual(animated.format, "GIF")
            self.assertTrue(animated.is_animated)
            self.assertEqual(animated.n_frames, 2)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "img_pool.gif"
            path.write_bytes(original)
            self.assertEqual(normalize_image_file_for_send(path), original)


if __name__ == "__main__":
    unittest.main()

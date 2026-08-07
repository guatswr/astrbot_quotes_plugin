from __future__ import annotations

import unittest

from models import Quote
from renderer import QuoteRenderer


class QuoteRendererTests(unittest.IsolatedAsyncioTestCase):
    async def test_templates_skip_unused_shiki_runtime_injection(self) -> None:
        templates: list[str] = []

        async def fake_html_render(
            template: str,
            data: dict,
            return_url: bool,
            options: dict | None,
        ) -> str:
            templates.append(template)
            return "rendered.png"

        renderer = QuoteRenderer(fake_html_render, {})
        await renderer.warmup()
        await renderer.render_quote_image(
            Quote(
                id="q_renderer",
                qq="10001",
                name="测试用户",
                text="渲染测试",
                created_by="20002",
                created_at=1.0,
                group="123456",
            ),
            "名言",
        )

        self.assertEqual(len(templates), 2)
        for template in templates:
            self.assertIn(QuoteRenderer.SHIKI_RUNTIME_MARKER, template)
        self.assertIn('<div class="signature">— 名言</div>', templates[1])


if __name__ == "__main__":
    unittest.main()

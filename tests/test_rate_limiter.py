from __future__ import annotations

import unittest

from rate_limiter import SlidingWindowRateLimiter


class SlidingWindowRateLimiterTests(unittest.TestCase):
    def test_fifth_event_warns_once_then_stays_silent(self) -> None:
        limiter = SlidingWindowRateLimiter(limit=4, window_seconds=120)
        key = ("group-1", "user-1")

        for timestamp in (0, 10, 20, 30):
            result = limiter.check(key, now=timestamp)
            self.assertTrue(result.allowed)

        fifth = limiter.check(key, now=40)
        self.assertFalse(fifth.allowed)
        self.assertTrue(fifth.notify)
        self.assertEqual(fifth.retry_after, 80)

        repeated = limiter.check(key, now=50)
        self.assertFalse(repeated.allowed)
        self.assertFalse(repeated.notify)
        self.assertEqual(repeated.retry_after, 70)

        almost_ready = limiter.check(key, now=119.1)
        self.assertFalse(almost_ready.allowed)
        self.assertFalse(almost_ready.notify)
        self.assertEqual(almost_ready.retry_after, 1)

    def test_limit_recovers_when_oldest_event_leaves_window(self) -> None:
        limiter = SlidingWindowRateLimiter(limit=4, window_seconds=120)
        key = ("group-1", "user-1")
        for timestamp in (0, 10, 20, 30):
            limiter.check(key, now=timestamp)
        limiter.check(key, now=40)

        recovered = limiter.check(key, now=120)
        self.assertTrue(recovered.allowed)

        blocked_again = limiter.check(key, now=121)
        self.assertFalse(blocked_again.allowed)
        self.assertTrue(blocked_again.notify)
        self.assertEqual(blocked_again.retry_after, 9)

    def test_users_and_sessions_are_independent(self) -> None:
        limiter = SlidingWindowRateLimiter(limit=1, window_seconds=120)

        self.assertTrue(limiter.check(("group-1", "user-1"), now=0).allowed)
        self.assertFalse(limiter.check(("group-1", "user-1"), now=1).allowed)
        self.assertTrue(limiter.check(("group-1", "user-2"), now=1).allowed)
        self.assertTrue(limiter.check(("group-2", "user-1"), now=1).allowed)


if __name__ == "__main__":
    unittest.main()

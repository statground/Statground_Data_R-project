#!/usr/bin/env python3

from __future__ import annotations

import urllib.error
import unittest
from unittest import mock

import verify_web_r_cdn_release as verify


class VerifyWebRCDNReleaseTest(unittest.TestCase):
    def test_head_status_keeps_builtin_timeout_inside_retry_loop(self) -> None:
        with mock.patch.object(
            verify.urllib.request,
            "urlopen",
            side_effect=TimeoutError("read timed out"),
        ):
            self.assertEqual(0, verify.head_status("https://cdn.example.invalid/manifest.json"))

    def test_head_status_keeps_url_error_inside_retry_loop(self) -> None:
        with mock.patch.object(
            verify.urllib.request,
            "urlopen",
            side_effect=urllib.error.URLError("temporary DNS failure"),
        ):
            self.assertEqual(0, verify.head_status("https://cdn.example.invalid/manifest.json"))


if __name__ == "__main__":
    unittest.main()

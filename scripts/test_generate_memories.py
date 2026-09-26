"""Regression tests for the memory generator's HTTP contract."""

import asyncio
import contextlib
import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).with_name("generate_memories.py")
SPEC = importlib.util.spec_from_file_location("generate_memories", SCRIPT)
assert SPEC and SPEC.loader
generator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generator)


class _Response:
    status_code = 200

    def raise_for_status(self) -> None:
        pass


class _Client:
    headers: dict[str, str] | None = None

    def __init__(self, *, headers: dict[str, str]) -> None:
        type(self).headers = headers

    async def __aenter__(self) -> "_Client":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def get(self, *args: object, **kwargs: object) -> _Response:
        return _Response()


class GenerateMemoriesAuthTest(unittest.TestCase):
    def test_random_memory_uses_the_create_api_field_names(self) -> None:
        payload = generator.random_memory()

        self.assertIn("policy", payload)
        self.assertNotIn("memory_type", payload)
        if payload["policy"] == "short_term":
            self.assertIn("ttl_seconds", payload)
            self.assertNotIn("ttl", payload)

    def test_run_sends_the_api_key_as_a_bearer_token(self) -> None:
        original_client = generator.httpx.AsyncClient
        generator.httpx.AsyncClient = _Client
        try:
            with contextlib.redirect_stdout(None):
                asyncio.run(generator.run("http://example.test/api/v1/memories", 0, 1, "test-key", 0))
        finally:
            generator.httpx.AsyncClient = original_client

        self.assertEqual(_Client.headers, {"Authorization": "Bearer test-key"})


if __name__ == "__main__":
    unittest.main()

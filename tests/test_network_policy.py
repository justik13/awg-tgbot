import os
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("SERVER_PUBLIC_KEY", "test-public-key")
os.environ.setdefault("SERVER_IP", "1.1.1.1:51820")
os.environ.setdefault("ENCRYPTION_SECRET", "test-secret")

import network_policy


class NetworkPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_domains_batches_dns_lookups_concurrently(self):
        active = 0
        max_active = 0
        seen: list[str] = []

        class FakeLoop:
            async def getaddrinfo(self, domain, port, type):
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                seen.append(domain)
                try:
                    await network_policy.asyncio.sleep(0)
                    host_id = int(domain.removeprefix("domain").removesuffix(".example"))
                    return [(socket.AF_INET, None, None, None, (f"192.0.2.{host_id}", port))]
                finally:
                    active -= 1

        domains = ",".join(f"domain{i}.example" for i in range(40))
        with patch("network_policy.asyncio.get_running_loop", return_value=FakeLoop()):
            resolved = await network_policy.resolve_domains(domains)

        self.assertEqual(len(resolved), 40)
        self.assertEqual(len(seen), 40)
        self.assertGreater(max_active, 1)
        self.assertLessEqual(max_active, network_policy.DENYLIST_DNS_CONCURRENCY)

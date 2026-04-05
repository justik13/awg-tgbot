from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import ipaddress
import socket

from config import logger
from content_settings import get_setting
from database import get_metric, increment_metric, set_metric, write_audit_log

DENYLIST_DNS_TIMEOUT_SECONDS = 2.0
DENYLIST_MAX_RESOLVED_CIDRS = 4096


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _domain_to_ascii(domain: str) -> str:
    domain = (domain or "").strip().rstrip(".")
    if not domain:
        return ""
    try:
        return domain.encode("idna").decode("ascii")
    except Exception:
        return domain


def parse_cidrs(raw: str) -> list[str]:
    cidrs: list[str] = []
    for item in _parse_csv(raw):
        network = ipaddress.ip_network(item, strict=False)
        cidrs.append(str(network))
    return cidrs


async def resolve_domains(domains_raw: str) -> list[str]:
    resolved: set[str] = set()
    loop = asyncio.get_running_loop()
    for domain in _parse_csv(domains_raw):
        query_domain = _domain_to_ascii(domain)
        if not query_domain:
            continue
        try:
            addrinfo = await asyncio.wait_for(
                loop.getaddrinfo(query_domain, 443, type=socket.SOCK_STREAM),
                timeout=DENYLIST_DNS_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning("denylist resolve timeout for domain=%s", domain)
            continue
        except OSError as error:
            if getattr(error, "errno", None) == -2:
                logger.info("denylist domain has no DNS records now: %s", domain)
            else:
                logger.warning("denylist resolve failed for domain=%s: %s", domain, error)
            continue
        for family, _, _, _, sockaddr in addrinfo:
            if family == socket.AF_INET:
                resolved.add(f"{sockaddr[0]}/32")
                if len(resolved) >= DENYLIST_MAX_RESOLVED_CIDRS:
                    return sorted(resolved)
    return sorted(resolved)


async def denylist_sync(run_docker) -> None:
    enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0) == 1
    if not enabled:
        try:
            await denylist_clear(run_docker)
            await set_metric("denylist_entries", 0)
        except Exception as e:
            await increment_metric("denylist_errors")
            logger.warning("denylist_clear failed while denylist is disabled: %s", e)
        return
    mode = str(await get_setting("EGRESS_DENYLIST_MODE", str) or "soft").strip().lower()
    vpn_prefix = str(await get_setting("VPN_SUBNET_PREFIX", str) or "10.8.1.")
    vpn_subnet = f"{vpn_prefix}0/24"
    domains = str(await get_setting("EGRESS_DENYLIST_DOMAINS", str) or "")
    cidrs = str(await get_setting("EGRESS_DENYLIST_CIDRS", str) or "")
    try:
        resolved = await resolve_domains(domains)
        cidr_values = parse_cidrs(cidrs)
    except Exception as e:
        await increment_metric("denylist_errors")
        await set_metric("denylist_last_sync_ok", 0)
        if mode == "strict":
            raise
        logger.warning("denylist parse/resolve failed in soft mode: %s", e)
        return
    payload = "\n".join(sorted(set(resolved + cidr_values)))
    try:
        await run_docker(["denylist-sync", "--vpn-subnet", vpn_subnet], input_data=payload)
        await set_metric("denylist_last_sync_ok", 1)
        await set_metric("denylist_last_sync_ts", int(datetime.utcnow().timestamp()))
        await set_metric("denylist_entries", len([line for line in payload.splitlines() if line.strip()]))
    except Exception as e:
        await increment_metric("denylist_errors")
        await set_metric("denylist_last_sync_ok", 0)
        if mode == "strict":
            raise
        logger.warning("denylist_sync failed in soft mode: %s", e)


async def denylist_clear(run_docker) -> None:
    vpn_prefix = str(await get_setting("VPN_SUBNET_PREFIX", str) or "10.8.1.")
    vpn_subnet = f"{vpn_prefix}0/24"
    await run_docker(["denylist-clear", "--vpn-subnet", vpn_subnet])


async def denylist_should_refresh() -> bool:
    enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0) == 1
    if not enabled:
        return False
    refresh_minutes = int(await get_setting("EGRESS_DENYLIST_REFRESH_MINUTES", int) or 30)
    last_ts = await get_metric("denylist_last_sync_ts")
    if last_ts <= 0:
        return True
    return datetime.utcnow() >= datetime.utcfromtimestamp(last_ts) + timedelta(minutes=max(refresh_minutes, 1))


async def policy_metrics() -> dict[str, int]:
    return {
        "denylist_errors": await get_metric("denylist_errors"),
        "denylist_last_sync_ok": await get_metric("denylist_last_sync_ok"),
        "denylist_last_sync_ts": await get_metric("denylist_last_sync_ts"),
        "denylist_entries": await get_metric("denylist_entries"),
    }

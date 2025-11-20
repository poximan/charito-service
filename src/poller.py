import logging
import threading
import time
from typing import Iterable

import requests

from config import Target
from identity import fetch_instance_id
from state import StateStore, _utc_now_iso
from broadcast import broadcast_whitelist


class CharitoPoller:
    def __init__(
            self,
            targets: Iterable[Target],
            state: StateStore,
            poll_interval: int,
            request_timeout: float,
    ) -> None:
        self._targets = list(targets)
        self._state = state
        self._interval = max(5, poll_interval)
        self._timeout = max(1.0, request_timeout)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._registry: dict[str, dict] = {}
        for target in self._targets:
            key = target.tracking_key
            self._registry[target.identity_url] = {
                "target": target,
                "key": key,
                "alias": target.alias,
                "resolved": bool(target.instance_id),
            }
            self._state.ensure_placeholder(key, target.alias)
        self._identities: dict[str, str] = {
            target.identity_url: target.instance_id
            for target in self._targets
            if target.instance_id
        }
        self._log = logging.getLogger("charito.poller")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="charito-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            for target in self._targets:
                if self._stop.is_set():
                    break
                self._poll_target(target)
            self._stop.wait(self._interval)

    def _poll_target(self, target: Target) -> None:
        registry = self._registry.get(target.identity_url) or {}
        key_hint = registry.get("key") or target.tracking_key
        alias = registry.get("alias") or target.alias
        instance_id = self._ensure_identity(target)
        effective_id = instance_id or key_hint
        try:
            response = requests.get(target.metrics_url, timeout=self._timeout)
            response.raise_for_status()
            metrics = response.json()
            payload = self._build_payload(effective_id, metrics, alias)
            self._state.upsert_online(payload, key_hint=key_hint, alias=alias)
            if instance_id and not registry.get("resolved"):
                registry["resolved"] = True
                registry["key"] = instance_id
                broadcast_whitelist(self._targets, overrides=self._current_keys())
        except Exception:
            self._state.mark_offline(effective_id, alias=alias)

    def _build_payload(self, instance_id: str, metrics: dict, alias: str) -> dict:
        cpu_load = metrics.get("cpuLoad", -1.0)
        cpu_temp = metrics.get("cpuTemperatureCelsius", -1.0)
        total_mem = metrics.get("totalMemoryBytes", 0)
        free_mem = metrics.get("freeMemoryBytes", 0)
        used_ratio = None
        if total_mem and free_mem is not None:
            used_ratio = 1.0 - (free_mem / max(total_mem, 1))
        network_info = self._extract_interfaces(metrics)
        payload = {
            "instanceId": instance_id,
            "alias": alias,
            "generatedAt": metrics.get("timestamp", _utc_now_iso()),
            "samples": 1,
            "windowSeconds": self._interval,
            "averageCpuLoad": cpu_load,
            "averageCpuTemperatureCelsius": cpu_temp,
            "averageMemoryUsageRatio": used_ratio if used_ratio is not None else -1.0,
            "averageFreeMemoryBytes": free_mem,
            "averageTotalMemoryBytes": total_mem,
            "networkInterfaces": network_info,
            "latestSample": self._latest_sample(metrics, network_info),
        }
        return payload

    def _latest_sample(self, metrics: dict, network_info: list | None = None) -> dict:
        sample = {
            "timestamp": metrics.get("timestamp"),
            "cpuLoad": metrics.get("cpuLoad"),
            "cpuTemperatureCelsius": metrics.get("cpuTemperatureCelsius"),
            "totalMemoryBytes": metrics.get("totalMemoryBytes"),
            "freeMemoryBytes": metrics.get("freeMemoryBytes"),
            "watchedProcesses": metrics.get("watchedProcesses", []),
        }
        sample["networkInterfaces"] = network_info if network_info is not None else self._extract_interfaces(metrics)
        return sample

    def _ensure_identity(self, target: Target) -> str | None:
        registry = self._registry.get(target.identity_url) or {}
        if registry.get("resolved"):
            return registry.get("key")
        cached = self._identities.get(target.identity_url)
        if cached:
            registry["resolved"] = True
            registry["key"] = cached
            return cached
        try:
            instance_id = fetch_instance_id(target, self._timeout)
        except Exception as exc:
            self._log.warning("No se pudo resolver instanceId para %s: %s", target.identity_url, exc)
            return None
        if not instance_id:
            self._log.warning("Endpoint de identidad no devolvio instanceId en %s", target.identity_url)
            return None
        self._identities[target.identity_url] = instance_id
        registry["resolved"] = True
        registry["key"] = instance_id
        broadcast_whitelist(self._targets, overrides=self._current_keys())
        self._prune_known_instances()
        return instance_id

    def _prune_known_instances(self) -> None:
        allowed = [entry["key"] for entry in self._registry.values() if entry.get("key")]
        if not allowed:
            return
        self._state.prune(allowed)

    def _current_keys(self) -> dict[str, str]:
        return {identity_url: entry["key"] for identity_url, entry in self._registry.items() if entry.get("key")}

    def _extract_interfaces(self, metrics: dict) -> list:
        interfaces = metrics.get("networkInterfaces") or []
        cleaned: list = []
        for entry in interfaces:
            if not isinstance(entry, dict):
                continue
            addresses = []
            for info in entry.get("addresses") or []:
                if not isinstance(info, dict):
                    continue
                address = str(info.get("address") or "").strip()
                netmask = str(info.get("netmask") or "").strip()
                addresses.append(
                    {
                        "address": address,
                        "netmask": netmask,
                    }
                )
            cleaned.append(
                {
                    "name": str(entry.get("name") or ""),
                    "displayName": str(entry.get("displayName") or ""),
                    "path": str(entry.get("path") or ""),
                    "macAddress": str(entry.get("macAddress") or ""),
                    "up": bool(entry.get("up")),
                    "virtual": bool(entry.get("virtual")),
                    "addresses": addresses,
                }
            )
        return cleaned

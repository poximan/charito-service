import logging
import threading
import time
from typing import Iterable

import requests

from config import Target
from identity import fetch_instance_id
from state import StateStore, _utc_now_iso


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
        instance_id = self._ensure_identity(target)
        if instance_id is None:
            return
        try:
            response = requests.get(target.metrics_url, timeout=self._timeout)
            response.raise_for_status()
            metrics = response.json()
            payload = self._build_payload(instance_id, metrics)
            self._state.upsert_online(payload)
        except Exception:
            self._state.mark_offline(instance_id)

    def _build_payload(self, instance_id: str, metrics: dict) -> dict:
        cpu_load = metrics.get("cpuLoad", -1.0)
        cpu_temp = metrics.get("cpuTemperatureCelsius", -1.0)
        total_mem = metrics.get("totalMemoryBytes", 0)
        free_mem = metrics.get("freeMemoryBytes", 0)
        used_ratio = None
        if total_mem and free_mem is not None:
            used_ratio = 1.0 - (free_mem / max(total_mem, 1))
        payload = {
            "instanceId": instance_id,
            "generatedAt": metrics.get("timestamp", _utc_now_iso()),
            "samples": 1,
            "windowSeconds": self._interval,
            "averageCpuLoad": cpu_load,
            "averageCpuTemperatureCelsius": cpu_temp,
            "averageMemoryUsageRatio": used_ratio if used_ratio is not None else -1.0,
            "averageFreeMemoryBytes": free_mem,
            "averageTotalMemoryBytes": total_mem,
            "latestSample": self._latest_sample(metrics),
        }
        return payload

    def _latest_sample(self, metrics: dict) -> dict:
        return {
            "timestamp": metrics.get("timestamp"),
            "cpuLoad": metrics.get("cpuLoad"),
            "cpuTemperatureCelsius": metrics.get("cpuTemperatureCelsius"),
            "totalMemoryBytes": metrics.get("totalMemoryBytes"),
            "freeMemoryBytes": metrics.get("freeMemoryBytes"),
            "watchedProcesses": metrics.get("watchedProcesses", []),
        }

    def _ensure_identity(self, target: Target) -> str | None:
        cached = self._identities.get(target.identity_url)
        if cached:
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
        self._prune_known_instances()
        return instance_id

    def _prune_known_instances(self) -> None:
        if not self._identities:
            return
        self._state.prune(self._identities.values())

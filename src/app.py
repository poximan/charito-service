import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException

from config import load_service_config
from broadcast import broadcast_whitelist
from poller import CharitoPoller
from state import StateStore


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        return default


DATA_DIR = Path(os.getenv("CHARITO_DATA_DIR", "/app/data"))
STATE_FILE = Path(os.getenv("CHARITO_STATE_FILE", DATA_DIR / "charito-state.json"))
TARGETS_FILE = os.getenv("CHARITO_TARGETS_FILE", "config/targets.json")
service_cfg = load_service_config(TARGETS_FILE)
POLL_INTERVAL = _env_int("CHARITO_POLL_INTERVAL_SECONDS", service_cfg.poll_interval_seconds)
HTTP_TIMEOUT = _env_float("CHARITO_HTTP_TIMEOUT_SECONDS", service_cfg.http_timeout_seconds)

app = FastAPI(title="charito-service", version="2.0.0")

os.makedirs(DATA_DIR, exist_ok=True)
state_store = StateStore(Path(STATE_FILE))
targets = service_cfg.instances

poller = CharitoPoller(
    targets=targets,
    state=state_store,
    poll_interval=POLL_INTERVAL,
    request_timeout=HTTP_TIMEOUT,
)


@app.on_event("startup")
def startup_event() -> None:
    try:
        broadcast_whitelist(targets)
    except Exception:
        pass
    poller.start()


@app.on_event("shutdown")
def shutdown_event() -> None:
    poller.stop()


@app.get("/health")
def health() -> dict:
    return {"status": "up", "targets": len(targets)}


@app.get("/api/charito/instances")
def list_instances(since: Optional[str] = None) -> dict:
    return state_store.build_index(since)


@app.get("/api/charito/instances/{instance_id}")
def get_instance(instance_id: str) -> dict:
    data = state_store.build_state([instance_id]).get("items", [])
    if not data:
        raise HTTPException(status_code=404, detail="instance not found")
    return data[0]


@app.get("/api/charito/state")
def full_state(ids: Optional[str] = None) -> dict:
    subset = None
    if ids:
        subset = [part.strip() for part in ids.split(",") if part.strip()]
    return state_store.build_state(subset)

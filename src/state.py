import json
import threading
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from timeauthority import get_time_authority

_AUTH = get_time_authority()


class StateStore:
    def __init__(self, data_path: Path) -> None:
        self._data_path = data_path
        self._data_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._items: Dict[str, Dict] = {}
        self._load()

    def _load(self) -> None:
        if not self._data_path.exists():
            return
        try:
            raw = json.loads(self._data_path.read_text(encoding="utf-8"))
            for entry in raw.get("items", []):
                if isinstance(entry, dict) and entry.get("instanceId"):
                    self._items[str(entry["instanceId"])] = entry
        except Exception:
            self._items.clear()

    def _persist(self) -> None:
        snapshot = {"ts": _AUTH.utc_iso(), "items": list(self._items.values())}
        self._data_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def upsert_online(self, payload: Dict, key_hint: Optional[str] = None, alias: Optional[str] = None) -> None:
        instance_id = str(payload.get("instanceId") or "").strip()
        key_candidate = str(key_hint or instance_id or "").strip()
        if not key_candidate:
            return
        key = instance_id or key_candidate
        payload = dict(payload)
        payload["instanceId"] = key
        payload["status"] = "online"
        payload["receivedAt"] = _AUTH.utc_iso()
        if alias:
            payload.setdefault("alias", alias)
        with self._lock:
            placeholder = None
            if instance_id and key_candidate and key_candidate != key and key_candidate in self._items:
                placeholder = self._items.pop(key_candidate, None)
            if placeholder and "alias" in placeholder:
                payload.setdefault("alias", placeholder.get("alias"))
            payload.setdefault("alias", key)
            self._items[key] = payload
            self._persist()

    def mark_offline(self, instance_id: str, alias: Optional[str] = None) -> None:
        iid = str(instance_id or "").strip()
        if not iid:
            return
        with self._lock:
            entry = self._items.get(iid, {"instanceId": iid})
            entry["status"] = "offline"
            entry["receivedAt"] = _AUTH.utc_iso()
            entry.setdefault("samples", 0)
            entry.setdefault("windowSeconds", 0)
            entry.setdefault("timeoutSeconds", 0)
            entry.setdefault("latestSample", {})
            if alias:
                entry.setdefault("alias", alias)
            else:
                entry.setdefault("alias", iid)
            self._items[iid] = entry
            self._persist()

    def ensure_placeholder(self, instance_id: str, alias: str) -> None:
        key = str(instance_id or "").strip()
        if not key:
            return
        with self._lock:
            entry = self._items.get(key)
            if not entry:
                entry = {
                    "instanceId": key,
                    "status": "offline",
                    "samples": 0,
                    "windowSeconds": 0,
                    "timeoutSeconds": 0,
                    "latestSample": {},
                }
            entry.setdefault("alias", alias or key)
            entry.setdefault("receivedAt", _AUTH.utc_iso())
            self._items[key] = entry
            self._persist()

    def build_state(self, requested_ids: Optional[Iterable[str]] = None) -> Dict:
        with self._lock:
            if requested_ids:
                wanted = {str(x) for x in requested_ids}
                items = [self._items[iid] for iid in sorted(self._items) if iid in wanted]
            else:
                items = [self._items[iid] for iid in sorted(self._items)]
        return {"ts": _AUTH.utc_iso(), "items": items}

    def build_index(self, since_iso: Optional[str]) -> Dict:
        since_dt = None
        if since_iso:
            try:
                since_dt = _AUTH.parse(since_iso)
            except Exception:
                since_dt = None

        selected: List[Dict] = []
        with self._lock:
            for entry in self._items.values():
                received = entry.get("receivedAt")
                include = True
                if since_dt and isinstance(received, str) and received:
                    try:
                        dt = _AUTH.parse(received)
                        include = dt > since_dt
                    except Exception:
                        include = True
                if include:
                    selected.append(
                            {
                                "instanceId": entry.get("instanceId"),
                                "status": entry.get("status", "unknown"),
                                "receivedAt": entry.get("receivedAt"),
                            }
                    )
        return {"ts": _AUTH.utc_iso(), "items": selected}

    def prune(self, allowed_ids: Iterable[str]) -> None:
        allowed = {str(iid).strip() for iid in allowed_ids if str(iid).strip()}
        if not allowed:
            return
        with self._lock:
            removed = False
            for iid in list(self._items.keys()):
                if iid not in allowed:
                    removed = True
                    self._items.pop(iid, None)
            if removed:
                self._persist()

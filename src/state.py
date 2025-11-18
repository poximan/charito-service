import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


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
        snapshot = {"ts": _utc_now_iso(), "items": list(self._items.values())}
        tmp = self._data_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._data_path)

    def upsert_online(self, payload: Dict) -> None:
        instance_id = str(payload.get("instanceId") or "").strip()
        if not instance_id:
            return
        payload["status"] = "online"
        payload["receivedAt"] = _utc_now_iso()
        with self._lock:
            self._items[instance_id] = payload
            self._persist()

    def mark_offline(self, instance_id: str) -> None:
        iid = str(instance_id).strip()
        if not iid:
            return
        with self._lock:
            entry = self._items.get(iid, {"instanceId": iid})
            entry["status"] = "offline"
            entry["receivedAt"] = _utc_now_iso()
            entry.setdefault("samples", 0)
            entry.setdefault("windowSeconds", 0)
            entry.setdefault("latestSample", {})
            self._items[iid] = entry
            self._persist()

    def build_state(self, requested_ids: Optional[Iterable[str]] = None) -> Dict:
        with self._lock:
            if requested_ids:
                wanted = {str(x) for x in requested_ids}
                items = [self._items[iid] for iid in sorted(self._items) if iid in wanted]
            else:
                items = [self._items[iid] for iid in sorted(self._items)]
        return {"ts": _utc_now_iso(), "items": items}

    def build_index(self, since_iso: Optional[str]) -> Dict:
        since_dt = None
        if since_iso:
            try:
                value = since_iso
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"
                since_dt = datetime.fromisoformat(value)
            except Exception:
                since_dt = None

        selected: List[Dict] = []
        with self._lock:
            for entry in self._items.values():
                received = entry.get("receivedAt")
                include = True
                if since_dt and isinstance(received, str) and received:
                    try:
                        value = received
                        if value.endswith("Z"):
                            value = value[:-1] + "+00:00"
                        dt = datetime.fromisoformat(value)
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
        return {"ts": _utc_now_iso(), "items": selected}

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

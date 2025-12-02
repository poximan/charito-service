import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class Target:
    alias: str
    instance_id: Optional[str]
    metrics_url: str
    identity_url: str

    @property
    def tracking_key(self) -> str:
        return self.instance_id or self.alias

    @staticmethod
    def from_dict(entry: dict) -> "Target":
        alias_raw = entry.get("alias")
        if not alias_raw or not str(alias_raw).strip():
            raise ValueError("Cada instancia debe definir 'alias'")
        alias = str(alias_raw).strip()
        instance_id_raw = entry.get("id")
        instance_id = str(instance_id_raw).strip() if instance_id_raw else None
        base_url = str(entry.get("baseUrl") or entry.get("url") or "").strip()
        if not base_url:
            raise ValueError("Cada instancia debe definir 'baseUrl'")
        metrics_path = str(entry.get("metricsPath") or "/metrics").strip()
        if not metrics_path.startswith("/"):
            metrics_path = f"/{metrics_path}"
        identity_path = str(entry.get("identityPath") or "/identity").strip()
        if not identity_path.startswith("/"):
            identity_path = f"/{identity_path}"
        base_url = base_url.rstrip("/")
        metrics_url = f"{base_url}{metrics_path}"
        identity_url = f"{base_url}{identity_path}"
        return Target(alias=alias, instance_id=instance_id, metrics_url=metrics_url, identity_url=identity_url)


@dataclass(frozen=True)
class ServiceConfig:
    poll_interval_seconds: int
    http_timeout_seconds: float
    instances: List[Target]


def load_service_config(path: str) -> ServiceConfig:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"No se encuentra el archivo de targets: {file_path}")
    data = json.loads(file_path.read_text(encoding="utf-8"))
    poll_interval = int(data.get("pollIntervalSeconds") or 20)
    http_timeout = float(data.get("httpTimeoutSeconds") or 4.0)
    items = data.get("instances")
    if not isinstance(items, list) or not items:
        raise ValueError("El archivo de targets debe contener una lista 'instances'")
    targets = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        targets.append(Target.from_dict(entry))
    if not targets:
        raise ValueError("La lista de instancias esta vacia")
    return ServiceConfig(
        poll_interval_seconds=poll_interval,
        http_timeout_seconds=http_timeout,
        instances=targets,
    )


def load_targets(path: str) -> List[Target]:
    return load_service_config(path).instances

import requests

from config import Target


def fetch_instance_id(target: Target, timeout: float) -> str:
    response = requests.get(target.identity_url, timeout=timeout)
    response.raise_for_status()
    data = response.json() or {}
    instance_id = str(data.get("instanceId") or data.get("id") or "").strip()
    if not instance_id:
        raise ValueError(f"Endpoint de identidad sin 'instanceId': {target.identity_url}")
    return instance_id

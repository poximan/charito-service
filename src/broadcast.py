import json
import os
import threading
from typing import Iterable

import paho.mqtt.client as mqtt

from config import Target
from identity import fetch_instance_id
from state import _utc_now_iso


MQTT_TOPIC = "charito/whitelist/instances"

_lock = threading.RLock()
_client: mqtt.Client | None = None
_published = False


def broadcast_whitelist(targets: Iterable[Target]) -> None:
    global _published
    if _published:
        return

    items = []
    for target in targets:
        instance_id = target.instance_id
        if not instance_id:
            try:
                instance_id = fetch_instance_id(target, _http_timeout())
            except Exception:
                continue
        instance_id = instance_id.strip()
        if instance_id:
            items.append({"instanceId": instance_id})

    if not items:
        return

    payload = {"ts": _utc_now_iso(), "items": items}
    _publish_once(json.dumps(payload, ensure_ascii=False))
    _published = True


def _publish_once(body: str) -> None:
    client = _get_client()
    info = client.publish(MQTT_TOPIC, payload=body, qos=1, retain=True)
    info.wait_for_publish()
    client.loop_stop()
    try:
        client.disconnect()
    finally:
        global _client
        with _lock:
            _client = None


def _get_client() -> mqtt.Client:
    global _client
    with _lock:
        if _client is not None:
            return _client
        client = mqtt.Client(clean_session=True)
        host = _require("MQTT_BROKER_HOST")
        port = int(_require("MQTT_BROKER_PORT"))
        username = _require("MQTT_BROKER_USERNAME")
        password = _require("MQTT_BROKER_PASSWORD")
        use_tls = _truthy(_require("MQTT_BROKER_USE_TLS", "true"))
        insecure = _truthy(_require("MQTT_TLS_INSECURE", "false"))
        keepalive = int(_require("MQTT_BROKER_KEEPALIVE", "60"))
        client.username_pw_set(username, password)
        if use_tls:
            client.tls_set()
            client.tls_insecure_set(insecure)
        client.connect(host, port, keepalive=keepalive)
        client.loop_start()
        _client = client
        return client


def _http_timeout() -> float:
    try:
        return float(os.getenv("CHARITO_HTTP_TIMEOUT_SECONDS", "4"))
    except ValueError:
        return 4.0


def _require(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or not value.strip():
        raise EnvironmentError(f"Falta variable obligatoria: {name}")
    return value.strip()


def _truthy(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}

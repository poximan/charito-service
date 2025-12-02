import json
import os
import threading
from typing import Iterable

import paho.mqtt.client as mqtt

from config import Target
from identity import fetch_instance_id
from timeauthority import get_time_authority

_AUTH = get_time_authority()


MQTT_TOPIC = "charito/whitelist/instances"

_lock = threading.RLock()
_client: mqtt.Client | None = None
_last_payload: str | None = None


def broadcast_whitelist(targets: Iterable[Target], overrides: dict[str, str] | None = None) -> None:
    items = []
    for target in targets:
        alias = target.alias
        resolved = overrides.get(target.identity_url) if overrides else None
        instance_id = resolved or target.instance_id
        provisional = False
        if not instance_id:
            try:
                instance_id = fetch_instance_id(target, _http_timeout())
            except Exception:
                instance_id = None
        if not instance_id:
            instance_id = alias
            provisional = True
        entry = {
            "instanceId": instance_id.strip(),
            "alias": alias,
            "provisional": provisional,
        }
        items.append(entry)

    if not items:
        return

    payload = {"ts": _AUTH.utc_iso(), "items": items}
    body = json.dumps(payload, ensure_ascii=False)
    global _last_payload
    if _last_payload == body:
        return
    _publish_once(body)
    _last_payload = body


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

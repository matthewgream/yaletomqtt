#!/usr/bin/env python3

# -----------------------------------------------------------------------------------------------------------------------------------------
# yaletomqtt: Yale Home locks (e.g. a Yale Doorman L3 with its Connect Wi-Fi bridge) to MQTT and back, through Yale's cloud.
#
# The locks' state and activity come from Yale Home's API (api.aaecosystem.com) and its websocket push, through yalexs - the library
# Home Assistant's Yale integration is built on - used the way that integration uses it. Yale Home only accepts OAuth, for Home
# Assistant's registered app, through Nabu Casa's account-link service: this does the same, without Home Assistant or a Nabu Casa
# account. Once: --authorize, and sign in with the Yale Home account at the link it prints; the tokens are kept, and refreshed, in
# the token file.
#
#   <topic>/locks/<id>   retained, each lock: {"id","name","house_id","model","state":"locked|unlocked|locking|unlocking|jammed|unknown",
#                        "door":"open|closed|unknown|disabled","battery":0-100,"bridge":"online|offline","ts"}
#                        - every lock at start: a subscription gets them all, then their changes
#   <topic>/events       each operation: {"type":"lock","lock","id","action","state","by","method","ts"}, from Yale's log (who, how);
#                        each door change: {"type":"door","lock","id","door","action","ts"};
#                        a state the lock reported with no operation behind it in Yale's log (its bridge back, a turn while it
#                        was away): {"type":"state","lock","id","state","was","ts"}; the bridge: {"type":"bridge","lock","id","bridge","ts"};
#                        and {"type":"request","ok":false,"error"} for a refused request
#   <topic>/status       retained: online (connected to Yale, push running) / offline
#   <topic>/request      {"lock":<id or name>,"action":"lock"|"unlock"} ("lock" may be left out when there's only one)
#
# Settings: a key=value config file (--config <file>, default yaletomqtt.cfg), each also a --key value flag:
#   mqtt-server=mqtt://localhost mqtt-client=yaletomqtt mqtt-topic=yale token-file=/var/lib/yaletomqtt/tokens.json debug=false
# -----------------------------------------------------------------------------------------------------------------------------------------

import asyncio
import gc
import json
import logging
import os
import secrets
import signal
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiomqtt
from yalexs.activity import ActivityType, LockOperationActivity
from yalexs.authenticator_common import Authentication, AuthenticationState
from yalexs.const import Brand
from yalexs.lock import LockDoorStatus, LockOperation, LockStatus
from yalexs.manager.const import CONF_BRAND
from yalexs.manager.data import YaleXSData
from yalexs.manager.gateway import Gateway
from yalexs.util import get_latest_activity, update_lock_detail_from_activity

CONFIG_FILE_DEFAULT = "yaletomqtt.cfg"
ACCOUNT_LINK = "account-link.nabucasa.com"  # Nabu Casa's OAuth account linking, as Home Assistant's cloud uses it
SERVICE = "yale"
TOKEN_MARGIN = 120  # refresh this many seconds before the access token expires
CHECK_INTERVAL = 60  # look for a change nothing signalled
DETAIL_INTERVAL = 900  # re-read each lock's detail (its bridge, battery) this often - yalexs on its own does it once a day
LOOKUP_DELAY = 6  # a push that says only what state a lock is in: by then Yale's log has the operation behind it, if there was one
LOOKUP_WINDOW = 60  # an operation in the log this close to the push is the one behind it
RECONNECT_MIN, RECONNECT_MAX = 10, 600
EVENTS_QUEUED = 1000  # events kept while the broker is away
REQUEST_TIMEOUT = 60  # a lock operation with no answer by then is reported as failed

# -----------------------------------------------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------


def config_load(argv):
    flags, i = {}, 1
    while i < len(argv):
        if argv[i].startswith("--"):
            key = argv[i][2:]
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                flags[key] = argv[i + 1]
                i += 1
            else:
                flags[key] = "true"
        i += 1
    file = flags.pop("config", CONFIG_FILE_DEFAULT)
    config = {}
    try:
        for line in Path(file).read_text().splitlines():
            text = line.strip()
            if text and not text.startswith("#") and "=" in text:
                key, value = text.split("=", 1)
                config[key.strip()] = value.strip()
    except OSError as e:
        print(f"config: could not load '{file}': {e}", file=sys.stderr)
    config.update(flags)
    print(f"config: file='{file}', " + ", ".join(f"{k}='{v}'" for k, v in config.items()))
    return config


config = config_load(sys.argv)
MQTT_SERVER = config.get("mqtt-server", "mqtt://localhost")
MQTT_CLIENT = config.get("mqtt-client", "yaletomqtt")
TOPIC = config.get("mqtt-topic", "yale")
TOKEN_FILE = Path(config.get("token-file", "/var/lib/yaletomqtt/tokens.json"))
DEBUG = config.get("debug", "false") in ("true", "1")

logging.basicConfig(level=logging.DEBUG if DEBUG else logging.WARNING, format="%(name)s: %(message)s")
log = print


def now():
    return int(time.time())


# -----------------------------------------------------------------------------------------------------------------------------------------
# the tokens: from Nabu Casa's account linking, once (--authorize); then refreshed through it, and kept in the token file (root only)
# -----------------------------------------------------------------------------------------------------------------------------------------


class TokenError(Exception):
    pass


class Tokens:
    def __init__(self, path):
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else None
        self._lock = asyncio.Lock()

    @property
    def access_token(self):
        return self.data["access_token"]

    def set(self, tokens):
        data = {**(self.data or {}), **tokens}
        if "expires_in" in tokens:
            data["expires_at"] = time.time() + float(tokens["expires_in"])
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".new")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(temporary, self.path)
        self.data = data

    async def ensure_valid(self, session):
        async with self._lock:
            if self.data is None:
                raise TokenError(f"no tokens in {self.path}: run with --authorize first")
            if self.data.get("expires_at", 0) - TOKEN_MARGIN > time.time():
                return
            if not self.data.get("refresh_token"):
                raise TokenError("the access token has expired and there's no refresh token: run with --authorize again")
            async with session.post(
                f"https://{ACCOUNT_LINK}/refresh_token/{SERVICE}",
                json={"refresh_token": self.data["refresh_token"]},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status >= 400:
                    raise TokenError(f"token refresh: HTTP {response.status}: {(await response.text())[:200]}")
                tokens = await response.json()
            self.set(tokens)
            log(f"yale: access token refreshed, valid for {int(self.data['expires_at'] - time.time())}s")


async def authorize():
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"https://{ACCOUNT_LINK}/v1") as ws:
            await ws.send_json({"service": SERVICE})
            response = await ws.receive_json(timeout=30)
            if "error" in response:
                raise SystemExit(f"authorize: {response['error']}")
            log("authorize: open this link and sign in with the Yale Home account (waiting up to 15 minutes):")
            log(f"\n    {response['authorize_url']}\n")
            response = await ws.receive_json(timeout=900)
            if "error" in response:
                raise SystemExit(f"authorize: {response['error']}")
    Tokens(TOKEN_FILE).set(response["tokens"])
    log(f"authorize: done - the tokens are in {TOKEN_FILE}")


# Home Assistant's YaleGateway, with these tokens in place of its OAuth session
class TokenGateway(Gateway):
    def __init__(self, config_path, session, tokens):
        super().__init__(config_path, session)
        self._tokens = tokens

    async def async_get_access_token(self):
        await self._tokens.ensure_valid(self._aiohttp_session)
        return self._tokens.access_token

    async def async_refresh_access_token_if_needed(self):
        await self._tokens.ensure_valid(self._aiohttp_session)

    async def async_authenticate(self):
        await self._tokens.ensure_valid(self._aiohttp_session)
        self.authentication = Authentication(AuthenticationState.AUTHENTICATED, None, None, None)
        return self.authentication


# Home Assistant's YaleData: the offline (Bluetooth) key is only noted - it's what a local route to the lock would need, never logged
class YaleData(YaleXSData):
    def async_offline_key_discovered(self, detail):
        log(f"yale: {detail.device_name}: an offline key is available (slot {detail.offline_slot}) - for a local, Bluetooth route")

    async def refresh_detail(self, lock_id):  # yalexs's own refresh, which keeps the pushed lock and door state; it runs it only daily
        await self._async_refresh_device_detail_by_id(lock_id)


# -----------------------------------------------------------------------------------------------------------------------------------------
# the locks: state as Home Assistant's lock and door sensor make it, from each lock's detail and its latest activities
# -----------------------------------------------------------------------------------------------------------------------------------------


def method_of(activity):
    if not isinstance(activity, LockOperationActivity):
        return None
    for method, flag in (
        ("autolock", activity.operated_autorelock),
        ("keypad", activity.operated_keypad),
        ("tag", activity.operated_tag),
        ("remote", activity.operated_remote),
        ("manual", activity.operated_manual),
    ):
        if flag:
            return method
    return "mobile" if activity.operated_by else None  # neither who nor how: a push of the lock's state, not an operation


class Bridge:
    def __init__(self):
        self.data = None
        self.states = {}  # lock id -> the last published state
        self.seen = {}  # (lock id, kind) -> the latest activity's start time
        self.online = False
        self.events = asyncio.Queue(maxsize=EVENTS_QUEUED)
        self.mqtt = None  # the connected client, if any
        self.pending = set()  # lock operations under way
        self.gateway = None
        self.detail_at = {}  # lock id -> when its detail was last read
        self.emitted = set()  # (lock id, start time) of the operations published

    # ---- publishing

    async def publish(self, subtopic, payload, retain=False):
        if self.mqtt is not None:
            try:
                await self.mqtt.publish(f"{TOPIC}/{subtopic}", payload if isinstance(payload, str) else json.dumps(payload), retain=retain)
            except aiomqtt.MqttError as e:
                log(f"mqtt: publish {subtopic}: {e}")

    def event(self, payload):
        payload = {**payload, "ts": payload.get("ts", now())}
        if DEBUG:
            log(f"event: {json.dumps(payload)}")
        try:
            self.events.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    async def status(self, online):
        self.online = online
        await self.publish("status", "online" if online else "offline", retain=True)

    # ---- state

    def latest(self, lock_id, types):
        return self.data.activity_stream.get_latest_device_activity(lock_id, types)

    def lock_state(self, lock_id):
        detail = self.data.get_device_detail(lock_id)
        lock_activity = self.latest(lock_id, {ActivityType.LOCK_OPERATION})
        without_operator = self.latest(lock_id, {ActivityType.LOCK_OPERATION_WITHOUT_OPERATOR})
        if latest := get_latest_activity(without_operator, lock_activity):
            update_lock_detail_from_activity(detail, latest)
        # a bridge activity has no time check of its own in yalexs: an old one would undo a fresh read of the detail
        if (bridge := self.latest(lock_id, {ActivityType.BRIDGE_OPERATION})) and bridge.activity_start_time.timestamp() > self.detail_at.get(lock_id, 0):
            update_lock_detail_from_activity(detail, bridge)
        if door := self.latest(lock_id, {ActivityType.DOOR_OPERATION}):
            update_lock_detail_from_activity(detail, door)
        status = detail.lock_status
        state = {
            LockStatus.LOCKED: "locked",
            LockStatus.UNLOCKED: "unlocked",
            LockStatus.UNLATCHED: "unlocked",
            LockStatus.LOCKING: "locking",
            LockStatus.UNLOCKING: "unlocking",
            LockStatus.UNLATCHING: "unlocking",
            LockStatus.JAMMED: "jammed",
        }.get(status, "unknown")
        door = detail.door_state.value if detail.doorsense and isinstance(detail.door_state, LockDoorStatus) else "disabled"
        return {
            "id": lock_id,
            "name": detail.device_name,
            "house_id": detail.house_id,
            "model": detail.model,
            "state": state,
            "door": door,
            "battery": round(detail.battery_level) if detail.battery_level is not None else None,
            "bridge": None if detail.bridge is None else ("online" if detail.bridge_is_online else "offline"),
        }

    async def update(self, lock_id, force=False):
        state = self.lock_state(lock_id)
        before = self.states.get(lock_id)
        if force or before != state:
            self.states[lock_id] = state
            await self.publish(f"locks/{lock_id}", {**state, "ts": now()}, retain=True)
            if before and DEBUG:
                log(f"state: {state['name']}: {state['state']}, door {state['door']}")
        if before and state["bridge"] is not None and before.get("bridge") != state["bridge"]:
            log(f"yale: {state['name']}: its bridge is {state['bridge']}")
            self.event({"type": "bridge", "lock": state["name"], "id": lock_id, "bridge": state["bridge"]})
        self.activities(lock_id, state, before)

    async def refresh(self, lock_id):
        self.detail_at[lock_id] = time.time()
        try:
            await self.data.refresh_detail(lock_id)
        except Exception as e:  # noqa: BLE001 - the next one may do
            log(f"yale: reading {lock_id}: {type(e).__name__}: {e}")

    # new activities since the last look: events (who, how) - at start just remembered, not replayed
    def activities(self, lock_id, state, before=None, replay=True):
        for kind, types in (
            ("lock", {ActivityType.LOCK_OPERATION, ActivityType.LOCK_OPERATION_WITHOUT_OPERATOR}),
            ("door", {ActivityType.DOOR_OPERATION}),
        ):
            activity = self.latest(lock_id, types)
            if activity is None:
                continue
            started = activity.activity_start_time.timestamp()
            if self.seen.get((lock_id, kind), 0) >= started:
                continue
            self.seen[(lock_id, kind)] = started
            if not replay:
                continue
            if kind == "lock":
                if activity.was_pushed and method_of(activity) is None:  # the lock's state, pushed: who and how are in Yale's log
                    asyncio.get_running_loop().create_task(self.lookup(lock_id, started, before["state"] if before else None))
                elif (lock_id, round(started, 3)) not in self.emitted:
                    self.operation(lock_id, state, activity)
            elif not (activity.was_pushed and before and before["door"] == state["door"]):  # not the same door state again
                self.event({"type": "door", "lock": state["name"], "id": lock_id, "door": state["door"], "action": activity.action, "ts": int(started)})

    def operation(self, lock_id, state, activity):
        started = activity.activity_start_time.timestamp()
        self.emitted.add((lock_id, round(started, 3)))
        by = activity.operated_by if isinstance(activity, LockOperationActivity) else None
        self.event({"type": "lock", "lock": state["name"], "id": lock_id, "action": activity.action, "state": state["state"], "by": by, "method": method_of(activity), "ts": int(started)})

    # a pushed state, without who or how: the operation behind it from Yale's log (yalexs keeps that only if it's timed after the
    # push), or - none there - the lock reporting its state (its bridge back, say), published only if it isn't the one known
    async def lookup(self, lock_id, pushed_at, was):
        await asyncio.sleep(LOOKUP_DELAY)
        if self.data is None:
            return
        try:
            token = await self.gateway.async_get_access_token()
            found = await self.gateway.api.async_get_house_activities(token, self.data.get_device_detail(lock_id).house_id, limit=10)
        except Exception as e:  # noqa: BLE001 - then it's reported as a state
            log(f"yale: looking up the operation behind a pushed state: {type(e).__name__}: {e}")
            found = []
        operations = sorted(
            (a for a in found if isinstance(a, LockOperationActivity) and a.device_id == lock_id and abs(a.activity_start_time.timestamp() - pushed_at) <= LOOKUP_WINDOW),
            key=lambda a: a.activity_start_time,
        )
        state = self.states.get(lock_id) or self.lock_state(lock_id)
        for activity in operations:
            if (lock_id, round(activity.activity_start_time.timestamp(), 3)) not in self.emitted:
                self.operation(lock_id, state, activity)
        if not operations and was is not None and was != state["state"]:
            self.event({"type": "state", "lock": state["name"], "id": lock_id, "state": state["state"], "was": was, "ts": int(pushed_at)})

    def changed(self, lock_id):
        loop = asyncio.get_running_loop()
        state = self.states.get(lock_id)
        if state and state["bridge"] == "offline" and time.time() - self.detail_at.get(lock_id, 0) > CHECK_INTERVAL:
            loop.create_task(self.refresh(lock_id))  # word from a lock whose bridge reads offline: it may be back
        loop.create_task(self.update(lock_id))

    # ---- requests: {"lock": id or name, "action": "lock"|"unlock"}

    async def request(self, text):
        def refuse(error):
            log(f"request: {error}: {text[:200]}")
            self.event({"type": "request", "ok": False, "error": error, "request": text[:200]})

        if self.data is None:
            return refuse("not connected to Yale yet")
        try:
            r = json.loads(text)
        except ValueError:
            return refuse("not JSON")
        if not isinstance(r, dict):
            return refuse("not a JSON object")
        action = str(r.get("action", "")).lower()
        if action not in ("lock", "unlock"):
            return refuse('"action" is "lock" or "unlock"')
        locks = list(self.data.locks)
        if "lock" in r:
            want = str(r["lock"]).lower()
            matches = [x for x in locks if x.device_id.lower() == want or (x.device_name or "").lower() == want]
        else:
            matches = locks if len(locks) == 1 else []
        if len(matches) != 1:
            return refuse(f'no single lock {r.get("lock", "")!r} (there\'s: {", ".join(f"{x.device_name} [{x.device_id}]" for x in locks)})')
        lock = matches[0]
        detail = self.data.get_device_detail(lock.device_id)
        hyper_bridge = bool(detail.bridge and detail.bridge.hyper_bridge)
        log(f"request: {action} {lock.device_name} [{lock.device_id}]{' (its bridge is offline)' if detail.bridge and not detail.bridge_is_online else ''}")
        operation = LockOperation.LOCK if action == "lock" else LockOperation.UNLOCK
        data = self.data

        def result(task):  # the operation's outcome, whenever it comes: on time, or after it was reported as unanswered
            self.pending.discard(task)
            if task.cancelled():
                return
            if (e := task.exception()) is not None:
                return refuse(f"{action} failed: {type(e).__name__}: {e}")
            for activity in task.result() or []:
                update_lock_detail_from_activity(detail, activity)
            data.async_signal_device_id_update(lock.device_id)
            log(f"request: {action} {lock.device_name}: done")

        # a task, waited for with asyncio.wait: yalexs can hold out against the cancelling that asyncio.wait_for would wait on
        task = asyncio.create_task(data.async_operate_lock(lock.device_id, operation, data.push_updates_connected, hyper_bridge))
        self.pending.add(task)
        task.add_done_callback(result)
        done, _ = await asyncio.wait({task}, timeout=REQUEST_TIMEOUT)
        if not done:
            refuse(f"{action} has had no answer in {REQUEST_TIMEOUT}s (the lock or its bridge offline?) - still waiting for one")

    # ---- Yale

    async def run_yale(self, session):
        tokens = Tokens(TOKEN_FILE)
        state_dir = TOKEN_FILE.parent
        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        gateway = TokenGateway(state_dir, session, tokens)
        await gateway.async_setup({CONF_BRAND: Brand.YALE_GLOBAL})
        await gateway.async_authenticate()
        self.gateway = gateway
        self.data = YaleData(gateway)
        await self.data.async_setup()
        locks = list(self.data.locks)
        for lock in locks:
            self.detail_at[lock.device_id] = time.time()  # the setup has just read them
        log(f"yale: connected, {len(locks)} lock(s): {', '.join(f'{x.device_name} [{x.device_id}]' for x in locks)}")
        for lock in locks:
            self.data.async_subscribe_device_id(lock.device_id, lambda lock_id=lock.device_id: self.changed(lock_id))
            self.data.activity_stream.async_subscribe_device_id(lock.device_id, lambda lock_id=lock.device_id: self.changed(lock_id))
            state = self.lock_state(lock.device_id)
            self.activities(lock.device_id, state, replay=False)
            self.states[lock.device_id] = state
            await self.publish(f"locks/{lock.device_id}", {**state, "ts": now()}, retain=True)
            log(f"yale: {state['name']}: {state['state']}, door {state['door']}, battery {state['battery']}%")
        await self.status(True)
        push_was = None
        while True:  # changes are signalled; this catches anything that isn't, and notices the push going
            await asyncio.sleep(CHECK_INTERVAL)
            await gateway.async_refresh_access_token_if_needed()
            if self.data.push_updates_connected != push_was:
                push_was = self.data.push_updates_connected
                log(f"yale: push updates {'connected' if push_was else 'not connected (polling)'}")
            for lock in list(self.data.locks):
                if time.time() - self.detail_at.get(lock.device_id, 0) >= DETAIL_INTERVAL:
                    await self.refresh(lock.device_id)
                await self.update(lock.device_id)

    async def yale_forever(self):
        delay = RECONNECT_MIN
        while True:
            async with aiohttp.ClientSession() as session:
                try:
                    await self.run_yale(session)
                except asyncio.CancelledError:
                    raise
                except TokenError as e:
                    log(f"yale: {e}")
                    delay = RECONNECT_MAX
                except Exception as e:  # noqa: BLE001 - log it, and start again
                    log(f"yale: {type(e).__name__}: {e}")
                finally:
                    if self.data is not None:
                        try:
                            await self.data.async_stop()
                        except Exception:  # noqa: BLE001
                            pass
                        self.data = None
                    if self.online:
                        await self.status(False)
            log(f"yale: reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)

    # ---- MQTT

    async def mqtt_forever(self):
        url = urlparse(MQTT_SERVER if "://" in MQTT_SERVER else f"mqtt://{MQTT_SERVER}")
        will = aiomqtt.Will(f"{TOPIC}/status", "offline", retain=True)
        while True:
            try:
                async with aiomqtt.Client(url.hostname or "localhost", port=url.port or 1883, identifier=f"{MQTT_CLIENT}-{secrets.token_hex(3)}", will=will) as client:
                    self.mqtt = client
                    log(f"mqtt: connected to {MQTT_SERVER}, publishing {TOPIC}/#, requests on {TOPIC}/request")
                    await client.subscribe(f"{TOPIC}/request")
                    await self.status(self.online)
                    for lock_id, state in self.states.items():
                        await self.publish(f"locks/{lock_id}", {**state, "ts": now()}, retain=True)
                    sender = asyncio.create_task(self.send_events())
                    try:
                        async for message in client.messages:
                            if message.topic.matches(f"{TOPIC}/request"):
                                asyncio.create_task(self.request(message.payload.decode(errors="replace")))
                    finally:
                        sender.cancel()
            except aiomqtt.MqttError as e:
                log(f"mqtt: {e}, reconnecting in 5s")
            self.mqtt = None
            await asyncio.sleep(5)

    async def send_events(self):
        while True:
            payload = await self.events.get()
            await self.publish("events", payload)


async def main():
    log(f"yaletomqtt: Yale Home, to {MQTT_SERVER} {TOPIC}/#")
    bridge = Bridge()
    tasks = [asyncio.create_task(bridge.mqtt_forever()), asyncio.create_task(bridge.yale_forever())]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    log("stopping")
    await bridge.status(False)
    for task in list(bridge.pending):  # operations still waiting for Yale: ended first, while their connections can still close
        task.cancel()
    if bridge.pending:
        await asyncio.wait(bridge.pending, timeout=5)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    # yalexs's websocket client (python-socketio, to websocket.aaecosystem.com) opens an HTTP session of its own and doesn't close it
    # when stopped: at exit, it's collected after the loop has gone, noisily - and harmlessly. Quiet just that, just now.
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)
    sys.unraisablehook = lambda u: (
        None if isinstance(u.exc_value, RuntimeError) and "Event loop is closed" in str(u.exc_value) else sys.__unraisablehook__(u)
    )
    gc.collect()
    await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(authorize() if config.get("authorize") == "true" else main())

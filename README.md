# yaletomqtt

Yale Home locks to MQTT and back, through Yale's cloud: each lock's state (locked, door open or closed, battery), retained, so a
subscription gets them all at once and then their changes; every operation and door change, with who and how; and requests to lock
and unlock. Written for a Yale Doorman L3 with its Yale Connect Wi-Fi bridge.

One of a small family of single-purpose *tomqtt bridges (plejdtomqtt, p1metertomqtt, evoaira100sg3tomqtt, ...) that share a
config convention.

## How it works

The locks come from Yale Home's API (`api.aaecosystem.com`) and its websocket push, through
[yalexs](https://github.com/bdraco/yalexs) — the library [Home Assistant's Yale integration](https://www.home-assistant.io/integrations/yale)
is built on — used the way that integration uses it: its state for a lock and its door sensor, from the lock's detail and its
latest activities.

Yale Home only accepts OAuth, for Home Assistant's registered app, through Nabu Casa's account-link service
(`account-link.nabucasa.com`). This does the same, without Home Assistant or a Nabu Casa account: `--authorize` once, and sign in
with the Yale Home account at the link it prints; the tokens come back to it, go in the token file (root only), and it refreshes
them, through the same service, as they expire. It borrows Home Assistant's app: should Yale or Nabu Casa change that, this (and
Home Assistant's integration) breaks.

The lock's offline (Bluetooth) key comes with it, and is noted in the log — never shown — for a local route to the lock later.

Not affiliated with Yale.

## MQTT

| topic | retained | |
|---|---|---|
| `yale/locks/<id>` | yes | `{"id","name","house_id","model","state":"locked"\|"unlocked"\|"locking"\|"unlocking"\|"jammed"\|"unknown","door":"open"\|"closed"\|"unknown"\|"disabled","battery":70,"bridge":"online"\|"offline","ts"}` |
| `yale/events` | no | each operation and door change — see below |
| `yale/status` | yes | `online` (connected to Yale) or `offline` |
| `yale/request` | — | `{"lock":"Front door","action":"lock"}`, `{"action":"unlock"}` |

`state` is `unknown` while the lock can't be reached (its bridge offline); `door` is `disabled` without its door sensor.

Events:

```
{"type":"lock","lock":"FRONT DOOR","id":"…","action":"unlock","state":"unlocked","by":"…","method":"keypad","ts":…}
{"type":"door","lock":"FRONT DOOR","id":"…","door":"open","action":"dooropen","ts":…}
{"type":"request","ok":false,"error":"…","request":"…","ts":…}                         a request refused or failed
```

`method` is `keypad`, `tag`, `remote` (the app, or a request here), `manual`, `autolock` or `mobile`; `by` is who, when Yale knows.

Requests — the lock by id or name, or left out when there's only one:

```
mosquitto_pub -t yale/request -m '{"action":"lock"}'
mosquitto_pub -t yale/request -m '{"lock":"FRONT DOOR","action":"unlock"}'
```

The result shows on `yale/locks/…` and `yale/events` as the lock reports it.

## Build and install

Python 3.10 or newer, and python3-venv; the dependencies (`requirements.txt`) go in `.venv`.

    make                    # the venv
    make authorize          # once: sign in with the Yale Home account at the link it prints
    make test               # run against the local .cfg, with debug
    make lint

**Host-specific config convention:** the Makefile installs `yaletomqtt.<hostname>.cfg` if it exists, otherwise the generic
`yaletomqtt.cfg`. The config holds no secrets: the tokens are in the token file.

    make install            # config  -> /etc/default/yaletomqtt
                            # unit    -> yaletomqtt.service (enabled, restarted), running from this directory
    make restart

## Configuration

A `key=value` config file (`--config <file>`, default `yaletomqtt.cfg`); each key is also a `--key value` flag.

| key | default | |
|---|---|---|
| `mqtt-server` | `mqtt://localhost` | broker URL |
| `mqtt-client` | `yaletomqtt` | client id (prefix) |
| `mqtt-topic` | `yale` | the topics' prefix |
| `token-file` | `/var/lib/yaletomqtt/tokens.json` | the OAuth tokens (root only), written by `--authorize` |
| `debug` | `false` | log every event and change, and yalexs's own debug |

## License

CC BY-NC-SA 4.0 — see [LICENSE](LICENSE).

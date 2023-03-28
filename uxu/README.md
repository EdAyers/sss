# uxu

[![PyPI - Version](https://img.shields.io/pypi/v/uxu.svg)](https://pypi.org/project/uxu)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/uxu.svg)](https://pypi.org/project/uxu)

Make little user interfaces without fussing around with JavaScript.

This library is very WIP.

## Installation

```console
pip install uxu
```

## License

`uxu` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Development

To develop the javascript portion of the library, please do

```sh
cd web
# to build
node build.mjs

# to build and watch and live-reload
node server.mjs
```

## Todos

- server: only connect websocket if interactivity is needed
- server: have checksum on patches to make sure state doesn't get messed up.
- server: timeouts on websocket connections
- server: auth
- server: live reloading
- components: implement forms
- components: spinners while waiting for patcher roundtrips
# websocket_proxy

A minimal transparent WebSocket proxy built on WebSocket++.

It listens for downstream client connections and, for each one, opens a
connection to a single configured upstream WebSocket server. Once both legs are
established it relays application messages verbatim in both directions.

```
[ client ] --ws--> [ websocket_proxy ] --ws--> [ upstream server ]
```

## Subprotocol negotiation

Subprotocol negotiation is proxied end to end rather than terminated at the
proxy:

1. The subprotocol offers from the downstream client are forwarded to the
   upstream server as the upstream connection's offers.
2. The downstream handshake response is **deferred**
   (`connection::defer_http_response()`) until the upstream handshake completes,
   so the proxy does not have to guess which subprotocol will be chosen.
3. Whatever subprotocol the upstream server selects is echoed back to the
   downstream client in its (now un-deferred) 101 response.

Because `process_handshake()` builds the response during handshake validation,
the negotiated subprotocol is applied to the deferred response with both
`select_subprotocol()` (so `get_subprotocol()` reports it) and
`replace_header()` (so it is written on the wire). See `proxy.hpp` for details.

Both endpoints share a single `io_context`, so all handlers run on one thread
and no synchronization between the two legs is required.

## Running

```
websocket_proxy [listen_port] [upstream_uri]
```

Defaults: listen on `9006`, forward to `ws://localhost:9002`. For example, to
put a proxy in front of the `echo_server` example:

```
./echo_server                       # listens on 9002
./websocket_proxy 9006 ws://localhost:9002
./echo_client ws://localhost:9006   # talks to the echo server through the proxy
```

## Test

`proxy_test.cpp` is an end-to-end test (registered with ctest as
`websocket_proxy_test`). It starts a pingpong upstream server and the proxy,
then runs multiple concurrent clients through the proxy. For every client it
checks that:

* a `ping` is answered with `pong` (bidirectional relay works),
* the client's `get_subprotocol()` reports the subprotocol the *upstream* server
  selected (negotiation was proxied back to the client), and
* the upstream server's own view of the negotiated subprotocol matches (the
  offers were proxied upstream).

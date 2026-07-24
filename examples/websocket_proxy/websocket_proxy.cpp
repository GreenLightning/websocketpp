/*
 * Copyright (c) 2026, Peter Thorson. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the WebSocket++ Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL PETER THORSON BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "proxy.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

// A transparent WebSocket proxy. It listens on a local port and forwards every
// connection (and its subprotocol negotiation) to a single upstream WebSocket
// server.
//
//   Usage: websocket_proxy [listen_port] [upstream_uri]
//   Defaults: listen on 9006, forward to ws://localhost:9002
int main(int argc, char * argv[]) {
    uint16_t listen_port = 9006;
    std::string upstream_uri = "ws://localhost:9002";

    if (argc >= 2) {
        listen_port = static_cast<uint16_t>(std::atoi(argv[1]));
    }
    if (argc >= 3) {
        upstream_uri = argv[2];
    }

    try {
        websocketpp::lib::asio::io_context io;

        proxy_server proxy(io, upstream_uri);
        proxy.listen(listen_port);

        std::cout << "WebSocket proxy listening on port " << listen_port
                  << " -> " << upstream_uri << std::endl;

        io.run();
    } catch (websocketpp::exception const & e) {
        std::cout << "proxy error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

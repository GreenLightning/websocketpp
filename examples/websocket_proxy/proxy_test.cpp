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

// End-to-end test for the WebSocket proxy.
//
// Topology:
//
//   [ N test clients ] --ws--> [ proxy ] --ws--> [ pingpong upstream server ]
//
// Each client offers a set of subprotocols, exchanges a ping/pong and a
// "whoami" request, and then closes. The test verifies, for every client:
//
//   * the ping was answered with a pong (bidirectional relay works),
//   * the client's get_subprotocol() reports the subprotocol the *upstream*
//     server selected (negotiation was proxied back to the client), and
//   * the upstream server's own view of the negotiated subprotocol (reported
//     in the "whoami" reply) matches (the offers were proxied upstream).
//
// The process exits 0 if every check passes, 1 otherwise.

#include "proxy.hpp"

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/server.hpp>
#include <websocketpp/client.hpp>

#include <websocketpp/common/thread.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

static std::string to_str(std::size_t n) {
    std::ostringstream ss;
    ss << n;
    return ss.str();
}

typedef websocketpp::server<websocketpp::config::asio> pingpong_server;
typedef websocketpp::client<websocketpp::config::asio_client> test_client;

using websocketpp::connection_hdl;
using websocketpp::lib::bind;
using websocketpp::lib::placeholders::_1;
using websocketpp::lib::placeholders::_2;

// ---------------------------------------------------------------------------
// test bookkeeping
// ---------------------------------------------------------------------------

static int g_failures = 0;

static void check(bool cond, std::string const & what) {
    if (!cond) {
        ++g_failures;
        std::cout << "  FAIL: " << what << std::endl;
    }
}

// ---------------------------------------------------------------------------
// upstream pingpong server
// ---------------------------------------------------------------------------

// Subprotocols the upstream server understands.
static bool server_supports(std::string const & p) {
    return p == "chat.v1" || p == "echo";
}

// Select the first offered subprotocol that the server supports.
static bool pingpong_validate(pingpong_server * s, connection_hdl hdl) {
    pingpong_server::connection_ptr con = s->get_con_from_hdl(hdl);

    std::vector<std::string> const & offers = con->get_requested_subprotocols();
    for (std::size_t i = 0; i < offers.size(); ++i) {
        if (server_supports(offers[i])) {
            con->select_subprotocol(offers[i]);
            break;
        }
    }
    return true;
}

static void pingpong_on_message(pingpong_server * s, connection_hdl hdl,
    pingpong_server::message_ptr msg)
{
    pingpong_server::connection_ptr con = s->get_con_from_hdl(hdl);
    std::string const & in = msg->get_payload();

    std::string out;
    if (in == "ping") {
        out = "pong";
    } else if (in == "whoami") {
        // Report the subprotocol negotiated on the upstream leg so the client
        // can confirm the offers were proxied all the way through.
        out = "proto:" + con->get_subprotocol();
    } else {
        out = in;
    }

    websocketpp::lib::error_code ec;
    s->send(hdl, out, msg->get_opcode(), ec);
}

// ---------------------------------------------------------------------------
// test clients
// ---------------------------------------------------------------------------

struct client_case {
    // input
    std::vector<std::string> offer;
    std::string expected;        // subprotocol the upstream should settle on

    // observed
    int step;                    // 0: awaiting pong, 1: awaiting whoami reply
    bool got_pong;
    std::string client_reported; // downstream get_subprotocol()
    std::string server_reported; // subprotocol reported by the upstream server
    bool closed;
    bool failed;

    client_case() : step(0), got_pong(false), closed(false), failed(false) {}
};

static void client_on_open(test_client * c, client_case * cc, connection_hdl hdl) {
    test_client::connection_ptr con = c->get_con_from_hdl(hdl);
    cc->client_reported = con->get_subprotocol();

    websocketpp::lib::error_code ec;
    c->send(hdl, "ping", websocketpp::frame::opcode::text, ec);
}

static void client_on_message(test_client * c, client_case * cc,
    connection_hdl hdl, test_client::message_ptr msg)
{
    std::string const & in = msg->get_payload();
    websocketpp::lib::error_code ec;

    if (cc->step == 0) {
        cc->got_pong = (in == "pong");
        cc->step = 1;
        c->send(hdl, "whoami", websocketpp::frame::opcode::text, ec);
    } else {
        std::string const prefix = "proto:";
        if (in.size() >= prefix.size() &&
            in.compare(0, prefix.size(), prefix) == 0)
        {
            cc->server_reported = in.substr(prefix.size());
        }
        test_client::connection_ptr con = c->get_con_from_hdl(hdl);
        con->close(websocketpp::close::status::normal, "done", ec);
    }
}

static void client_on_close(client_case * cc, connection_hdl) {
    cc->closed = true;
}

static void client_on_fail(test_client * c, client_case * cc, connection_hdl hdl) {
    cc->failed = true;
    test_client::connection_ptr con = c->get_con_from_hdl(hdl);
    if (con) {
        std::cout << "  [fail] ec=" << con->get_ec().message()
                  << " local=" << con->get_local_close_code()
                  << " remote=" << con->get_remote_close_code() << std::endl;
    }
}

// ---------------------------------------------------------------------------

static void run_io(websocketpp::lib::asio::io_context * io) {
    io->run();
}

// Guards against a hang: if the test has not finished within the timeout, turn
// it into a hard failure instead of blocking forever (e.g. under ctest).
static bool g_done = false;

static void watchdog() {
    websocketpp::lib::asio::io_context io;
    websocketpp::lib::asio::steady_timer timer(io);
    timer.expires_after(std::chrono::seconds(30));
    timer.wait();
    if (!g_done) {
        std::cout << "TIMEOUT: proxy test did not complete" << std::endl;
        std::exit(2);
    }
}

int main() {
    const uint16_t UPSTREAM_PORT = 9101;
    const uint16_t PROXY_PORT = 9102;
    const std::string proxy_uri = "ws://localhost:9102";
    const std::string upstream_uri = "ws://localhost:9101";

    websocketpp::lib::asio::io_context io_upstream;
    websocketpp::lib::asio::io_context io_proxy;

    // ---- upstream pingpong server ----
    pingpong_server upstream;
    upstream.clear_access_channels(websocketpp::log::alevel::all);
    upstream.clear_error_channels(websocketpp::log::elevel::all);
    upstream.init_asio(&io_upstream);
    upstream.set_reuse_addr(true);
    upstream.set_validate_handler(bind(&pingpong_validate, &upstream, _1));
    upstream.set_message_handler(bind(&pingpong_on_message, &upstream, _1, _2));
    upstream.listen(UPSTREAM_PORT);
    upstream.start_accept();

    // ---- proxy ----
    proxy_server proxy(io_proxy, upstream_uri);
    proxy.listen(PROXY_PORT);

    // listen() ran synchronously, so both ports are bound before any client
    // connects. Run each server's io_context on its own thread.
    websocketpp::lib::thread t_upstream(bind(&run_io, &io_upstream));
    websocketpp::lib::thread t_proxy(bind(&run_io, &io_proxy));

    websocketpp::lib::thread t_watchdog(&watchdog);
    t_watchdog.detach();

    // ---- define the client cases ----
    // Distinct scenarios, each instantiated twice to exercise multiple
    // concurrent clients.
    std::vector<client_case> cases;
    for (int rep = 0; rep < 2; ++rep) {
        client_case a;                       // upstream picks the 2nd offer
        a.offer.push_back("chat.v2");        // (not supported)
        a.offer.push_back("chat.v1");        // (supported) -> selected
        a.expected = "chat.v1";
        cases.push_back(a);

        client_case b;                       // single supported offer
        b.offer.push_back("chat.v1");
        b.expected = "chat.v1";
        cases.push_back(b);

        client_case c;                       // client-order preference honored
        c.offer.push_back("echo");
        c.offer.push_back("chat.v1");
        c.expected = "echo";
        cases.push_back(c);

        client_case d;                       // no offers -> no subprotocol
        d.expected = "";
        cases.push_back(d);

        client_case e;                       // offer nothing the server knows
        e.offer.push_back("unsupported");
        e.expected = "";
        cases.push_back(e);
    }

    // ---- run all clients on a single client endpoint ----
    test_client client;
    client.clear_access_channels(websocketpp::log::alevel::all);
    client.clear_error_channels(websocketpp::log::elevel::all);
    client.init_asio();

    bool setup_ok = true;
    for (std::size_t i = 0; i < cases.size(); ++i) {
        websocketpp::lib::error_code ec;
        test_client::connection_ptr con = client.get_connection(proxy_uri, ec);
        if (ec) {
            std::cout << "  FAIL: get_connection: " << ec.message() << std::endl;
            setup_ok = false;
            break;
        }
        for (std::size_t j = 0; j < cases[i].offer.size(); ++j) {
            con->add_subprotocol(cases[i].offer[j], ec);
        }
        con->set_open_handler(bind(&client_on_open, &client, &cases[i], _1));
        con->set_message_handler(bind(&client_on_message, &client, &cases[i], _1, _2));
        con->set_close_handler(bind(&client_on_close, &cases[i], _1));
        con->set_fail_handler(bind(&client_on_fail, &client, &cases[i], _1));
        client.connect(con);
    }

    // Runs until every client connection has closed.
    client.run();

    // ---- tear down the servers ----
    io_proxy.stop();
    io_upstream.stop();
    t_proxy.join();
    t_upstream.join();

    // Everything past this point is deterministic and cannot hang.
    g_done = true;

    if (!setup_ok) {
        ++g_failures;
    }

    // ---- verify ----
    for (std::size_t i = 0; i < cases.size(); ++i) {
        client_case const & cc = cases[i];
        std::string const tag = "case " +
            to_str(i) + " (expected \"" + cc.expected + "\")";

        check(!cc.failed, tag + ": connection did not fail");
        check(cc.closed, tag + ": connection closed cleanly");
        check(cc.got_pong, tag + ": ping was answered with pong");
        check(cc.client_reported == cc.expected,
            tag + ": client get_subprotocol() == \"" + cc.expected +
            "\" (got \"" + cc.client_reported + "\")");
        check(cc.server_reported == cc.expected,
            tag + ": upstream negotiated \"" + cc.expected +
            "\" (got \"" + cc.server_reported + "\")");
    }

    if (g_failures == 0) {
        std::cout << "OK: " << cases.size()
                  << " clients proxied successfully (subprotocol negotiation included)"
                  << std::endl;
        return 0;
    }

    std::cout << g_failures << " check(s) failed" << std::endl;
    return 1;
}

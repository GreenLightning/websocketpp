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

#ifndef WEBSOCKETPP_EXAMPLE_PROXY_HPP
#define WEBSOCKETPP_EXAMPLE_PROXY_HPP

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/server.hpp>
#include <websocketpp/client.hpp>

#include <map>
#include <string>
#include <vector>

// A minimal transparent WebSocket proxy.
//
// It accepts downstream client connections on a listening port and, for each
// one, opens a matching connection to a single configured upstream WebSocket
// server. Once both legs are established, application messages are relayed
// verbatim in both directions.
//
// Subprotocol negotiation is proxied end to end:
//
//   1. The subprotocol offers the downstream client sent are forwarded to the
//      upstream server as the upstream connection's offers.
//   2. The downstream handshake response is deferred (defer_http_response())
//      until the upstream handshake completes, so the proxy does not have to
//      guess which subprotocol will be chosen.
//   3. Whatever subprotocol the upstream server selects is echoed back to the
//      downstream client in its (now un-deferred) 101 response.
//
// Both endpoints share a single io_context, so every handler runs on the same
// thread and no additional synchronization between the two legs is required.
class proxy_server {
public:
    typedef websocketpp::server<websocketpp::config::asio> downstream_endpoint;
    typedef websocketpp::client<websocketpp::config::asio_client> upstream_endpoint;

    typedef websocketpp::connection_hdl connection_hdl;
    typedef websocketpp::lib::error_code error_code;

    proxy_server(websocketpp::lib::asio::io_context & io,
                 std::string const & upstream_uri)
        : m_upstream_uri(upstream_uri)
    {
        using websocketpp::lib::bind;
        using websocketpp::lib::placeholders::_1;

        // The proxy relays payloads it never inspects, so library logging is
        // left off to keep it out of the way. Enable channels on the endpoints
        // returned by downstream()/upstream() if you want a trace.
        m_downstream.clear_access_channels(websocketpp::log::alevel::all);
        m_downstream.clear_error_channels(websocketpp::log::elevel::all);
        m_upstream.clear_access_channels(websocketpp::log::alevel::all);
        m_upstream.clear_error_channels(websocketpp::log::elevel::all);

        m_downstream.init_asio(&io);
        m_upstream.init_asio(&io);

        // Allow the listening socket to be re-bound immediately, which keeps
        // repeated runs (e.g. tests) from failing with "address in use".
        m_downstream.set_reuse_addr(true);

        m_downstream.set_validate_handler(
            bind(&proxy_server::on_validate, this, _1));
    }

    // Start listening for downstream clients. The shared io_context must be run
    // separately (e.g. io.run()).
    void listen(uint16_t port) {
        m_downstream.listen(port);
        m_downstream.start_accept();
    }

    // Stop accepting new downstream connections. Existing connections are left
    // to drain.
    void stop_listening() {
        error_code ec;
        m_downstream.stop_listening(ec);
    }

    downstream_endpoint & downstream() { return m_downstream; }
    upstream_endpoint & upstream() { return m_upstream; }

private:
    // Links one downstream connection to its upstream counterpart. The session
    // owns a strong reference to both connections, which is what keeps the
    // downstream connection alive while its handshake response is deferred (the
    // normal path is held open by the pending response write, which deferral
    // skips).
    struct session {
        downstream_endpoint::connection_ptr dcon;
        upstream_endpoint::connection_ptr ucon;
    };
    typedef websocketpp::lib::shared_ptr<session> session_ptr;

    // Sessions are looked up from a handler's connection_hdl by the underlying
    // connection pointer, which is unique across both endpoints.
    typedef std::map<void *, session_ptr> session_map;

    session_ptr find(connection_hdl hdl) {
        websocketpp::lib::shared_ptr<void> sp = hdl.lock();
        if (!sp) {
            return session_ptr();
        }
        session_map::iterator it = m_sessions.find(sp.get());
        if (it == m_sessions.end()) {
            return session_ptr();
        }
        return it->second;
    }

    void forget(session_ptr s) {
        if (s->dcon) {
            m_sessions.erase(s->dcon.get());
        }
        if (s->ucon) {
            m_sessions.erase(s->ucon.get());
        }
    }

    // --- downstream (client-facing) side ------------------------------------

    // Called for every incoming downstream handshake. Defers the response and
    // kicks off the upstream connection carrying the same subprotocol offers.
    bool on_validate(connection_hdl hdl) {
        using websocketpp::lib::bind;
        using websocketpp::lib::placeholders::_1;
        using websocketpp::lib::placeholders::_2;

        error_code ec;
        downstream_endpoint::connection_ptr dcon =
            m_downstream.get_con_from_hdl(hdl, ec);
        if (ec || !dcon) {
            return false;
        }

        // Create the upstream connection and mirror the downstream client's
        // subprotocol offers onto it.
        upstream_endpoint::connection_ptr ucon =
            m_upstream.get_connection(m_upstream_uri, ec);
        if (ec || !ucon) {
            // Reject the downstream handshake; we cannot reach the upstream.
            return false;
        }

        std::vector<std::string> const & offers =
            dcon->get_requested_subprotocols();
        for (std::vector<std::string>::const_iterator it = offers.begin();
             it != offers.end(); ++it)
        {
            ucon->add_subprotocol(*it, ec);
            if (ec) {
                return false;
            }
        }

        // Hold the downstream 101 until the upstream handshake completes.
        ec = dcon->defer_http_response();
        if (ec) {
            return false;
        }

        session_ptr s = websocketpp::lib::make_shared<session>();
        s->dcon = dcon;
        s->ucon = ucon;
        m_sessions[dcon.get()] = s;
        m_sessions[ucon.get()] = s;

        ucon->set_open_handler(
            bind(&proxy_server::on_upstream_open, this, _1));
        ucon->set_message_handler(
            bind(&proxy_server::on_upstream_message, this, _1, _2));
        ucon->set_close_handler(
            bind(&proxy_server::on_upstream_close, this, _1));
        ucon->set_fail_handler(
            bind(&proxy_server::on_upstream_fail, this, _1));

        dcon->set_message_handler(
            bind(&proxy_server::on_downstream_message, this, _1, _2));
        dcon->set_close_handler(
            bind(&proxy_server::on_downstream_close, this, _1));

        m_upstream.connect(ucon);
        return true;
    }

    void on_downstream_message(connection_hdl hdl,
        downstream_endpoint::message_ptr msg)
    {
        session_ptr s = find(hdl);
        // The downstream leg only opens after the upstream leg is established,
        // so the upstream connection is always ready here.
        if (s && s->ucon &&
            s->ucon->get_state() == websocketpp::session::state::open)
        {
            s->ucon->send(msg->get_payload(), msg->get_opcode());
        }
    }

    void on_downstream_close(connection_hdl hdl) {
        session_ptr s = find(hdl);
        if (!s) {
            return;
        }
        error_code ec;
        if (s->ucon &&
            s->ucon->get_state() == websocketpp::session::state::open)
        {
            s->ucon->close(websocketpp::close::status::normal,
                "downstream closed", ec);
        }
        forget(s);
    }

    // --- upstream (server-facing) side --------------------------------------

    // The upstream handshake is complete: relay the negotiated subprotocol back
    // to the downstream client and release its deferred 101 response.
    void on_upstream_open(connection_hdl hdl) {
        session_ptr s = find(hdl);
        if (!s || !s->dcon || !s->ucon) {
            return;
        }

        error_code ec;
        std::string const selected = s->ucon->get_subprotocol();
        if (!selected.empty()) {
            // select_subprotocol() records the choice (so the downstream
            // connection's get_subprotocol() reports it) and rejects a value
            // the client never offered. replace_header() then puts it on the
            // wire: process_handshake() already built the response during
            // validation, so simply selecting it is not enough on its own.
            s->dcon->select_subprotocol(selected, ec);
            if (ec) {
                s->ucon->close(websocketpp::close::status::protocol_error,
                    "upstream selected an unrequested subprotocol", ec);
                s->dcon->terminate(websocketpp::lib::error_code());
                forget(s);
                return;
            }
            s->dcon->replace_header("Sec-WebSocket-Protocol", selected, ec);
        }

        s->dcon->send_http_response(ec);
    }

    void on_upstream_message(connection_hdl hdl,
        upstream_endpoint::message_ptr msg)
    {
        session_ptr s = find(hdl);
        if (s && s->dcon &&
            s->dcon->get_state() == websocketpp::session::state::open)
        {
            s->dcon->send(msg->get_payload(), msg->get_opcode());
        }
    }

    void on_upstream_close(connection_hdl hdl) {
        session_ptr s = find(hdl);
        if (!s) {
            return;
        }
        error_code ec;
        if (s->dcon &&
            s->dcon->get_state() == websocketpp::session::state::open)
        {
            s->dcon->close(websocketpp::close::status::normal,
                "upstream closed", ec);
        }
        forget(s);
    }

    void on_upstream_fail(connection_hdl hdl) {
        session_ptr s = find(hdl);
        if (!s) {
            return;
        }
        // The upstream connection never opened. Tear down the (still deferred)
        // downstream connection so the client does not hang.
        if (s->dcon) {
            s->dcon->terminate(websocketpp::lib::error_code());
        }
        forget(s);
    }

    std::string m_upstream_uri;
    downstream_endpoint m_downstream;
    upstream_endpoint m_upstream;
    session_map m_sessions;
};

#endif // WEBSOCKETPP_EXAMPLE_PROXY_HPP

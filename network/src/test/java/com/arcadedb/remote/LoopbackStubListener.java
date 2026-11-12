/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.remote;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * The listening socket of a stub HTTP server that the client tests of this module dial as {@code 127.0.0.1:<port>}.
 * <p>
 * The socket is bound to {@code 127.0.0.1} itself, the address the client dials, never to the wildcard address that
 * {@code new ServerSocket(port)} takes. On macOS (and the BSDs) a wildcard listener does not own the port for every
 * address: another process can still bind {@code 127.0.0.1} on the same port, because Java sets {@code SO_REUSEADDR}
 * on every server socket, and the more specific binding then receives every connection to {@code 127.0.0.1}. During a
 * parallel build a stranger answered a stub server's client that way with a {@code 404 not found} (issue #11538).
 * A socket bound to {@code 127.0.0.1} makes the same stranger fail with "Address already in use", and a stranger that
 * binds the wildcard address instead is the less specific binding, so the client still reaches the stub.
 * <p>
 * Asking for port {@code 0} is fine here: the stub keeps the socket bound for as long as the test dials it, so the
 * kernel cannot hand the port to anybody else in the meantime.
 */
final class LoopbackStubListener {
  /** The address every stub-server test of this module dials, spelled the same way. */
  static final String HOST = "127.0.0.1";

  private LoopbackStubListener() {
  }

  /** A listener on {@code 127.0.0.1} and a port the kernel picks. */
  static ServerSocket bind() throws IOException {
    return bind(0);
  }

  /** A listener on {@code 127.0.0.1:port}; {@code 0} lets the kernel pick the port. */
  static ServerSocket bind(final int port) throws IOException {
    return new ServerSocket(port, 50, address());
  }

  static InetAddress address() throws UnknownHostException {
    return InetAddress.getByName(HOST);
  }
}

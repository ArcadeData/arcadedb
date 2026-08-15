/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import java.util.Set;

/**
 * The ways a server list can spell the loopback host, in one place. Three parts of the HA code have to
 * recognise it - the inbound gRPC allowlist seeds itself with it, the server-list validation rejects a list
 * that mixes it with real hosts, and the self-forward guard has to see that {@code localhost:2480} and
 * {@code 127.0.0.1:2480} are one socket (issue #6204) - and a spelling missing from any one copy is a silent
 * hole in that check rather than a visible failure.
 * <p>
 * Purely textual: no name is resolved. That keeps the recognition free of DNS on a request path and, more
 * importantly, honest - it answers "was this written as the loopback host", which is a question about the
 * configuration, not about what a resolver would say about it today.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LoopbackHosts {

  /** The IP literals, as they appear on the wire and in a server list. */
  static final Set<String> IPS = Set.of("127.0.0.1", "0:0:0:0:0:0:0:1", "::1");

  private LoopbackHosts() {
  }

  /**
   * True when {@code host} is the loopback host written in any of its accepted forms: the name
   * {@code localhost} in any case, the IPv4 literal, or the IPv6 literal with or without brackets.
   * <p>
   * Only <em>the</em> loopback address qualifies. {@code 127.0.0.2} is a loopback address as well, but a
   * different one, and two nodes bound to different addresses of the range with the same port is a working
   * single-machine cluster that must keep forwarding between its nodes.
   */
  static boolean isLoopback(final String host) {
    if (host == null || host.isEmpty())
      return false;

    final String bare = host.charAt(0) == '[' && host.charAt(host.length() - 1) == ']' ?
        host.substring(1, host.length() - 1) :
        host;

    // The IP literals carry no letters, so an exact match is already case-insensitive for them.
    return "localhost".equalsIgnoreCase(bare) || IPS.contains(bare);
  }
}

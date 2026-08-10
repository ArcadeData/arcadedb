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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link PostServerCommandHandler#isBlockedHost(String)}, the SSRF guard for client-supplied
 * restore/import URLs (no server startup required: the method is a pure function of the host string).
 * <p>
 * GHSA-67m7-7w7g-mpmh: an IPv6 literal that merely encodes a blocked IPv4 address through a transition mechanism
 * (NAT64, 6to4, Teredo) bypassed the previous flag-based checks. {@code isBlockedHost} now delegates to the same
 * {@link com.arcadedb.utility.IPAddressBlocklist} used by {@code ImportSecurityValidator.isBlockedAddress}, whose
 * own test covers the transition-address unwrapping exhaustively; these assert the handler's wiring to it.
 */
class PostServerCommandHandlerSsrfTest {

  @Test
  void blocksPlainPrivateAndLoopbackHosts() {
    assertThat(PostServerCommandHandler.isBlockedHost("127.0.0.1")).isTrue();
    assertThat(PostServerCommandHandler.isBlockedHost("169.254.169.254")).isTrue();
    assertThat(PostServerCommandHandler.isBlockedHost("192.168.1.1")).isTrue();
    assertThat(PostServerCommandHandler.isBlockedHost("10.0.0.5")).isTrue();
  }

  @Test
  void blocksIPv6TransitionAddressEncodingBlockedIPv4() {
    // NAT64 (RFC 6052) for 169.254.169.254 (cloud metadata)
    assertThat(PostServerCommandHandler.isBlockedHost("64:ff9b::a9fe:a9fe")).isTrue();
    // NAT64 (RFC 6052) for 192.168.1.1
    assertThat(PostServerCommandHandler.isBlockedHost("64:ff9b::c0a8:0101")).isTrue();
    // 6to4 (RFC 3056) for 192.168.1.1
    assertThat(PostServerCommandHandler.isBlockedHost("2002:c0a8:0101::1")).isTrue();
    // Teredo (RFC 4380) for 192.168.1.1
    assertThat(PostServerCommandHandler.isBlockedHost("2001:0000:4136:e378:8000:63bf:3f57:fefe")).isTrue();
  }

  @Test
  void allowsPublicHostsIncludingTransitionEncodedOnes() {
    assertThat(PostServerCommandHandler.isBlockedHost("8.8.8.8")).isFalse();
    // NAT64 for the public address 8.8.8.8 must not be blocked.
    assertThat(PostServerCommandHandler.isBlockedHost("64:ff9b::808:808")).isFalse();
  }

  @Test
  void blocksUnresolvableHost() {
    assertThat(PostServerCommandHandler.isBlockedHost("this-host-does-not-exist.invalid")).isTrue();
  }
}

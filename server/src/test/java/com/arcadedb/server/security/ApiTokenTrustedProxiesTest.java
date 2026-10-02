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
package com.arcadedb.server.security;

import com.arcadedb.utility.IPAddressBlocklist;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7821: the trusted-proxy contract shared by the HTTP and gRPC API token mints, pinned independently of
 * both callers.
 */
class ApiTokenTrustedProxiesTest {

  @Test
  void onlyEveryHopReportingHttpsIsFullyEncrypted() {
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("https")).isTrue();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted(" HTTPS , https ")).isTrue();

    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted(null)).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("   ")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("http")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("https,http")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("https,")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted("https,,https")).isFalse();
    assertThat(ApiTokenTrustedProxies.forwardedProtoIsFullyEncrypted(",https")).isFalse();
  }

  @Test
  void aBlankListTrustsNobody() {
    assertThat(ApiTokenTrustedProxies.parse(null).isEmpty()).isTrue();
    assertThat(ApiTokenTrustedProxies.parse("").isEmpty()).isTrue();
  }

  @Test
  void literalAddressesAndRangesAreMatched() throws Exception {
    final IPAddressBlocklist list = ApiTokenTrustedProxies.parse("10.0.0.5, 192.168.1.0/24, fd00::/8");

    assertThat(list.isBlocked(InetAddress.getByName("10.0.0.5"))).isTrue();
    assertThat(list.isBlocked(InetAddress.getByName("192.168.1.77"))).isTrue();
    assertThat(list.isBlocked(InetAddress.getByName("fd00::1"))).isTrue();
    assertThat(list.isBlocked(InetAddress.getByName("10.0.0.6"))).isFalse();
  }

  /** A typo in an allow-list has to deny, never widen, and must not throw into the caller. */
  @Test
  void anUnparseableListTrustsNobodyInsteadOfThrowing() {
    assertThat(ApiTokenTrustedProxies.parse("proxy.example.com").isEmpty()).isTrue();
    assertThat(ApiTokenTrustedProxies.parse("10.0.0.5, not-an-address").isEmpty()).isTrue();
  }
}

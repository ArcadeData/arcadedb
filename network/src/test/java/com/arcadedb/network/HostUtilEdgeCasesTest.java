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
package com.arcadedb.network;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HostUtilEdgeCasesTest {

  @Test
  void nullHostThrowsException() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress(null, HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Host null");
  }

  @Test
  void emptyHostThrowsException() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Host is empty");
  }

  @Test
  void whitespaceOnlyHostThrowsException() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("   ", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Host is empty");
  }

  @Test
  void invalidHostFormatThrowsException() {
    // 3 parts is invalid (neither IPv4/IPv6 nor with port)
    assertThatThrownBy(() -> HostUtil.parseHostAddress("a:b:c", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void hostWithLeadingAndTrailingWhitespace() {
    final String[] parts = HostUtil.parseHostAddress("  10.0.0.1  ", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("10.0.0.1");
    assertThat(parts[1]).isEqualTo(HostUtil.CLIENT_DEFAULT_PORT);
  }

  @Test
  void constantsAreCorrect() {
    assertThat(HostUtil.CLIENT_DEFAULT_PORT).isEqualTo("2480");
    assertThat(HostUtil.HA_DEFAULT_PORT).isEqualTo("2424");
  }

  @Test
  void iPv4WithCustomDefaultPort() {
    final String[] parts = HostUtil.parseHostAddress("192.168.1.1", "8080");
    assertThat(parts[0]).isEqualTo("192.168.1.1");
    assertThat(parts[1]).isEqualTo("8080");
  }

  @Test
  void iPv6WithHADefaultPort() {
    // IPv6 addresses have 8 groups when split by ':'
    final String[] parts = HostUtil.parseHostAddress("2001:db8:85a3:0:0:8a2e:370:7334", HostUtil.HA_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("2001:db8:85a3:0:0:8a2e:370:7334");
    assertThat(parts[1]).isEqualTo(HostUtil.HA_DEFAULT_PORT);
  }

  // -- Bracketed IPv6 (RFC 3986) --

  @Test
  void bracketedIPv6NoPort() {
    final String[] parts = HostUtil.parseHostAddress("[::1]", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("::1");
    assertThat(parts[1]).isEqualTo(HostUtil.CLIENT_DEFAULT_PORT);
  }

  @Test
  void bracketedIPv6WithPort() {
    final String[] parts = HostUtil.parseHostAddress("[::1]:2480", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("::1");
    assertThat(parts[1]).isEqualTo("2480");
  }

  @Test
  void bracketedIPv6FullWithPort() {
    final String[] parts = HostUtil.parseHostAddress("[2001:db8::1]:8080", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("2001:db8::1");
    assertThat(parts[1]).isEqualTo("8080");
  }

  @Test
  void bracketedIPv6FullNoPort() {
    final String[] parts = HostUtil.parseHostAddress("[2001:db8:85a3:0:0:8a2e:370:7334]", HostUtil.CLIENT_DEFAULT_PORT);
    assertThat(parts[0]).isEqualTo("2001:db8:85a3:0:0:8a2e:370:7334");
    assertThat(parts[1]).isEqualTo(HostUtil.CLIENT_DEFAULT_PORT);
  }

  // -- Malformed inputs that used to be silently accepted (issue #5891) --

  @Test
  void trailingColonIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void doubleTrailingColonIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host::", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void emptyHostBeforeColonIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress(":2480", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void bracketedIPv6EmptyPortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[::1]:", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void bracketedIPv6DoubleColonPortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[::1]::2480", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void emptyBracketedAddressIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[]", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void strayClosingBracketIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("]", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void nonNumericPortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:abc", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void negativePortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:-1", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void outOfRangePortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:99999999999999", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void portZeroIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:0", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void portAboveRangeIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("host:65536", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void validEdgePortsAreAccepted() {
    assertThat(HostUtil.parseHostAddress("host:1", HostUtil.CLIENT_DEFAULT_PORT)[1]).isEqualTo("1");
    assertThat(HostUtil.parseHostAddress("host:65535", HostUtil.CLIENT_DEFAULT_PORT)[1]).isEqualTo("65535");
  }

  @Test
  void bracketedIPv6NonNumericPortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[::1]:abc", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void bracketedIPv6NegativePortIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[::1]:-1", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }

  @Test
  void embeddedBracketInAddressIsRejected() {
    assertThatThrownBy(() -> HostUtil.parseHostAddress("[a[b]:80", HostUtil.CLIENT_DEFAULT_PORT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host");
  }
}

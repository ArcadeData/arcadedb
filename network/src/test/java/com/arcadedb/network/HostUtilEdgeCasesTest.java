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
}

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

import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for address parsing in RaftHAServer, including IPv6 bracketed notation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerAddressParsingTest {

  @Test
  void parseHostPortWithIPv4() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("192.168.1.1:2424");
    assertThat(result[0]).isEqualTo("192.168.1.1");
    assertThat(result[1]).isEqualTo("2424");
  }

  @Test
  void parseHostPortWithHostname() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("myhost:2424");
    assertThat(result[0]).isEqualTo("myhost");
    assertThat(result[1]).isEqualTo("2424");
  }

  @Test
  void parseHostPortWithBracketedIPv6() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("[::1]:2424");
    assertThat(result[0]).isEqualTo("[::1]");
    assertThat(result[1]).isEqualTo("2424");
  }

  @Test
  void parseHostPortWithFullIPv6() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("[2001:db8::1]:9090");
    assertThat(result[0]).isEqualTo("[2001:db8::1]");
    assertThat(result[1]).isEqualTo("9090");
  }

  @Test
  void parseHostPortWithExtraPortField() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("myhost:2424:2480");
    assertThat(result[0]).isEqualTo("myhost");
    assertThat(result[1]).isEqualTo("2424");
    assertThat(result[2]).isEqualTo("2480");
  }

  @Test
  void parseHostPortIPv6WithExtraPortField() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("[::1]:2424:2480");
    assertThat(result[0]).isEqualTo("[::1]");
    assertThat(result[1]).isEqualTo("2424");
    assertThat(result[2]).isEqualTo("2480");
  }

  @Test
  void parseHostPortRejectsMissingPort() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("myhost"))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void parseHostPortRejectsEmptyInput() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort(""))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void parseHostPortRejectsBareIPv6WithoutBrackets() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("::1:2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("IPv6");
  }

  @Test
  void parseHostPortRejectsIPv6MissingClosingBracket() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("[::1:2424"))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void parseHostPortRejectsBareIPv6LinkLocal() {
    // fe80::1:2424 has 4 colons and no dots - correctly detected as bare IPv6
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("fe80::1:2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("IPv6");
  }

  @Test
  void parseHostPortRejectsBareIPv6FullAddress() {
    // 2001:db8::1:2424 - full IPv6 without brackets
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("2001:db8::1:2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("IPv6");
  }

  @Test
  void parseHostPortAcceptsBracketedLinkLocal() {
    final String[] result = RaftPeerAddressResolver.parseHostPort("[fe80::1]:2424");
    assertThat(result[0]).isEqualTo("[fe80::1]");
    assertThat(result[1]).isEqualTo("2424");
  }
}

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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for peer address validation in RaftHAServer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAServerValidatePeerAddressTest {

  @Test
  void validIPv4Address() {
    assertThatCode(() -> RaftHAServer.validatePeerAddress("192.168.1.1:2424"))
        .doesNotThrowAnyException();
  }

  @Test
  void validHostnameAddress() {
    assertThatCode(() -> RaftHAServer.validatePeerAddress("myhost:2424"))
        .doesNotThrowAnyException();
  }

  @Test
  void validBracketedIPv6Address() {
    assertThatCode(() -> RaftHAServer.validatePeerAddress("[::1]:2424"))
        .doesNotThrowAnyException();
  }

  @Test
  void validBoundaryPortMin() {
    assertThatCode(() -> RaftHAServer.validatePeerAddress("myhost:1"))
        .doesNotThrowAnyException();
  }

  @Test
  void validBoundaryPortMax() {
    assertThatCode(() -> RaftHAServer.validatePeerAddress("myhost:65535"))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsPortZero() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("myhost:0"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("port");
  }

  @Test
  void rejectsPortAbove65535() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("myhost:70000"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("port");
  }

  @Test
  void rejectsNegativePort() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("myhost:-1"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("port");
  }

  @Test
  void rejectsNonNumericPort() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("myhost:abc"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("port");
  }

  @Test
  void rejectsMissingPort() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("myhost"))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void rejectsEmptyAddress() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress(""))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void rejectsEmptyHost() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress(":2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("host");
  }

  @Test
  void rejectsIPv6PortOutOfRange() {
    assertThatThrownBy(() -> RaftHAServer.validatePeerAddress("[::1]:99999"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("port");
  }
}

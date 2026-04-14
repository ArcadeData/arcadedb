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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ClusterTokenProvider}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusterTokenProviderTest {

  @Test
  void deriveTokenProducesDeterministicResult() {
    final char[] pw = "secret123".toCharArray();
    final String token1 = ClusterTokenProvider.deriveTokenFromPassword("myCluster", pw);

    // pw was not zeroed by the method (caller owns it), so we can reuse
    final String token2 = ClusterTokenProvider.deriveTokenFromPassword("myCluster", pw);
    assertThat(token1).isEqualTo(token2);
  }

  @Test
  void differentClusterNamesProduceDifferentTokens() {
    final String token1 = ClusterTokenProvider.deriveTokenFromPassword("cluster-a", "password".toCharArray());
    final String token2 = ClusterTokenProvider.deriveTokenFromPassword("cluster-b", "password".toCharArray());
    assertThat(token1).isNotEqualTo(token2);
  }

  @Test
  void differentPasswordsProduceDifferentTokens() {
    final String token1 = ClusterTokenProvider.deriveTokenFromPassword("cluster", "pw1".toCharArray());
    final String token2 = ClusterTokenProvider.deriveTokenFromPassword("cluster", "pw2".toCharArray());
    assertThat(token1).isNotEqualTo(token2);
  }

  @Test
  void callerCanZeroPasswordAfterDerivation() {
    // Verifies the defense-in-depth pattern: callers pass char[] and zero it after use.
    final char[] password = "sensitivePassword".toCharArray();

    final String token = ClusterTokenProvider.deriveTokenFromPassword("test", password);
    assertThat(token).isNotEmpty();

    // Caller zeros the password
    Arrays.fill(password, '\0');

    // Verify it's actually zeroed
    for (final char c : password)
      assertThat(c).isEqualTo('\0');

    // Token derivation with the same original value should still produce the same result
    final String token2 = ClusterTokenProvider.deriveTokenFromPassword("test", "sensitivePassword".toCharArray());
    assertThat(token).isEqualTo(token2);
  }

  @Test
  void tokenIsHexEncoded() {
    final String token = ClusterTokenProvider.deriveTokenFromPassword("cluster", "password".toCharArray());
    // PBKDF2 with 256-bit key = 64 hex characters
    assertThat(token).hasSize(64);
    assertThat(token).matches("[0-9a-f]+");
  }
}

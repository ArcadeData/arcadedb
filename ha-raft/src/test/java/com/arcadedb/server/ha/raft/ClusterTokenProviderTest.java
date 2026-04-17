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

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

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

  /**
   * Known-answer test: the derived token for a fixed {@code (clusterName, password)} pair is
   * pinned to a specific value. Any silent downgrade of the PBKDF2 parameters (iteration count,
   * HMAC algorithm, key length, salt construction) flips the output and breaks this assertion.
   * <p>
   * Expected value was generated externally with the same parameters ArcadeDB's
   * {@link ClusterTokenProvider} uses in production: PBKDF2WithHmacSHA256, 100&nbsp;000
   * iterations, 256-bit key, salt {@code "arcadedb-cluster-token:" + clusterName}, password
   * {@code clusterName + ":" + password}.
   */
  @Test
  void knownAnswerTestPinsPbkdf2Parameters() {
    final String token = ClusterTokenProvider.deriveTokenFromPassword(
        "known-answer-cluster", "known-answer-password".toCharArray());
    assertThat(token).isEqualTo("4bd1c732d5a9bda9f88b2d96176f74304ce2cc432921d33a7a4d5b9c6f336743");
  }

  /**
   * Negative control for the known-answer test: if the production code were silently downgraded
   * to a weaker PBKDF2 configuration (e.g., 1000 iterations), this independently-computed token
   * would still differ from the one produced by {@link ClusterTokenProvider}. Guards against a
   * refactor that changes iterations in both the implementation and the known-answer test at
   * the same time.
   */
  @Test
  void weakIterationCountProducesDifferentToken() throws Exception {
    final String clusterName = "known-answer-cluster";
    final String password = "known-answer-password";

    // Independently compute with 1000 iterations (vs production 100_000).
    final char[] pwChars = (clusterName + ":" + password).toCharArray();
    final byte[] salt = ("arcadedb-cluster-token:" + clusterName).getBytes(StandardCharsets.UTF_8);
    final PBEKeySpec spec = new PBEKeySpec(pwChars, salt, 1000, 256);
    final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
    final String weakToken = HexFormat.of().formatHex(factory.generateSecret(spec).getEncoded());

    final String productionToken = ClusterTokenProvider.deriveTokenFromPassword(clusterName, password.toCharArray());

    assertThat(weakToken).isNotEqualTo(productionToken);
    assertThat(weakToken).hasSize(64);
  }

  /**
   * Verifies that the token is NOT derivable from the cluster name alone: two different
   * passwords under the same cluster name must produce non-trivially different tokens (not just
   * a tail-difference that would suggest the password was truncated or ignored).
   */
  @Test
  void tokenDependsNonTriviallyOnPassword() {
    final String token1 = ClusterTokenProvider.deriveTokenFromPassword(
        "same-cluster", "password-variant-a".toCharArray());
    final String token2 = ClusterTokenProvider.deriveTokenFromPassword(
        "same-cluster", "password-variant-b".toCharArray());

    assertThat(token1).isNotEqualTo(token2);
    // Hamming distance should be large: for a proper cryptographic PRF, flipping input bits
    // avalanches across the output. Require that the first 16 hex chars (64 bits) differ,
    // which would be astronomically unlikely by chance (~2^-64) unless the password is actually
    // being mixed into the derivation.
    assertThat(token1.substring(0, 16)).isNotEqualTo(token2.substring(0, 16));
  }
}

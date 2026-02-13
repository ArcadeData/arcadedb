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
package com.arcadedb.bolt;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for Bolt protocol version encoding helpers and negotiation logic.
 */
class BoltVersionNegotiationTest {

  // ============ Version encoding helper tests ============

  @Test
  void getMajorVersion() {
    assertThat(BoltNetworkExecutor.getMajorVersion(0x00000404)).isEqualTo(4); // v4.4
    assertThat(BoltNetworkExecutor.getMajorVersion(0x00000004)).isEqualTo(4); // v4.0
    assertThat(BoltNetworkExecutor.getMajorVersion(0x00000003)).isEqualTo(3); // v3.0
    assertThat(BoltNetworkExecutor.getMajorVersion(0x00000405)).isEqualTo(5); // v5.4
    assertThat(BoltNetworkExecutor.getMajorVersion(0x00000000)).isEqualTo(0); // padding
  }

  @Test
  void getMinorVersion() {
    assertThat(BoltNetworkExecutor.getMinorVersion(0x00000404)).isEqualTo(4); // v4.4
    assertThat(BoltNetworkExecutor.getMinorVersion(0x00000004)).isEqualTo(0); // v4.0
    assertThat(BoltNetworkExecutor.getMinorVersion(0x00000003)).isEqualTo(0); // v3.0
    assertThat(BoltNetworkExecutor.getMinorVersion(0x00000405)).isEqualTo(4); // v5.4
  }

  @Test
  void getVersionRange() {
    assertThat(BoltNetworkExecutor.getVersionRange(0x00020404)).isEqualTo(2); // v4.4 range=2
    assertThat(BoltNetworkExecutor.getVersionRange(0x00000404)).isEqualTo(0); // v4.4 no range
    assertThat(BoltNetworkExecutor.getVersionRange(0x00030405)).isEqualTo(3); // v5.4 range=3
  }

  @Test
  void versionEncodingRoundTrip() {
    // Verify that encoding major=4, minor=4 yields the expected constant
    final int version = (4 << 8) | 4;
    assertThat(version).isEqualTo(0x00000404);
    assertThat(BoltNetworkExecutor.getMajorVersion(version)).isEqualTo(4);
    assertThat(BoltNetworkExecutor.getMinorVersion(version)).isEqualTo(4);
    assertThat(BoltNetworkExecutor.getVersionRange(version)).isEqualTo(0);
  }

  // ============ Version negotiation tests ============
  // These test the negotiation algorithm by simulating what performHandshake() does
  // with various client version proposals against the server's SUPPORTED_VERSIONS.

  private static final int[] SUPPORTED_VERSIONS = { 0x00000404, 0x00000004, 0x00000003 }; // v4.4, v4.0, v3.0

  /**
   * Simulates the version negotiation logic from BoltNetworkExecutor.performHandshake().
   */
  private static int negotiate(final int[] clientVersions) {
    for (final int clientVersion : clientVersions) {
      if (clientVersion == 0)
        break;

      final int clientMajor = BoltNetworkExecutor.getMajorVersion(clientVersion);
      final int clientMinor = BoltNetworkExecutor.getMinorVersion(clientVersion);
      final int clientRange = BoltNetworkExecutor.getVersionRange(clientVersion);

      for (final int supportedVersion : SUPPORTED_VERSIONS) {
        final int serverMajor = BoltNetworkExecutor.getMajorVersion(supportedVersion);
        final int serverMinor = BoltNetworkExecutor.getMinorVersion(supportedVersion);

        if (clientMajor == serverMajor && serverMinor <= clientMinor && serverMinor >= clientMinor - clientRange) {
          return supportedVersion;
        }
      }
    }
    return 0;
  }

  @Test
  void exactVersionMatch() {
    // Client proposes exactly v4.4
    final int result = negotiate(new int[] { 0x00000404, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000404);
  }

  @Test
  void exactMatchV4_0() {
    final int result = negotiate(new int[] { 0x00000004, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000004);
  }

  @Test
  void exactMatchV3_0() {
    final int result = negotiate(new int[] { 0x00000003, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000003);
  }

  @Test
  void rangeMatchClientSupports4_2through4_4() {
    // Client proposes v4.4 with range=2 (supports 4.2, 4.3, 4.4)
    final int result = negotiate(new int[] { 0x00020404, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000404); // server's v4.4 falls in range
  }

  @Test
  void rangeMatchClientSupports4_1through4_4() {
    // Client proposes v4.4 with range=3 (supports 4.1, 4.2, 4.3, 4.4)
    final int result = negotiate(new int[] { 0x00030404, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000404);
  }

  @Test
  void rangeMatchFallsBackToV4_0() {
    // Client proposes v4.2 with range=2 (supports 4.0, 4.1, 4.2)
    // Server has v4.4 (too high) but also v4.0 (in range)
    final int result = negotiate(new int[] { 0x00020204, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000004); // v4.0
  }

  @Test
  void serverVersionOutsideClientRange() {
    // Client proposes v4.3 with range=0 (only 4.3 exactly)
    // Server supports v4.4 and v4.0, neither is 4.3
    final int result = negotiate(new int[] { 0x00000304, 0, 0, 0 });
    assertThat(result).isEqualTo(0);
  }

  @Test
  void noMatchUnsupportedMajorVersion() {
    // Client only supports Bolt v5.x
    final int result = negotiate(new int[] { 0x00020405, 0x00000005, 0, 0 });
    assertThat(result).isEqualTo(0);
  }

  @Test
  void neo4j5xDriverTypicalHandshake() {
    // Simulate a modern Neo4j 5.x driver proposing:
    // v5.4 range=2 (5.2-5.4), v5.1 range=1 (5.0-5.1), v4.4 range=1 (4.3-4.4), v4.2 exact
    final int result = negotiate(new int[] { 0x00020405, 0x00010105, 0x00010404, 0x00000204 });
    // Server doesn't support 5.x, should negotiate to v4.4
    assertThat(result).isEqualTo(0x00000404);
  }

  @Test
  void neo4j4xDriverTypicalHandshake() {
    // Simulate an older Neo4j 4.x driver proposing:
    // v4.4, v4.0, v3.0, padding
    final int result = negotiate(new int[] { 0x00000404, 0x00000004, 0x00000003, 0 });
    assertThat(result).isEqualTo(0x00000404);
  }

  @Test
  void clientPrefersHigherVersionFirst() {
    // Client proposes v5.0 first then v4.4 — should pick v4.4 (first match)
    final int result = negotiate(new int[] { 0x00000005, 0x00000404, 0, 0 });
    assertThat(result).isEqualTo(0x00000404);
  }

  @Test
  void allZeroPaddingReturnsNoMatch() {
    final int result = negotiate(new int[] { 0, 0, 0, 0 });
    assertThat(result).isEqualTo(0);
  }

  @Test
  void zeroAfterValidVersionStopsProcessing() {
    // First entry is unsupported v5.0, second is zero (padding), third would match v4.4
    // but should stop at zero per Bolt spec
    final int result = negotiate(new int[] { 0x00000005, 0, 0x00000404, 0 });
    assertThat(result).isEqualTo(0);
  }

  @Test
  void clientRangeCoversMultipleServerVersions() {
    // Client proposes v4.4 with range=4 (supports 4.0 through 4.4)
    // Server's first supported version is v4.4, which should be picked (highest preference)
    final int result = negotiate(new int[] { 0x00040404, 0, 0, 0 });
    assertThat(result).isEqualTo(0x00000404);
  }
}

/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The compare-and-set precondition of issue #7509 is only as good as this fingerprint.
 * <p>
 * Two properties have to hold at once, and they pull in opposite directions. It must be INSENSITIVE to how a
 * node happens to have serialised the document - the user map is a {@code ConcurrentHashMap}, the group
 * document is assembled key by key, and a fingerprint that changed with iteration order would refuse every
 * entry between two healthy nodes. And it must be SENSITIVE to content, or it would accept the stale document
 * the issue is about.
 */
class SecurityDocumentFingerprintTest {

  @Test
  void objectKeyOrderIsNotPartOfTheDocument() {
    assertThat(SecurityDocumentFingerprint.of("{\"version\":2,\"databases\":{\"a\":1,\"b\":2}}"))
        .isEqualTo(SecurityDocumentFingerprint.of("{\"databases\":{\"b\":2,\"a\":1},\"version\":2}"));
  }

  @Test
  void arrayOrderIsNotPartOfTheDocument() {
    assertThat(SecurityDocumentFingerprint.of("[{\"name\":\"alice\"},{\"name\":\"bob\"}]"))
        .isEqualTo(SecurityDocumentFingerprint.of("[{\"name\":\"bob\"},{\"name\":\"alice\"}]"));
  }

  @Test
  void whitespaceIsNotPartOfTheDocument() {
    assertThat(SecurityDocumentFingerprint.of("{\"a\": 1}"))
        .isEqualTo(SecurityDocumentFingerprint.of("{\"a\":1}"));
  }

  @Test
  void anAddedEntryChangesTheFingerprint() {
    assertThat(SecurityDocumentFingerprint.of("[{\"name\":\"alice\"}]"))
        .isNotEqualTo(SecurityDocumentFingerprint.of("[{\"name\":\"alice\"},{\"name\":\"bob\"}]"));
  }

  @Test
  void aRemovedEntryChangesTheFingerprint() {
    assertThat(SecurityDocumentFingerprint.of("[{\"name\":\"alice\"},{\"name\":\"bob\"}]"))
        .isNotEqualTo(SecurityDocumentFingerprint.of("[{\"name\":\"alice\"}]"));
  }

  @Test
  void aChangedValueChangesTheFingerprint() {
    assertThat(SecurityDocumentFingerprint.of("[{\"name\":\"alice\",\"password\":\"one\"}]"))
        .isNotEqualTo(SecurityDocumentFingerprint.of("[{\"name\":\"alice\",\"password\":\"two\"}]"));
  }

  /**
   * Two strings must not be able to run together into one: without escaping, {@code ["a","b"]} and
   * {@code ["ab"]} would canonicalise to the same characters and a revocation could be made to look like the
   * document it revoked.
   */
  @Test
  void adjacentStringsCannotCollide() {
    assertThat(SecurityDocumentFingerprint.of("[\"a\",\"b\"]"))
        .isNotEqualTo(SecurityDocumentFingerprint.of("[\"ab\"]"));
    assertThat(SecurityDocumentFingerprint.of("[\"a\\\"\",\"b\"]"))
        .isNotEqualTo(SecurityDocumentFingerprint.of("[\"a\",\"\\\"b\"]"));
  }

  @Test
  void theSameDocumentAlwaysFingerprintsTheSame() {
    final String document = "{\"version\":1,\"tokens\":[{\"tokenHash\":\"ab12\",\"name\":\"ci\"}]}";
    assertThat(SecurityDocumentFingerprint.of(document))
        .isEqualTo(SecurityDocumentFingerprint.of(document))
        .hasSize(64);
  }

  @Test
  void aDocumentThatIsNotJsonIsRejectedRatherThanFingerprinted() {
    assertThatThrownBy(() -> SecurityDocumentFingerprint.of("not json"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SecurityDocumentFingerprint.of(null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}

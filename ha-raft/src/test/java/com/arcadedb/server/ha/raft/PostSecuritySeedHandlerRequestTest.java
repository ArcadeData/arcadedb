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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ReplicatedSecurityFingerprintRepository;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@link PostSecuritySeedHandler} makes of the request body, before any Raft entry is submitted
 * (claude-review on PR #7854).
 * <p>
 * Two decisions live there and neither is reached by the integration tests, which send well-formed requests
 * from the production client: whether the caller's digests mean "already in step", and what a garbled
 * {@code fingerprints} field is answered with. The second matters because the failure was a 500 - this node is
 * broken - for a request that is simply wrong, which is the kind of answer that sends an operator looking at
 * the wrong machine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostSecuritySeedHandlerRequestTest {

  private static final String USERS  = "users-digest";
  private static final String GROUPS = "groups-digest";
  private static final String TOKENS = "tokens-digest";

  // ------------------------------------------------------------------ the fingerprint comparison

  /** All three match: nothing is submitted, which is what makes a no-op rolling restart cost no Raft entries. */
  @Test
  void aCallerHoldingEveryDocumentIsUpToDate() {
    assertThat(PostSecuritySeedHandler.isUpToDate(USERS, GROUPS, TOKENS, fingerprints(USERS, GROUPS, TOKENS)))
        .isTrue();
  }

  /** One differing digest is enough to seed: the documents are replaced as a set, not one by one. */
  @Test
  void oneDifferingDocumentIsNotUpToDate() {
    assertThat(PostSecuritySeedHandler.isUpToDate(USERS, GROUPS, TOKENS,
        fingerprints(USERS, "a-group-document-from-before-this-node-went-down", TOKENS)))
        .isFalse();
  }

  /**
   * A caller that names only some of them is seeded rather than refused. An absent digest cannot match, which
   * is the right answer for an incomplete set and needs no error of its own.
   */
  @Test
  void anIncompleteSetOfDigestsIsNotUpToDate() {
    assertThat(PostSecuritySeedHandler.isUpToDate(USERS, GROUPS, TOKENS,
        new JSONObject().put(ReplicatedSecurityFingerprintRepository.USERS, USERS)))
        .isFalse();
  }

  // ------------------------------------------------------------------ reading the field

  /** A request that names no fingerprints is an admission: it is seeded, not compared. */
  @Test
  void aRequestWithoutFingerprintsReadsAsNone() {
    assertThat(PostSecuritySeedHandler.readFingerprints(new JSONObject().put("reason", "an admission"))).isNull();
  }

  /** And one that names them reads them back. */
  @Test
  void aRequestWithFingerprintsReadsThem() {
    assertThat(PostSecuritySeedHandler.readFingerprints(
        new JSONObject().put("fingerprints", fingerprints(USERS, GROUPS, TOKENS)))
        .getString(ReplicatedSecurityFingerprintRepository.USERS, ""))
        .isEqualTo(USERS);
  }

  /**
   * The regression: a {@code fingerprints} that is present and is not an object used to throw out of the
   * handler as a 500. The two-argument {@code getJSONObject} substitutes its default only for an ABSENT field,
   * not for one of the wrong type.
   */
  @Test
  void aFingerprintsFieldOfTheWrongTypeIsRefusedRatherThanThrown() {
    for (final Object malformed : new Object[] { "a string", new JSONArray().put("an array"), 42 })
      assertThatThrownBy(() -> PostSecuritySeedHandler.readFingerprints(
          new JSONObject().put("fingerprints", malformed)))
          .as("a %s must be answered as a bad request, not as a broken node", malformed.getClass().getSimpleName())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("fingerprints");
  }

  // ------------------------------------------------------------------ which requests may reuse a recent seed

  /**
   * The request type is read from the body, not inferred from whether fingerprints came with it. A catch-up on
   * a node with NO security store has no digests to send, and inferring the type from their absence read it as
   * an admission - which may be answered by a seed it did not cause, leaving the node it was repairing stale
   * (CodeRabbit on PR #7854).
   */
  @Test
  void aCatchUpWithNoFingerprintsIsStillACatchUp() {
    final JSONObject request = new JSONObject().put("reason", "a node with no security store").put("catchUp", true);

    assertThat(PostSecuritySeedHandler.readFingerprints(request)).as("it carries none").isNull();
    assertThat(request.getBoolean("catchUp", false))
        .as("and is still not allowed to reuse a recent seed")
        .isTrue();
  }

  /** An admission says nothing, which is what lets it be answered by the membership change's own seed. */
  @Test
  void anAdmissionIsNotMarkedAsACatchUp() {
    assertThat(new JSONObject().put("reason", "the admission of peer 'arcadedb-3'").getBoolean("catchUp", false))
        .isFalse();
  }

  /** The three digests a caller sends, in the shape the client builds them. */
  private static JSONObject fingerprints(final String users, final String groups, final String apiTokens) {
    return new JSONObject()
        .put(ReplicatedSecurityFingerprintRepository.USERS, users)
        .put(ReplicatedSecurityFingerprintRepository.GROUPS, groups)
        .put(ReplicatedSecurityFingerprintRepository.API_TOKENS, apiTokens);
  }
}

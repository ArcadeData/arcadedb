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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.ExecutionResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7521: {@code POST /api/v1/cluster/peer} reported a security seed that had not
 * landed as an HTTP <b>200</b> carrying a {@code warning} field.
 * <p>
 * Operator automation reads the status, so a 200 meant the peer was treated as fully joined while it kept
 * authenticating and authorizing from its own copies of {@code server-users.jsonl}, {@code server-groups.json}
 * and {@code server-api-tokens.json}. For a node re-added after having been out of the cluster, those are the
 * security state from whenever it left: a user dropped since, a group narrowed since or a token revoked since is
 * still in force there.
 * <p>
 * The status/body contract is what this pins. The retry that produces the list, and the
 * {@code connect cluster} sibling that reaches the same seed, are pinned by
 * {@code Issue7521SecuritySeedRetryTest} in the server module.
 */
class Issue7521SeedOutcomeResponseTest {

  @Test
  void aFullySeededPeerIsAPlain200() {
    final ExecutionResponse response = PostAddPeerHandler.seedOutcomeResponse("node2", List.of());

    assertThat(response.getCode()).isEqualTo(200);

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("result")).isEqualTo("Peer node2 added");
    assertThat(body.has("error")).as("nothing failed, so nothing is reported").isFalse();
    assertThat(body.has("detail")).isFalse();
  }

  /**
   * 503 rather than 200: the caller has to see a failure. 503 rather than 500 because the seed goes through Raft
   * and its usual failure is a momentary loss of quorum - the condition clears, and re-issuing the very same
   * request is the fix.
   */
  @Test
  void aPeerWhoseSeedDidNotLandIsA503NamingTheDocuments() {
    final ExecutionResponse response = PostAddPeerHandler.seedOutcomeResponse("node2",
        List.of("users", "API tokens"));

    assertThat(response.getCode())
        .as("a partial failure must not be reported as success")
        .isEqualTo(503);

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("result"))
        .as("the peer IS a member - the membership change is not rolled back")
        .isEqualTo("Peer node2 added");

    assertThat(body.getString("error"))
        .as("the short half names the documents the peer is stale on")
        .contains("users").contains("API tokens");

    final String detail = body.getString("detail");
    assertThat(detail)
        .as("the operator has to be told what the peer enforces in the meantime")
        .contains("authenticates and authorizes from its own copies");
    assertThat(detail)
        .as("and what to do about it - re-running the request is idempotent and reissues the seed")
        .contains("Re-run this request");
  }

  /** One failing document is named on its own, without the separator a joined list would leave dangling. */
  @Test
  void aSingleFailingDocumentIsNamedWithoutASeparator() {
    final ExecutionResponse response = PostAddPeerHandler.seedOutcomeResponse("node3", List.of("groups"));

    assertThat(response.getCode()).isEqualTo(503);
    assertThat(new JSONObject(response.getResponse()).getString("error"))
        .endsWith("seeded to it: groups");
  }
}

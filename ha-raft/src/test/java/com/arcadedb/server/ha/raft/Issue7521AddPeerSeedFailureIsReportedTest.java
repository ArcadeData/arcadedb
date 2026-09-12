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
 * Regression test for issue #7521, the {@code POST /api/v1/cluster/peer} half.
 * <p>
 * The route admits the peer and then seeds it with the three security documents a Raft snapshot install does
 * not carry. Before this, a seed that never committed was reported as HTTP <b>200</b> carrying a
 * {@code warning} field - so an operator's automation, which reads the status code, recorded a clean
 * admission while the new peer went on serving requests against its own config directory. For a node re-added
 * after time out of the cluster that directory can still hold a user dropped since, a group narrowed since or
 * a token revoked since.
 * <p>
 * {@link PostAddPeerHandler#addPeerResponse} is the decision this pins: which status code the handler answers
 * with, and what the body has to carry so the operator can act on it. The membership half is deliberately not
 * rolled back - the peer is already a committed member - so both outcomes still report it as added.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7521AddPeerSeedFailureIsReportedTest {

  @Test
  void anAdmissionWhoseSeedsAllCommittedIs200() {
    final ExecutionResponse response = PostAddPeerHandler.addPeerResponse("arcadedb-3", List.of());

    assertThat(response.getCode()).isEqualTo(200);

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("result")).contains("arcadedb-3");
    assertThat(body.has("error")).as("a clean admission carries no error").isFalse();
    assertThat(body.has("failedSeeds")).as("a clean admission carries no failure list").isFalse();
  }

  /**
   * The status code is the assertion. A {@code warning} inside a 200 is invisible to every caller that checks
   * the status and moves on, which is what an operator's join automation does.
   */
  @Test
  void anAdmissionWhoseSeedDidNotCommitIsNotReportedAsSuccess() {
    final ExecutionResponse response = PostAddPeerHandler.addPeerResponse("arcadedb-3", List.of("API tokens"));

    assertThat(response.getCode())
        .as("a peer serving with its own stale credentials must not read as a clean join")
        .isEqualTo(503);
  }

  /**
   * The operator has to know <i>which</i> documents did not land, because the remediation differs: re-issuing
   * a user change is not the same act as re-issuing a token revocation. A machine-readable list, not only
   * prose, so a join script can branch on it.
   */
  @Test
  void theFailedDocumentsAreNamedBothInProseAndAsAList() {
    final ExecutionResponse response = PostAddPeerHandler.addPeerResponse("arcadedb-3",
        List.of("users", "API tokens"));

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("error")).contains("users").contains("API tokens");
    assertThat(body.getJSONArray("failedSeeds").toList())
        .as("reported in the order seedSecurityStateClusterWide reports them")
        .containsExactly("users", "API tokens");
  }

  /**
   * Split across {@code error} and {@code detail} the way {@code AbstractServerHttpHandler.error2json} splits
   * every other failure on this server. Studio's {@code globalNotifyError} renders {@code error} as the
   * notification title and {@code detail} as its body, so a single long {@code error} becomes an unreadable
   * title over the placeholder "Error on execution of the command".
   */
  @Test
  void theSummaryAndTheRemediationAreSeparateFields() {
    final ExecutionResponse response = PostAddPeerHandler.addPeerResponse("arcadedb-3", List.of("users"));

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("error")).as("a title, not a paragraph").hasSizeLessThan(160);
    assertThat(body.getString("detail")).contains("Re-POST").contains("securitySeedRetryTimeout");
  }

  /**
   * The membership change did happen, and a caller that concluded from the 503 that the cluster is unchanged
   * would be wrong about its own topology. The remediation is re-POSTing the same peer, which is idempotent
   * on the membership change and reissues the seed - so the body says the peer was added even while failing.
   */
  @Test
  void aFailedSeedStillReportsThatThePeerBecameAMember() {
    final ExecutionResponse response = PostAddPeerHandler.addPeerResponse("arcadedb-3", List.of("groups"));

    final JSONObject body = new JSONObject(response.getResponse());
    assertThat(body.getString("result")).as("the peer IS a member").contains("arcadedb-3").contains("added");
    assertThat(body.getString("detail")).contains("Re-POST the same peer");
  }
}

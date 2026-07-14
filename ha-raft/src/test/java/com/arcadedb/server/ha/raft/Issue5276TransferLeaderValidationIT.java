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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5276: request validation and honest outcome reporting on
 * {@code POST /api/v1/cluster/leader}.
 * <ol>
 *   <li>A missing request body must return 400 with a usage message, not a 500 from an NPE.</li>
 *   <li>An unrecognized field (e.g. {@code "target"} instead of {@code "peerId"}) must return 400
 *       naming the bad field, instead of being silently ignored and executing a transfer-to-any -
 *       the "false success" reported in the issue.</li>
 *   <li>A successful targeted transfer must report the confirmed leader in the response.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5276TransferLeaderValidationIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void invalidRequestsReturn400WithoutTransferring() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    // 1) Missing body: must be a 400 with a usage hint, not a 500 NPE.
    final HttpResult noBody = postTransferLeader(leaderIndex, null);
    assertThat(noBody.code()).as("Response: %s", noBody.body()).isEqualTo(400);
    assertThat(noBody.body()).contains("peerId");

    // 2) Unknown field ("target" instead of "peerId"): must be rejected loudly, NOT silently
    // ignored (which executed a transfer-to-any and reported success - issue #5276).
    final String unknownField = new JSONObject().put("target", peerIdForIndex(leaderIndex == 0 ? 1 : 0)).toString();
    final HttpResult badField = postTransferLeader(leaderIndex, unknownField);
    assertThat(badField.code()).as("Response: %s", badField.body()).isEqualTo(400);
    assertThat(badField.body()).contains("target").contains("peerId");

    // Neither invalid request may have moved leadership.
    assertThat(findLeaderIndex()).isEqualTo(leaderIndex);
  }

  @Test
  void targetedTransferReportsConfirmedLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int targetIndex = leaderIndex == 0 ? 1 : 0;
    final String targetPeerId = peerIdForIndex(targetIndex);

    final String body = new JSONObject().put("peerId", targetPeerId).put("timeoutMs", 10_000).toString();
    final HttpResult result = postTransferLeader(leaderIndex, body);
    assertThat(result.code()).as("Response: %s", result.body()).isEqualTo(200);

    // The response must report the confirmed leader, so callers can verify the outcome instead of
    // trusting a bare success string (issue #5276).
    final JSONObject response = new JSONObject(result.body());
    assertThat(response.getString("leaderId", "")).isEqualTo(targetPeerId);

    // And leadership must actually be on the target.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(findLeaderIndex()).isEqualTo(targetIndex));
  }

  private HttpResult postTransferLeader(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + (2480 + serverIndex) + "/api/v1/cluster/leader").toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (body != null)
      conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));

    final int code = conn.getResponseCode();
    final String responseBody;
    if (code >= 400 && conn.getErrorStream() != null)
      responseBody = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    else if (conn.getInputStream() != null)
      responseBody = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    else
      responseBody = "";
    conn.disconnect();
    return new HttpResult(code, responseBody);
  }

  private record HttpResult(int code, String body) {
  }
}

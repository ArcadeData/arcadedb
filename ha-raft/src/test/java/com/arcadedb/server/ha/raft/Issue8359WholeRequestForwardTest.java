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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.RecordingLeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.database;
import static com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.forward;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8359, the forwarding side: the forward that relays the client's key (issue #8347) used to post a body rebuilt
 * from the statement, so the leader answered it - and settled the client's key with - its default rendering, not the
 * 'serializer', 'limit' or 'typeHints' the client's body names. That forward now posts the client's own body, and the
 * leader's answer is handed back to the handler to be sent as it is. A forward that is only a part of the request keeps
 * the rebuilt body and hands nothing back.
 */
class Issue8359WholeRequestForwardTest {

  private static final String KEY     = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final String COMMAND = "INSERT INTO V SET id = 1";
  private static final String BODY    =
      "{ \"serializer\": \"studio\", \"limit\": 7, \"typeHints\": true, \"language\": \"sql\", \"command\": \"" + COMMAND
          + "\" }";
  // What a leader renders for the 'studio' serializer: not a row array, so it cannot be parsed back into rows.
  private static final String STUDIO_ANSWER =
      "{\"user\":\"root\",\"result\":{\"vertices\":[],\"edges\":[],\"records\":[{\"id\":1}]},\"limit\":7}";

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  @Test
  void theWholeRequestForwardPostsTheClientsBodyAndHandsBackTheLeadersAnswer() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader(STUDIO_ANSWER)) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY);

      forward(db, COMMAND);

      assertThat(leader.bodies()).as("the client's own body, byte for byte").containsExactly(BODY);
      assertThat(leader.clientKeys()).containsExactly(KEY);
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer())
          .as("the leader's answer in the client's rendering, as it is").isEqualTo(STUDIO_ANSWER);
    }
  }

  /** A second forward in the same request is a part of it: the body is rebuilt and nothing is handed back. */
  @Test
  void aSecondForwardPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader(STUDIO_ANSWER)) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY);

      forward(db, COMMAND);
      ForwardedRequestIdContext.takeWholeRequestAnswer();
      forward(db, COMMAND);

      assertThat(leader.bodies()).hasSize(2);
      assertRebuilt(leader.bodies().get(1));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  /** A write the declared statement issues from inside - another statement - is a part of the request. */
  @Test
  void aForwardOfAnotherStatementPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8359", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "SELECT writes()", BODY);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(leader.clientKeys()).containsExactly((String) null);
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  /** Without the cluster token the key is not relayed, and neither is the body: the forward is what it was before. */
  @Test
  void withoutAClusterTokenTheBodyIsRebuilt() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, false, null);
      ForwardedRequestIdContext.set("client-8359", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  /** A POST to this node itself, the new leader, runs the request here: nothing to substitute or hand back. */
  @Test
  void aForwardToItselfPostsTheRebuiltBody() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, true);
      ForwardedRequestIdContext.set("client-8359", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", COMMAND, BODY);

      forward(db, COMMAND);

      assertRebuilt(leader.bodies().get(0));
      assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    }
  }

  private static void assertRebuilt(final String body) {
    final JSONObject json = new JSONObject(body);
    assertThat(json.getString("command")).isEqualTo(COMMAND);
    assertThat(json.has("serializer")).as("a rebuilt body carries none of the client's rendering fields").isFalse();
    assertThat(json.has("limit")).isFalse();
  }
}

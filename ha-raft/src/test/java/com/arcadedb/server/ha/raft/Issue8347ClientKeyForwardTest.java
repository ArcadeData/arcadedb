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

import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.RecordingLeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.database;
import static com.arcadedb.server.ha.raft.Issue8323SqlForwardRequestIdTest.forward;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8347, the forwarding side: the SQL write a follower forwards to the leader carries a body rebuilt from the
 * statement, so the leader could not match it to a retry the client sends it directly with its own body. The forward
 * now relays, beside the client's id, the key the client's request has on the follower - but only when the forward IS
 * that whole request. A write issued by a command the follower executes itself, or any forward after the first, is a
 * part of the request: settling the client's key with its answer would replay that answer to a direct retry of the
 * whole request.
 */
class Issue8347ClientKeyForwardTest {

  private static final String KEY  = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
  private static final String BODY = "{\"language\":\"sql\",\"command\":\"INSERT INTO V SET id = 1\"}";

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  @Test
  void theForwardOfAOneCommandRequestRelaysTheClientKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8347");
      assertThat(leader.clientKeys()).containsExactly(KEY);
    }
  }

  @Test
  void aSecondForwardInTheSameRequestRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");
      forward(db, "INSERT INTO V SET id = 2");

      assertThat(leader.clientKeys()).containsExactly(KEY, null);
    }
  }

  /** A route whose body is not one command (the handler did not opt in) never relays its key on a command forward. */
  @Test
  void aRouteThatIsNotOneCommandRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader);
      ForwardedRequestIdContext.set("client-8347", KEY, false);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.requestIds()).containsExactly("client-8347");
      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  /** Without a cluster token the leader would not honor it, so it is not sent. */
  @Test
  void withoutAClusterTokenNoKeyIsSent() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, false, null);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  /** The POST goes to this node itself, which holds the client's reservation already: nothing is relayed. */
  @Test
  void aForwardToItselfAsTheNewLeaderRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final RaftReplicatedDatabase db = database(leader, true);
      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);

      forward(db, "INSERT INTO V SET id = 1");

      assertThat(leader.clientKeys()).containsExactly((String) null);
    }
  }

  /**
   * Through the real command() entry point: a write command is forwarded whole and relays the key; a read-only command
   * is executed on the follower, and the write it issues from inside - forwarded on its own - is a part of the request
   * and relays none.
   */
  @Test
  void aWriteIssuedByACommandExecutedLocallyRelaysNoKey() throws Exception {
    try (final RecordingLeader leader = new RecordingLeader()) {
      final LocalDatabase proxied = mock(LocalDatabase.class);
      final QueryEngineManager engines = mock(QueryEngineManager.class);
      final QueryEngine engine = mock(QueryEngine.class);
      when(proxied.getQueryEngineManager()).thenReturn(engines);
      when(engines.getEngine(eq("sql"), any())).thenReturn(engine);
      when(engine.analyze(anyString())).thenAnswer(inv -> analyzed(((String) inv.getArgument(0)).startsWith("SELECT")));

      final RaftReplicatedDatabase db = database(leader, false, "test-token", proxied);
      // The local execution of the read-only command issues one write through the same database.
      when(proxied.command(eq("sql"), eq("SELECT nested()"), any(), any(Object[].class))).thenAnswer(inv -> {
        db.command("sql", "INSERT INTO V SET id = 3");
        return new InternalResultSet();
      });

      ForwardedRequestIdContext.set("client-8347", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "INSERT INTO V SET id = 1", BODY);
      db.command("sql", "INSERT INTO V SET id = 1");
      assertThat(leader.clientKeys()).as("the write command is the whole request").containsExactly(KEY);

      ForwardedRequestIdContext.set("client-8347-b", KEY, true);
      ForwardedRequestIdContext.declareWholeRequestCommand("sql", "SELECT nested()", BODY);
      db.command("sql", "SELECT nested()");
      assertThat(leader.requestIds()).containsExactly("client-8347", "client-8347-b");
      assertThat(leader.clientKeys()).as("the nested write is a part of the request").containsExactly(KEY, null);
    }
  }

  private static QueryEngine.AnalyzedQuery analyzed(final boolean idempotent) {
    return new QueryEngine.AnalyzedQuery() {
      @Override
      public boolean isIdempotent() {
        return idempotent;
      }

      @Override
      public boolean isDDL() {
        return false;
      }
    };
  }
}

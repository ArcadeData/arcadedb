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
package com.arcadedb.server.ws;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11779: a {@code /ws} insert session tagged its thread {@code ws} only while it applied the rows of a chunk, so
 * the {@code begin()} of the {@code start} frame and the {@code commit()}/{@code rollback()} of the closing frame of a
 * {@code per_stream} session ran tagged {@link ProtocolContext#INTERNAL}. Under HA that is the value the
 * directory-replacement and quarantine gate of {@code RaftReplicatedDatabase} exempts, so the session's own transaction was
 * never refused. Every frame is now tagged where it is executed.
 * <p>
 * The tag is read where the gate reads it, at {@code begin()} and {@code commit()} of the database, through the same
 * {@code setWrappedDatabaseInstance} hook {@code RaftReplicatedDatabase} installs itself with.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue11779WsInsertProtocolTagTest extends BaseGraphServerTest {
  private static final String GATE_REFUSAL = "Database is being replaced on this server from the leader's snapshot";

  private final List<String>  seen       = new CopyOnWriteArrayList<>();
  private final AtomicBoolean gateClosed = new AtomicBoolean();
  private final AtomicBoolean committedButPublicationFailed = new AtomicBoolean();

  private LocalDatabase local;

  @BeforeEach
  void installRecordingWrapper() {
    local = (LocalDatabase) ((DatabaseInternal) getServerDatabase(0, getDatabaseName())).getEmbedded();
    final DatabaseInternal real = local;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          final String name = method.getName();
          if (method.getParameterCount() == 0 && ("begin".equals(name) || "commit".equals(name) || "rollback".equals(name)))
            seen.add(name + "=" + ProtocolContext.get());
          // What RaftReplicatedDatabase.refuseClientWhileDirectoryIsReplaced() does: refuse a CLIENT commit, exempt INTERNAL
          if (gateClosed.get() && "commit".equals(name) && method.getParameterCount() == 0 && !ProtocolContext.INTERNAL.equals(
              ProtocolContext.get())) {
            real.rollback();
            throw new NeedRetryException(GATE_REFUSAL);
          }
          // What the HA leader raises when the entry committed cluster-wide and the local publication then conflicted
          if (committedButPublicationFailed.get() && "commit".equals(name) && method.getParameterCount() == 0) {
            method.invoke(real, args);
            throw new TransactionCommittedRemotelyException("Transaction committed cluster-wide but the local apply failed",
                new ConcurrentModificationException("page version mismatch"));
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    local.setWrappedDatabaseInstance(wrapper);
  }

  @AfterEach
  void removeRecordingWrapper() {
    gateClosed.set(false);
    committedButPublicationFailed.set(false);
    // A database with no HA wrapper is its own wrapped instance: this restores the state before the test
    if (local != null && local.isOpen())
      local.setWrappedDatabaseInstance(local);
  }

  @Test
  void perStreamBeginAndCommitAreTaggedWs() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client);
      assertThat(chunk(client, sessionId).getLong("inserted", -1)).isEqualTo(1);
      final JSONObject committed = new JSONObject(
          client.send(new JSONObject().put("action", "commit").put("sessionId", sessionId).toString()));
      assertThat(committed.getString("action", "")).isEqualTo("committed");
    }

    assertThat(seen).as("the start frame's begin and the commit frame's commit are client work, tagged ws")
        .contains("begin=ws", "commit=ws").doesNotContain("begin=internal", "commit=internal");
  }

  @Test
  void theHaGateRefusesTheSessionCommitAndTheClientIsToldToRetry() throws Exception {
    final long before = getServerDatabase(0, getDatabaseName()).countType("Person", false);
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client);
      chunk(client, sessionId);
      gateClosed.set(true);
      final JSONObject answer = new JSONObject(
          client.send(new JSONObject().put("action", "commit").put("sessionId", sessionId).toString()));
      gateClosed.set(false);

      assertThat(answer.getString("result", "")).isEqualTo("error");
      assertThat(answer.getString("error", "")).as("a retryable refusal, not a server fault").isEqualTo("Retry later");
      assertThat(answer.getString("detail", "")).isEqualTo(GATE_REFUSAL);
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType("Person", false)).as("the refused session wrote nothing")
        .isEqualTo(before);
  }

  @Test
  void aSessionCommittedClusterWideIsNeverAnsweredAsRetryable() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client);
      chunk(client, sessionId);
      committedButPublicationFailed.set(true);
      final JSONObject answer = new JSONObject(
          client.send(new JSONObject().put("action", "commit").put("sessionId", sessionId).toString()));
      committedButPublicationFailed.set(false);

      assertThat(answer.getString("result", "")).isEqualTo("error");
      assertThat(answer.getString("error", "")).as("the nested conflict must not invite a replay of a committed load")
          .doesNotContain("Retry").contains("do not retry");
    }
  }

  @Test
  void perStreamRollbackIsTaggedWs() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client);
      chunk(client, sessionId);
      final JSONObject rolledBack = new JSONObject(
          client.send(new JSONObject().put("action", "rollback").put("sessionId", sessionId).toString()));
      assertThat(rolledBack.getString("result", "")).isEqualTo("ok");
    }

    assertThat(seen).contains("begin=ws", "rollback=ws").doesNotContain("begin=internal", "rollback=internal");
  }

  private WebSocketClientHelper connect() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServerHttpPort(0) + "/ws", "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String start(final WebSocketClientHelper client) throws Exception {
    final JSONObject started = new JSONObject(client.send(new JSONObject().put("action", "start").put("database", getDatabaseName())
        .put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "per_stream")).toString()));
    assertThat(started.getString("action", "")).isEqualTo("started");
    return started.getString("sessionId");
  }

  private static JSONObject chunk(final WebSocketClientHelper client, final String sessionId) throws Exception {
    return new JSONObject(client.send(new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", 1)
        .put("records", new JSONArray().put(new JSONObject().put("name", "n"))).toString()));
  }
}

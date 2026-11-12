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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11541: {@link RemoteDatabase#commit()} reset the identity of the records created in the transaction on EVERY
 * failure, so after a commit whose outcome is unknown (a dispatched replication that timed out, a 503 page of a hop in front
 * of the server, a transport failure after the request was sent) or that did land (409 committed remotely), a caller that
 * re-saved the same objects inserted them a second time. The identities now go back to provisional only when the failure
 * proves the commit was not applied.
 * <p>
 * No server is involved: the {@code /commit} request is answered from a script, or fails with a scripted transport error.
 */
class Issue11541RemoteCommitIdentityTest {
  private static final RID ASSIGNED = new RID(3, 7);

  private final List<ScriptedDatabase> opened = new ArrayList<>();

  @AfterEach
  void closeDatabases() {
    for (final ScriptedDatabase database : opened)
      database.close();
  }

  // ---- OUTCOMES THAT LEAVE OPEN THAT THE COMMIT WAS APPLIED: THE ASSIGNED RID IS KEPT -------------------------------------

  @Test
  void aReplicationThatTimedOutAfterDispatchKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(500, typed("com.arcadedb.server.ha.raft.ReplicationDispatchedTimeoutException",
        "dispatched, outcome unknown"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class);

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  @Test
  void anOutcomeTheServerCouldNotLearnKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(500, typed("com.arcadedb.exception.TransactionOutcomeUnknownException", "outcome unknown"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class);

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  @Test
  void aProxy503PageKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = new Scripted(503, "<html><body><h1>503 Service Temporarily Unavailable</h1></body></html>");

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class)
        .hasMessageContaining("may have been applied");

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  @Test
  void aCommitThatLandedRemotelyKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(409, typed(TransactionCommittedRemotelyException.class.getName(), "committed cluster-wide"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(TransactionCommittedRemotelyException.class);

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  @Test
  void aTransportFailureAfterTheRequestWasSentKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitFailure = new HttpTimeoutException("request timed out");

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(HttpTimeoutException.class);

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  @Test
  void anUntypedServerErrorKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(500, new JSONObject().put("error", "Internal error").put("detail", "boom"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class);

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  /**
   * The failure arm of {@code transaction()} resets the created records too, after the commit already decided: it must not
   * undo a decision to keep them. A re-save of the kept record is then an update of the RID the server may hold.
   */
  @Test
  void transactionDoesNotResetWhatAnUncertainCommitKept() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(500, typed("com.arcadedb.server.ha.raft.ReplicationDispatchedTimeoutException",
        "dispatched, outcome unknown"));

    final MutableDocument[] record = new MutableDocument[1];
    assertThatThrownBy(() -> database.transaction(() -> record[0] = createdRecord(database), false, 3))
        .isInstanceOf(TransactionException.class);

    assertThat(record[0].getIdentity()).isEqualTo(ASSIGNED);
    // NOT RETRIED: AN UNCERTAIN OUTCOME IS NEVER RE-RUN
    assertThat(database.commits).isEqualTo(1);
  }

  // ---- OUTCOMES THAT PROVE THE COMMIT WAS NOT APPLIED: THE IDENTITY GOES BACK TO PROVISIONAL (ISSUE #4562) -----------------

  @Test
  void aRetryableRefusalResetsTheIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(503, typed(ConcurrentModificationException.class.getName(), "conflict"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(NeedRetryException.class);

    assertThat(record.getIdentity()).isNull();
  }

  @Test
  void anUntypedServer503ResetsTheIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(503, new JSONObject().put("error", "Server is installing a snapshot, please retry"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(NeedRetryException.class);

    assertThat(record.getIdentity()).isNull();
  }

  @Test
  void aDuplicatedKeyResetsTheIdentity() {
    final ScriptedDatabase database = open();
    database.commitAnswer = answer(409, typed(DuplicatedKeyException.class.getName(), "duplicated key")
        .put("exceptionArgs", "Person[name]|Jay|#3:1"));

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(DuplicatedKeyException.class);

    assertThat(record.getIdentity()).isNull();
  }

  @Test
  void aConnectionThatCouldNotBeEstablishedResetsTheIdentity() {
    final ScriptedDatabase database = open();
    database.commitFailure = new ConnectException("Connection refused");

    final MutableDocument record = createdInTransaction(database);
    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class)
        .hasCauseInstanceOf(ConnectException.class);

    assertThat(record.getIdentity()).isNull();
  }

  @Test
  void anExplicitRollbackResetsTheIdentity() {
    final ScriptedDatabase database = open();

    final MutableDocument record = createdInTransaction(database);
    database.rollback();

    assertThat(record.getIdentity()).isNull();
  }

  @Test
  void aSuccessfulCommitKeepsTheAssignedIdentity() {
    final ScriptedDatabase database = open();

    final MutableDocument record = createdInTransaction(database);
    database.commit();

    assertThat(record.getIdentity()).isEqualTo(ASSIGNED);
  }

  /** The predicate on its own, for the cases the HTTP mapping cannot produce from a scripted answer. */
  @Test
  void theProofIsOnlyARefusalADuplicateOrAConnectionNeverMade() {
    assertThat(RemoteDatabase.provesCommitNotApplied(new NeedRetryException("refused"))).isTrue();
    assertThat(RemoteDatabase.provesCommitNotApplied(new DuplicatedKeyException("idx", "k", null))).isTrue();
    assertThat(RemoteDatabase.provesCommitNotApplied(new ConnectException("refused"))).isTrue();

    assertThat(RemoteDatabase.provesCommitNotApplied(new RemoteIntermediaryRefusalException("503 page"))).isFalse();
    assertThat(RemoteDatabase.provesCommitNotApplied(new TransactionCommittedRemotelyException("landed"))).isFalse();
    assertThat(RemoteDatabase.provesCommitNotApplied(new TransactionException("unknown"))).isFalse();
    assertThat(RemoteDatabase.provesCommitNotApplied(new RemoteException("untyped"))).isFalse();
    assertThat(RemoteDatabase.provesCommitNotApplied(new IOException("connection reset"))).isFalse();
    assertThat(RemoteDatabase.provesCommitNotApplied(null)).isFalse();
  }

  // ---- FIXTURE -------------------------------------------------------------------------------------------------------------

  private ScriptedDatabase open() {
    final ScriptedDatabase database = new ScriptedDatabase();
    opened.add(database);
    return database;
  }

  /** Begins a transaction and registers a record the server assigned {@link #ASSIGNED} to, as {@code saveRecord} does. */
  private static MutableDocument createdInTransaction(final ScriptedDatabase database) {
    database.begin();
    return createdRecord(database);
  }

  private static MutableDocument createdRecord(final ScriptedDatabase database) {
    final MutableDocument record = new MutableDocument(null, null, null) {
    };
    record.setIdentity(ASSIGNED);
    database.trackCreatedRecord(record);
    return record;
  }

  private static JSONObject typed(final String exception, final String detail) {
    return new JSONObject().put("error", detail).put("detail", detail).put("exception", exception);
  }

  private static Scripted answer(final int status, final JSONObject body) {
    return new Scripted(status, body.toString());
  }

  private record Scripted(int status, String body) {
  }

  /** Answers {@code /begin} with a session, {@code /rollback} with success, and {@code /commit} from the script. */
  private static final class ScriptedDatabase extends RemoteDatabase {
    Scripted    commitAnswer;
    IOException commitFailure;
    int         commits;

    ScriptedDatabase() {
      super("127.0.0.1", 2480, "db", "root", "password", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // NO SERVER TO ASK
    }

    @Override
    protected void sleepBeforeRetry(final long delayMs) {
      // NOTHING TO WAIT FOR
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException {
      final String path = request.uri().getPath();
      final String route = path.substring("/api/v1/".length(), path.lastIndexOf('/'));

      final Map<String, List<String>> headers = new HashMap<>();
      if ("commit".equals(route)) {
        ++commits;
        if (commitFailure != null)
          throw commitFailure;
        if (commitAnswer != null)
          return new ScriptedResponse(request, commitAnswer.status(), commitAnswer.body(),
              HttpHeaders.of(headers, (name, value) -> true));
      } else if ("begin".equals(route))
        headers.put(ARCADEDB_SESSION_ID, List.of("AS-1"));

      return new ScriptedResponse(request, 204, "", HttpHeaders.of(headers, (name, value) -> true));
    }
  }

  private record ScriptedResponse(HttpRequest request, int statusCode, String body, HttpHeaders headers)
      implements HttpResponse<String> {
    @Override
    public Optional<HttpResponse<String>> previousResponse() {
      return Optional.empty();
    }

    @Override
    public Optional<SSLSession> sslSession() {
      return Optional.empty();
    }

    @Override
    public URI uri() {
      return request.uri();
    }

    @Override
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }
  }
}

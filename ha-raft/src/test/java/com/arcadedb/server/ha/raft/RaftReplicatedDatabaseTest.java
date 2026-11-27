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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RaftReplicatedDatabaseTest {

  @Test
  void implementsDatabaseInternal() {
    assertThat(DatabaseInternal.class.isAssignableFrom(RaftReplicatedDatabase.class)).isTrue();
  }

  /**
   * Issue #9959: {@code DatabaseInternal.saveConfiguration(String)} falls back to the whole-difference save, so a
   * database class that saves the configuration and does not forward the keyed save would ship, under Raft HA, a change
   * another statement has not submitted yet. Every one of them declares it.
   */
  @Test
  void everyDatabaseThatSavesTheConfigurationDeclaresTheKeyedSave() throws NoSuchMethodException {
    for (final Class<?> database : List.of(LocalDatabase.class, ServerDatabase.class, RaftReplicatedDatabase.class))
      assertThat(database.getDeclaredMethod("saveConfiguration", String.class).getDeclaringClass()).isEqualTo(database);
  }

  @Test
  void parseResultSetFromJsonWithRecords() {
    final String json = "{\"result\":[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((String) results.get(0).getProperty("name")).isEqualTo("Alice");
    assertThat((int) results.get(0).getProperty("age")).isEqualTo(30);
    assertThat((String) results.get(1).getProperty("name")).isEqualTo("Bob");
  }

  @Test
  void parseResultSetFromJsonEmptyResult() {
    final String json = "{\"result\":[]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonNoResultKey() {
    final String json = "{\"user\":\"root\"}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void parseResultSetFromJsonWithScalarValues() {
    final String json = "{\"result\":[42,\"hello\"]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(2);
    assertThat((int) results.get(0).getProperty("value")).isEqualTo(42);
    assertThat((String) results.get(1).getProperty("value")).isEqualTo("hello");
  }

  /**
   * Issue #12519: a row rebuilt from the leader's JSON is plain data whatever it stood for there, so the gRPC service cannot tell
   * the record it was (its {@code @type} column) from a local projection with a column of that name, except by the mark this
   * method puts on every row it builds.
   */
  @Test
  void parseResultSetFromJsonMarksEveryRowItBuilds() {
    final String json = "{\"result\":[{\"@rid\":\"#1:0\",\"@type\":\"P\",\"@cat\":\"v\",\"name\":\"a\"},{\"name\":\"b\"},42]}";

    final ResultSet rs = RaftReplicatedDatabase.parseResultSetFromJson(json);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());

    assertThat(results).hasSize(3);
    for (final Result result : results) {
      assertThat(result.isElement()).isFalse();
      assertThat(result.getMetadata(ResultInternal.FORWARDED_ROW_METADATA)).isEqualTo(Boolean.TRUE);
    }
    assertThat((String) results.get(0).getProperty("@type")).isEqualTo("P");
  }

  // reconstructLeaderException tests

  @Test
  void reconstructLeaderExceptionDuplicatedKey() {
    final String body = "{\"error\":\"Found duplicate key in index\","
        + "\"detail\":\"Duplicated key [42] found on index 'Account_PK' already assigned to record #7:1\","
        + "\"exception\":\"com.arcadedb.exception.DuplicatedKeyException\","
        + "\"exceptionArgs\":\"Account_PK|[42]|#7:1\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(DuplicatedKeyException.class);
    final DuplicatedKeyException dup = (DuplicatedKeyException) result;
    assertThat(dup.getIndexName()).isEqualTo("Account_PK");
    assertThat(dup.getKeys()).isEqualTo("[42]");
    assertThat(dup.getCurrentIndexedRID()).isEqualTo(new RID(7, 1L));
  }

  /**
   * The answer a node gives to a write forwarded onto it by a peer whose leader address named the wrong node
   * (issue #6191). It has to survive the hop as itself: collapsed into a plain TransactionException the caller
   * loses both the retryability it inherits from NeedRetryException and the leader the message names.
   */
  @Test
  void reconstructLeaderExceptionNotTheLeaderKeepsTypeAndLeader() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Refusing to forward a write that a cluster peer already forwarded to the leader\","
        + "\"exception\":\"com.arcadedb.network.binary.ServerIsNotTheLeaderException\","
        + "\"exceptionArgs\":\"ArcadeDB_0\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(400, body);

    assertThat(result).isInstanceOf(ServerIsNotTheLeaderException.class);
    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(((ServerIsNotTheLeaderException) result).getLeaderAddress()).isEqualTo("ArcadeDB_0");
    assertThat(result.getMessage())
        .isEqualTo("Refusing to forward a write that a cluster peer already forwarded to the leader");
  }

  /** The leader was unknown to the refusing node, so there is no address to carry: the type still is. */
  @Test
  void reconstructLeaderExceptionNotTheLeaderWithoutALeaderAddress() {
    final String body = "{\"error\":\"Cannot execute command\",\"detail\":\"not the leader\","
        + "\"exception\":\"com.arcadedb.network.binary.ServerIsNotTheLeaderException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(400, body);

    assertThat(result).isInstanceOf(ServerIsNotTheLeaderException.class);
    assertThat(((ServerIsNotTheLeaderException) result).getLeaderAddress()).isNull();
    assertThat(result.getMessage()).isEqualTo("not the leader");
  }

  @Test
  void reconstructLeaderExceptionNeedRetry() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Record has been modified by another transaction\","
        + "\"exception\":\"com.arcadedb.exception.NeedRetryException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Record has been modified by another transaction");
  }

  @Test
  void reconstructLeaderExceptionConcurrentModification() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Concurrent modification detected\","
        + "\"exception\":\"com.arcadedb.exception.ConcurrentModificationException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    // Reconstructed as the exact type, not collapsed to its NeedRetryException supertype, so a
    // caller catching ConcurrentModificationException specifically still matches.
    assertThat(result).isInstanceOf(ConcurrentModificationException.class);
    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Concurrent modification detected");
  }

  @Test
  void reconstructLeaderExceptionLockTimeoutIsRetryable() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Timeout on acquiring lock\","
        + "\"exception\":\"com.arcadedb.exception.LockTimeoutException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(503, body);

    // LockTimeoutException extends NeedRetryException: it must stay retryable on the Follower.
    assertThat(result).isInstanceOf(LockTimeoutException.class);
    assertThat(result).isInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Timeout on acquiring lock");
  }

  @Test
  void reconstructLeaderExceptionTimeoutIsNotRetryable() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Query timeout\","
        + "\"exception\":\"com.arcadedb.exception.TimeoutException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    // A plain (query-deadline) TimeoutException is NOT retryable and must not become a
    // NeedRetryException, otherwise callers would wrongly retry it.
    assertThat(result).isInstanceOf(TimeoutException.class);
    assertThat(result).isNotInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).isEqualTo("Query timeout");
  }

  @Test
  void reconstructLeaderExceptionSqlParsingKeepsSubtype() {
    final String body = "{\"error\":\"Error on parsing query\","
        + "\"detail\":\"Syntax error near 'SELCT'\","
        + "\"exception\":\"com.arcadedb.exception.CommandSQLParsingException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    // Reconstructed as the exact subtype, so a caller catching CommandParsingException matches too.
    assertThat(result).isInstanceOf(CommandSQLParsingException.class);
    assertThat(result).isInstanceOf(CommandParsingException.class);
    assertThat(result.getMessage()).isEqualTo("Syntax error near 'SELCT'");
  }

  @Test
  void reconstructLeaderExceptionCommittedRemotelyKeepsDoNotRetryContract() {
    // #5064: a follower forwarding a write must hand the caller the SAME do-not-retry contract the
    // leader produced - collapsing it to a generic TransactionException (or worse, a retryable type)
    // would let the client re-drive a transaction the cluster already committed, inserting duplicates.
    final String body = "{\"error\":\"Transaction committed cluster-wide but the local apply failed - do not retry\","
        + "\"detail\":\"Transaction TX(1) is committed cluster-wide but the local apply failed. Do NOT retry: reload the records and continue\","
        + "\"exception\":\"com.arcadedb.exception.TransactionCommittedRemotelyException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(TransactionCommittedRemotelyException.class);
    assertThat(result).isNotInstanceOf(NeedRetryException.class);
    assertThat(result.getMessage()).contains("committed cluster-wide").contains("Do NOT retry");
  }

  @Test
  void reconstructLeaderExceptionQueryNotIdempotent() {
    final String body = "{\"error\":\"Cannot execute command\","
        + "\"detail\":\"Query is not idempotent\","
        + "\"exception\":\"com.arcadedb.exception.QueryNotIdempotentException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(QueryNotIdempotentException.class);
    assertThat(result.getMessage()).isEqualTo("Query is not idempotent");
  }

  @Test
  void reconstructLeaderExceptionTransactionException() {
    final String body = "{\"error\":\"Error on transaction commit\","
        + "\"detail\":\"Commit failed\","
        + "\"exception\":\"com.arcadedb.exception.TransactionException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).isEqualTo("Commit failed");
  }

  @Test
  void reconstructLeaderExceptionUnknownClass() {
    final String body = "{\"error\":\"Some error\","
        + "\"detail\":\"Some detail\","
        + "\"exception\":\"com.example.SomeUnknownException\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionNoExceptionField() {
    final String body = "{\"error\":\"Internal error\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, body);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionMalformedJson() {
    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, "not-json");

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionNullBody() {
    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(500, null);

    assertThat(result).isInstanceOf(TransactionException.class);
    assertThat(result.getMessage()).contains("Leader returned HTTP 500");
  }

  @Test
  void reconstructLeaderExceptionDuplicatedKeyMalformedArgs() {
    // exceptionArgs with wrong number of segments - should fall back to TransactionException
    final String body = "{\"error\":\"Found duplicate key in index\","
        + "\"exception\":\"com.arcadedb.exception.DuplicatedKeyException\","
        + "\"exceptionArgs\":\"only-one-segment\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(TransactionException.class);
  }

  /**
   * Issue #9473: a key value containing the '|' separator. Splitting into three parts put "b]|#7:1" in the RID slot,
   * the RID failed to parse, and the forwarded duplicate lost its type on the follower.
   */
  @Test
  void reconstructLeaderExceptionDuplicatedKeyWithAPipeInTheKeys() {
    final String body = "{\"error\":\"Found duplicate key in index\","
        + "\"exception\":\"com.arcadedb.exception.DuplicatedKeyException\","
        + "\"exceptionArgs\":\"Account_name|[a|b]|#7:1\"}";

    final RuntimeException result = RaftReplicatedDatabase.reconstructLeaderException(409, body);

    assertThat(result).isInstanceOf(DuplicatedKeyException.class);
    final DuplicatedKeyException dup = (DuplicatedKeyException) result;
    assertThat(dup.getIndexName()).isEqualTo("Account_name");
    assertThat(dup.getKeys()).isEqualTo("[a|b]");
    assertThat(dup.getCurrentIndexedRID()).isEqualTo(new RID(7, 1L));
  }

  /**
   * Regression test for issue #10108: the configuration entry {@code RaftReplicatedDatabase} submits carried the setting its
   * statement named but the value the in-memory configuration held when the entry was built. Two statements on the SAME setting
   * on one node therefore still shipped a value the first one did not set: A stored 5, B stored 7, A reached its save and
   * submitted 7, B's own submit was then refused and its caller told that nothing had changed anywhere in the cluster.
   * <p>
   * The wrapper is real and wraps a real database; only the broker is a recording one, so what the wrapper submits is read off the
   * call it makes.

   */
  @Nested
  class ReplicatedSettingValue {
    private static final String KEY = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();

    @TempDir
    Path tempDir;

    private LocalDatabase          local;
    private RaftReplicatedDatabase database;
    private final List<String>     submitted = new ArrayList<>();

    @BeforeEach
    void setUp() {
      local = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
      final RaftTransactionBroker broker = new RaftTransactionBroker(null, Quorum.MAJORITY, 5_000) {
        @Override
        public long replicateDatabaseConfiguration(final String dbName, final String changesJson) {
          submitted.add(changesJson);
          return submitted.size();
        }
      };
      final FakeRaftHAServer raft = FakeRaftHAServer.detached();
      raft.leader(true);
      raft.transactionBroker(broker);
      database = new RaftReplicatedDatabase(null, local, raft);
    }

    @AfterEach
    void tearDown() {
      if (local != null && local.isOpen())
        local.drop();
    }

    private LocalDatabase.SettingChange set(final Object value) {
      return local.changeSetting(KEY, () -> local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, value));
    }

    private int submittedBuckets(final int entry) {
      return new JSONObject(submitted.get(entry)).getJSONObject("configuration").getInt("typeDefaultBuckets");
    }

    // Regression for #10108: ReplicatedSettingValue
    @Test
    void anEntryCarriesTheValueItsStatementSetWhenAnotherStoredTheSameSettingMeanwhile() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      final LocalDatabase.SettingChange b = set(7);

      database.saveConfiguration(a);

      assertThat(submitted).hasSize(1);
      assertThat(submittedBuckets(0)).as("A's entry carries A's value, not the 7 that B stored before A reached its save").isEqualTo(5);

      // B's own submit is a second entry carrying B's: it was never part of A's
      database.saveConfiguration(b);
      assertThat(submitted).hasSize(2);
      assertThat(submittedBuckets(1)).isEqualTo(7);
    }

    // Regression for #10108: ReplicatedSettingValue
    @Test
    void aRefusedSubmitOfTheSecondStatementLeavesTheFirstOnesValueTheOnlyOneShipped() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      final LocalDatabase.SettingChange b = set(7);

      database.saveConfiguration(a);
      // B's submit is refused (the capability interlock, a quorum failure, a broker refusal): it never reaches the broker, and
      // the undo of its change leaves what A's entry applies
      local.undoSettingChange(b);

      assertThat(submitted).as("nothing of B's reached the log").hasSize(1);
      assertThat(submittedBuckets(0)).isEqualTo(5);
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("this node holds what the cluster holds once B is undone before A's entry is applied here: A's value, which B replaced")
          .isEqualTo(5);
    }

    // Regression for #10108: ReplicatedSettingValue
    @Test
    void aRefusedSubmitUndoneAfterTheFirstEntryWasAppliedHereLeavesThatEntrysValue() throws IOException {
      final LocalDatabase.SettingChange a = set(5);
      final LocalDatabase.SettingChange b = set(7);

      database.saveConfiguration(a);
      // The apply of A's entry on this node: what every node, the originator included, does with a committed entry
      local.applyConfigurationChanges(submitted.getFirst());
      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(5);

      local.undoSettingChange(b);

      assertThat(local.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
          .as("another writer changed the setting since B stored it, so its value stands").isEqualTo(5);
      assertThat(submitted).hasSize(1);
    }

    // Regression for #10108: ReplicatedSettingValue
    @Test
    void theKeyedSaveWithNoChangeStillReadsTheValueMemoryHolds() throws IOException {
      local.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 9);

      database.saveConfiguration(KEY);

      assertThat(submittedBuckets(0)).isEqualTo(9);
    }
  }

  /**
   * Issue #8021: a mock handed to {@code RaftReplicatedDatabase} after the JIT compiled its delegation against the real
   * {@code LocalDatabase} (see {@link SubclassMocks} for the mechanism). Warms the delegate up on a real database, then
   * reads a mock's name back through it. It can only fail on a JIT that keeps stale code (Graal), and only while no
   * inline mock has retransformed {@code LocalDatabase} earlier in the fork; it cannot fail falsely.
   */
  @Nested
  class MockSeenThroughJitCompiledDelegate {
    private static final String MOCKED_NAME = "issue8021-mocked";

    @TempDir
    Path tempDir;

    // Regression for #8021: MockSeenThroughJitCompiledDelegate
    @Test
    void aSubclassMockIsAnsweredThroughADelegateTheJitCompiledAgainstTheRealClass() throws InterruptedException {
      try (final HttpClient httpClient = HttpClient.newHttpClient();
          final DatabaseFactory factory = new DatabaseFactory(tempDir.resolve("db").toString())) {
        final LocalDatabase real = (LocalDatabase) factory.create();
        try {
          final RaftReplicatedDatabase warm = new RaftReplicatedDatabase(null, real, null, httpClient);
          long chars = 0;
          // Enough calls for every tier to compile the delegate, with pauses that let the background compiler finish.
          for (int round = 0; round < 10; round++) {
            for (int i = 0; i < 100_000; i++)
              chars += nameThrough(warm).length();
            Thread.sleep(20);
          }
          // Consuming the result keeps the warm-up loop from being eliminated as dead code.
          assertThat(chars).isPositive();
        } finally {
          real.drop();
        }

        final LocalDatabase proxied = SubclassMocks.mock(LocalDatabase.class);
        when(proxied.getName()).thenReturn(MOCKED_NAME);
        final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, null, httpClient);

        assertThat(proxied.getName()).isEqualTo(MOCKED_NAME);
        assertThat(nameThrough(database)).as("the compiled delegate must reach the stub, not the mock's null field").isEqualTo(MOCKED_NAME);
      }
    }

    /** The call site the JIT compiles: the same delegation the commit path makes when it builds a LocalCommit. */
    private static String nameThrough(final RaftReplicatedDatabase database) {
      return database.getName();
    }
  }

  /**
   * Regression test for issue #8356: {@code RaftHAServer.stop()} nulls its {@code transactionBroker} field while
   * {@code raftHAServer} itself - {@code RaftReplicatedDatabase}'s own, {@code final} reference to it - stays
   * non-null, so {@code requireRaftServer()} alone cannot detect the window. A write that reaches
   * {@code replicateAndConclude} after the broker has been cleared but before the HTTP server stops accepting
   * requests used to dereference the null broker directly: {@code catch (ArcadeDBException e)} does not match a
   * {@code NullPointerException}, so it fell through to the generic {@code catch (Exception e)} and came out the
   * other side as a plain {@code TransactionException} - not retryable, so {@code AbstractServerHttpHandler}
   * reports it as a 500 instead of the clean, retryable 503 a {@code NeedRetryException} gets. Either shape is
   * wrong for a write that only failed because the server is shutting down; this test checks the caller now sees
   * a retryable failure, not either non-retryable one.
   * <p>
   * Mirrors the construction and commit-handshake style of {@code LocalCommitHandshakeTest}, with the
   * broker cleared instead of stubbed.
   */
  @Nested
  class NullTransactionBrokerAfterHaStop {
    private static final String DB_NAME   = "issue8356";
    private static final long   WAL_TX_ID = 4242L;

    private final String dbPath = "issue8356-null-transaction-broker-" + UUID.randomUUID();

    private LocalDatabase          proxied;
    private FakeRaftHAServer           raftServer;
    private TransactionContext     tx;
    private ArcadeStateMachine     stateMachine;
    private RaftReplicatedDatabase database;
    private RaftReplicatedDatabase.ReplicationPayload payload;

    @BeforeEach
    void setUp() {
      proxied = mock(LocalDatabase.class);
      when(proxied.getDatabasePath()).thenReturn(dbPath);
      when(proxied.getName()).thenReturn(DB_NAME);
      when(proxied.getTransactionManager()).thenReturn(mock(TransactionManager.class));
      when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
      when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

      // The race this issue is about: RaftHAServer.stop() has already cleared the broker, but this
      // RaftReplicatedDatabase's own (final) reference to the RaftHAServer instance is still non-null, so
      // requireRaftServer() alone would not catch the window.
      raftServer = FakeRaftHAServer.detached();
      raftServer.leader(true);
      raftServer.transactionBroker(null);
      stateMachine = new ArcadeStateMachine();
      raftServer.stateMachine(stateMachine);

      tx = mock(TransactionContext.class);
      DatabaseContext.INSTANCE.init(proxied, tx);
      database = new RaftReplicatedDatabase(null, proxied, raftServer);
      payload = new RaftReplicatedDatabase.ReplicationPayload(tx, null, walTransactionBytes(WAL_TX_ID), Map.of());
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.removeContext(dbPath);
    }

    // Regression for #8356: NullTransactionBrokerAfterHaStop
    @Test
    void aNullTransactionBrokerIsReportedAsRetryableInsteadOfThrowingNPE() {
      assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
          .isInstanceOf(NeedRetryException.class)
          .isNotInstanceOf(NullPointerException.class);

      // A caught, retryable failure - not a half-applied local commit left dangling.
      verify(proxied).rollback();
      verify(tx, never()).completeCommit();
      assertThat(stateMachine.pendingLocalCommits()).as("the withdrawal removed the registration").isZero();
    }

    /** The wire layout {@code CommittedEntryDecoding.deserializeWalTransaction} reads: id, timestamp, no pages. */
    private static byte[] walTransactionBytes(final long txId) {
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
      buf.putLong(txId);
      buf.putLong(System.currentTimeMillis());
      buf.putInt(0);
      buf.putInt(0);
      return buf.array();
    }
  }
}

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
package com.arcadedb.server.grpc;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Regression test: a gRPC write issued to a follower must be forwarded to the leader. Before the
 * fix, ArcadeDbGrpcService.getDatabase() called DatabaseContext.init() but never propagated the
 * authenticated user, so RaftReplicatedDatabase.forwardCommandToLeaderViaRaft threw
 * "Cannot forward command to leader: no authenticated user in the current security context"
 * and every write hitting a follower was silently dropped on the client side.
 * <p>
 * The nested groups pin the rest of what a follower does with a gRPC call on a cluster that declares no gRPC ports:
 * read-your-writes after a leader commit, a client transaction opened on a follower, and the graph batch load it
 * refuses outright.
 */
class GrpcFollowerForwardingIT extends BaseRaftHATest {

  // One gRPC port per server, drawn free for every test instance rather than fixed (issue #7496), from the same
  // ledger as the Raft ports so the two can never coincide (issue #8203).
  private final int[] grpcPorts = allocateFixturePorts(3);

  private static final String VERTEX_TYPE = "GrpcFollowerWrite";

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");

    final String existingPlugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String pluginEntry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (existingPlugins == null || existingPlugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, pluginEntry);
    else if (!existingPlugins.contains(pluginEntry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, existingPlugins + "," + pluginEntry);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  @Test
  void executeCommandThroughFollowerIsForwardedToLeader() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int followerIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        followerIndex = i;
        break;
      }
    assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

    // Schema goes through the leader to avoid mixing schema replication with the forwarding under test.
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });
    waitForAllServers();

    // Open the gRPC channel against the follower deliberately.
    channel = ManagedChannelBuilder
        .forAddress("localhost", grpcPorts[followerIndex])
        .usePlaintext()
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();

    // Write via SQL command - the follower must forward to leader, which requires an authenticated
    // user on the DatabaseContext. Before the fix this surfaced a SecurityException to the caller.
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials)
        .setLanguage("sql")
        .setCommand("CREATE VERTEX " + VERTEX_TYPE + " SET name = 'forwarded'")
        .build();

    final ExecuteCommandResponse response = stub.executeCommand(request);
    assertThat(response).as("Follower-forwarded executeCommand must return a response").isNotNull();

    waitForAllServers();

    for (int i = 0; i < getServerCount(); i++) {
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      assertThat(serverDb.countType(VERTEX_TYPE, true))
          .as("Server %d (leader=%d, write went through follower=%d) must see the forwarded vertex",
              i, leaderIndex, followerIndex)
          .isEqualTo(1);
    }
  }

  // Regression: #11948
  /**
   * Read-your-writes on a follower for a gRPC client (issue #11948). A gRPC commit answered no commit index and a gRPC
   * query took no read-after index, so a client that writes on the leader and reads on a follower read its own commit
   * back as missing in about one round in five. The commit now answers {@code x-arcadedb-commit-index}, and a read that
   * sends it back as {@code x-arcadedb-read-after} waits until the follower has applied it, as HTTP does.
   */
  @Nested
  @Tag("slow")
  class ReadYourWrites {
    private static final String TYPE   = "Issue11948";
    private static final int    ROUNDS = 100;

    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void closeChannels() throws InterruptedException {
      for (final ManagedChannel channel : channels) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
      channels.clear();
    }

    @Test
    void aFollowerReadAfterTheCommitBookmarkSeesTheCommit() {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
      final int followerIndex = leaderIndex == 0 ? 1 : 0;
      createType(leaderIndex);

      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader = stub(leaderIndex);
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(followerIndex);

      long previous = -1;
      for (int i = 0; i < ROUNDS; i++) {
        final String key = "k" + i;
        final AtomicReference<Metadata> headers = new AtomicReference<>();
        final AtomicReference<Metadata> trailers = new AtomicReference<>();
        commitOne(leader.withInterceptors(MetadataUtils.newCaptureMetadataInterceptor(headers, trailers)), key);

        final String bookmark = headers.get() != null ? headers.get().get(GrpcReadConsistencyInterceptor.COMMIT_INDEX_HEADER) : null;
        assertThat(bookmark).as("the commit of round %d answers its read-your-writes bookmark", i).isNotNull();
        final long index = Long.parseLong(bookmark);
        assertThat(index).as("the bookmark of round %d covers the commits before it", i).isGreaterThan(previous);
        previous = index;

        final Metadata readAfter = new Metadata();
        readAfter.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, bookmark);
        readAfter.put(GrpcReadConsistencyInterceptor.READ_CONSISTENCY_HEADER, "read_your_writes");
        final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub followerAfter =
            follower.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(readAfter));
        assertThat(count(followerAfter, key))
            .as("a follower read with the bookmark of round %d sees the row committed in it", i).isEqualTo(1);
        // The other two read paths: a streamed query resolves the database on its call's thread, a read inside a client
        // transaction runs on that transaction's own thread, where submitToActiveTransaction applies the consistency
        assertThat(countStreamed(followerAfter, key))
            .as("a streamed follower read with the bookmark of round %d sees the row committed in it", i).isEqualTo(1);
        assertThat(countInTransaction(followerAfter, key))
            .as("a follower read inside a transaction, with the bookmark of round %d, sees the row committed in it", i)
            .isEqualTo(1);
      }
    }

    @Test
    void aBookmarkThatIsNotANumberIsRefused() {
      final int leaderIndex = findLeaderIndex();
      createType(leaderIndex);

      final Metadata readAfter = new Metadata();
      readAfter.put(GrpcReadConsistencyInterceptor.READ_AFTER_HEADER, "not-a-number");
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(leaderIndex == 0 ? 1 : 0)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(readAfter));

      assertThatThrownBy(() -> count(follower, "any")).isInstanceOfSatisfying(StatusRuntimeException.class,
          e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void theConfiguredConsistencyAppliesToAFollowerReadWithoutHeaders() {
      final int leaderIndex = findLeaderIndex();
      final int followerIndex = leaderIndex == 0 ? 1 : 0;
      createType(leaderIndex);

      // arcadedb.ha.readConsistency is what an HTTP read without headers gets; a gRPC read ignored it
      final ContextConfiguration followerConfig = getServerDatabase(followerIndex, getDatabaseName()).getConfiguration();
      final String before = followerConfig.getValueAsString(GlobalConfiguration.HA_READ_CONSISTENCY);
      followerConfig.setValue(GlobalConfiguration.HA_READ_CONSISTENCY, "linearizable");
      try {
        final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader = stub(leaderIndex);
        final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub follower = stub(followerIndex);
        for (int i = 0; i < ROUNDS; i++) {
          commitOne(leader, "d" + i);
          assertThat(count(follower, "d" + i))
              .as("a linearizable follower read, the configured default, sees the row committed in round %d", i).isEqualTo(1);
        }
      } finally {
        followerConfig.setValue(GlobalConfiguration.HA_READ_CONSISTENCY, before);
      }
    }

    private void createType(final int leaderIndex) {
      final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
      leaderDb.transaction(() -> {
        if (!leaderDb.getSchema().existsType(TYPE))
          leaderDb.getSchema().createDocumentType(TYPE);
      });
      waitForAllServers();
    }

    private void commitOne(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub leader, final String key) {
      final String txId = leader.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
      leader.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("INSERT INTO " + TYPE + " SET k = '" + key + "'").setTransaction(tx).build());
      assertThat(leader.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build())
          .getCommitted()).isTrue();
    }

    private int count(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
      final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'").build());
      int rows = 0;
      for (final QueryResult result : response.getResultsList())
        rows += result.getRecordsCount();
      return rows;
    }

    private int countStreamed(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
      final Iterator<QueryResult> results = stub.streamQuery(StreamQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'").build());
      int rows = 0;
      while (results.hasNext())
        rows += results.next().getRecordsCount();
      return rows;
    }

    private int countInTransaction(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String key) {
      final String txId = stub.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
      try {
        final ExecuteQueryResponse response = stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
            .setCredentials(credentials()).setLanguage("sql").setQuery("SELECT FROM " + TYPE + " WHERE k = '" + key + "'")
            .setTransaction(tx).build());
        int rows = 0;
        for (final QueryResult result : response.getResultsList())
          rows += result.getRecordsCount();
        return rows;
      } finally {
        stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build());
      }
    }

    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub(final int serverIndex) {
      final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
      channels.add(channel);
      return ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              headers.put(USER_HEADER, "root");
              headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
              super.start(responseListener, headers);
            }
          };
        }
      }));
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }
  }

  // Regression: #12365
  /**
   * Issue #12365 over gRPC: a client was told its commit failed with a {@code ConcurrentModificationException} while the write it
   * carried was on every node. Up to 26.10.1 a write sent to a FOLLOWER inside a transaction the client opened with
   * {@code BeginTransaction} was forwarded to the leader and committed there on its own, at once, so neither the refusal of the
   * later {@code CommitTransaction} nor a {@code RollbackTransaction} could take it back. Since #10040 it runs in the client's
   * transaction on the follower: these pin that for gRPC, which had no test of it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FollowerTransaction {
    private static final String LEDGER  = "Issue12365Ledger";
    private static final String ACCOUNT = "Issue12365Account";

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @AfterEach
    void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }

    /** The deterministic shape: a rolled back transaction of a follower leaves nothing on any node. */
    @Test
    @Timeout(180)
    void aRollbackOnAFollowerUndoesTheWritesOfTheTransaction() {
      final int leader = requireLeaderIndex();
      createTypes(leader);
      connect(firstFollowerOf(leader));

      final String txId = begin();
      execute(txId, "INSERT INTO " + LEDGER + " SET key = 'rolled-back'");
      assertThat(count(leader, LEDGER, "rolled-back")).as("nothing reaches the leader before the transaction ends").isZero();
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(txId)).build());

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(count(i, LEDGER, "rolled-back")).as("server %d holds no row of the rolled back transaction", i).isZero();

      final String committed = begin();
      execute(committed, "INSERT INTO " + LEDGER + " SET key = 'committed'");
      final CommitTransactionResponse ok = stub.commitTransaction(
          CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(committed)).build());
      assertThat(ok.getCommitted()).isTrue();

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(count(i, LEDGER, "committed")).as("server %d holds the committed row", i).isEqualTo(1L);
    }

    /**
     * The reporter's shape: the transaction writes a ledger row and updates a record that another writer updates before the
     * commit. The commit is refused as a conflict, and then none of the transaction is anywhere, its ledger row included.
     */
    @Test
    @Timeout(180)
    void aCommitRefusedAsAConflictLeavesNoWriteOfTheTransactionOnAnyNode() {
      final int leader = requireLeaderIndex();
      createTypes(leader);
      final Database leaderDb = getServerDatabase(leader, getDatabaseName());
      leaderDb.transaction(() -> leaderDb.command("sql", "INSERT INTO " + ACCOUNT + " SET id = 1, balance = 100"));
      waitForAllServers();
      connect(firstFollowerOf(leader));

      final String txId = begin();
      execute(txId, "INSERT INTO " + LEDGER + " SET key = 'conflicting'");
      execute(txId, "UPDATE " + ACCOUNT + " SET balance = balance + 1 WHERE id = 1");

      // a concurrent writer moves the same record and commits first
      leaderDb.transaction(() -> leaderDb.command("sql", "UPDATE " + ACCOUNT + " SET balance = 500 WHERE id = 1"));
      waitForAllServers();

      final StatusRuntimeException refused = catchThrowableOfType(StatusRuntimeException.class, () -> stub.commitTransaction(
          CommitTransactionRequest.newBuilder().setCredentials(credentials()).setTransaction(txRef(txId)).build()));
      assertThat(refused).as("the commit is refused").isNotNull();
      assertThat(refused.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++) {
        assertThat(count(i, LEDGER, "conflicting")).as("server %d holds no row of the refused transaction", i).isZero();
        try (final ResultSet rs = getServerDatabase(i, getDatabaseName()).query("sql", "SELECT balance FROM " + ACCOUNT + " WHERE id = 1")) {
          assertThat(rs.next().<Number>getProperty("balance").intValue()).as("server %d keeps the other writer's update", i).isEqualTo(500);
        }
      }
    }

    private void createTypes(final int leader) {
      final Database leaderDb = getServerDatabase(leader, getDatabaseName());
      leaderDb.command("sql", "CREATE DOCUMENT TYPE " + LEDGER + " IF NOT EXISTS");
      leaderDb.command("sql", "CREATE DOCUMENT TYPE " + ACCOUNT + " IF NOT EXISTS");
      waitForAllServers();
    }

    private void connect(final int serverIndex) {
      channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
      final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
      stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
    }

    private String begin() {
      return stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build())
          .getTransactionId();
    }

    private void execute(final String txId, final String sql) {
      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setLanguage("sql")
          .setCommand(sql).setTransaction(txRef(txId)).build());
    }

    private TransactionContext txRef(final String txId) {
      return TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private long count(final int serverIndex, final String type, final String key) {
      try (final ResultSet rs = getServerDatabase(serverIndex, getDatabaseName()).query("sql",
          "SELECT count(@rid) AS c FROM " + type + " WHERE key = ?", key)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  // Regression: #6070
  /**
   * A graph batch load must not run on a follower (issue #6070). The bulk path mutates state that only the leader
   * can serialize - the schema dictionary above all - so running it on a follower races the local state-machine
   * apply in {@code Dictionary.getIdByName}, which is the corruption issue #4122 was about and the reason the
   * HTTP endpoint relays the payload to the leader instead of loading it locally.
   * <p>
   * {@code GraphBatchLoad} had no such guard: it took the load wherever the client happened to connect. It does not
   * relay the way HTTP does, because relaying a bulk load through a follower would double the traffic of the
   * transport chosen to avoid exactly that, so it refuses - before writing anything, and naming the leader, so the
   * caller can redirect rather than reconcile a load that got half-way. That the named address is one the caller can
   * actually dial is issue #6091, covered by {@link GrpcLeaderRoutingIT.GraphBatchLoadLeaderRedirect}.
   * <p>
   * What makes this worth a cluster test rather than a unit test is the second assertion: the same load, over the
   * same RPC, must still succeed against the leader. A guard that refused everywhere would pass a test that only
   * checked the follower.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBatchLoadLeaderGuard {
    private static final String VERTEX_TYPE = "Issue6070LeaderGuardNode";

    private ManagedChannel channel;

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }

    @Test
    void aLoadOnAFollowerIsRefusedAndTheSameLoadOnTheLeaderSucceeds() throws Exception {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

      int followerIndex = -1;
      for (int i = 0; i < getServerCount(); i++)
        if (i != leaderIndex) {
          followerIndex = i;
          break;
        }
      assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

      // Schema through the leader, so the guard under test is the only thing the load depends on.
      final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
      leaderDb.transaction(() -> {
        if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
          leaderDb.getSchema().createVertexType(VERTEX_TYPE);
      });
      waitForAllServers();

      // The follower refuses, and says where to go instead.
      final Throwable refusal = loadOneVertex(followerIndex, "onFollower");
      assertThat(refusal).as("a load aimed at a follower must be refused, not silently taken")
          .isInstanceOf(StatusRuntimeException.class);

      final StatusRuntimeException failure = (StatusRuntimeException) refusal;
      assertThat(failure.getStatus().getCode()).as("the caller has to be able to tell this from an engine fault")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
      assertThat(failure.getStatus().getDescription()).contains("not the cluster leader");

      // Refused before writing anything: nothing to reconcile anywhere in the cluster.
      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
            .as("server %d must hold nothing from the refused load", i)
            .isZero();

      // The very same load against the leader goes through, and replicates.
      assertThat(loadOneVertex(leaderIndex, "onLeader")).as("the leader must still accept the load").isNull();

      waitForAllServers();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
            .as("server %d (leader=%d) must see the vertex loaded on the leader", i, leaderIndex)
            .isEqualTo(1);
    }

    /**
     * The refusal names the leader, which is a fact about the cluster's layout. It must therefore come after the
     * caller has been resolved against the database it asked for, the way every other RPC on this service starts.
     * A follower asked to load into a database that cannot be reached has to answer about that database, not
     * volunteer where the leader lives - and the difference is only observable on a follower, because that is the
     * only place the leadership branch is reached at all.
     */
    @Test
    void aFollowerResolvesTheDatabaseBeforeNamingTheLeader() throws Exception {
      final int leaderIndex = findLeaderIndex();
      assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

      int followerIndex = -1;
      for (int i = 0; i < getServerCount(); i++)
        if (i != leaderIndex) {
          followerIndex = i;
          break;
        }
      assertThat(followerIndex).as("At least one follower must exist").isGreaterThanOrEqualTo(0);

      final Throwable refusal = loadOneVertexInto(followerIndex, "unauthorized", "Issue6070NoSuchDatabase");

      assertThat(refusal).as("a load naming a database that cannot be reached must be refused").isNotNull();
      assertThat(refusal.getMessage())
          .as("the refusal must be about the database, not about where the leader is")
          .doesNotContain("not the cluster leader")
          .doesNotContain("Reconnect to the leader");
    }

    /**
     * Runs a one-vertex load against the given server and returns the failure it ended with, or null if it
     * succeeded.
     */
    private Throwable loadOneVertex(final int serverIndex, final String tempId) throws Exception {
      return loadOneVertexInto(serverIndex, tempId, getDatabaseName());
    }

    private Throwable loadOneVertexInto(final int serverIndex, final String tempId, final String databaseName)
        throws Exception {
      channel = ManagedChannelBuilder.forAddress("localhost", grpcPorts[serverIndex]).usePlaintext().build();
      try {
        final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());

        final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);

        final StreamObserver<GraphBatchChunk> observer = ArcadeDbServiceGrpc.newStub(authenticated).graphBatchLoad(
            new StreamObserver<>() {
              @Override
              public void onNext(final GraphBatchResult result) {
              }

              @Override
              public void onError(final Throwable t) {
                errorRef.set(t);
                done.countDown();
              }

              @Override
              public void onCompleted() {
                done.countDown();
              }
            });

        observer.onNext(GraphBatchChunk.newBuilder()
            .setDatabase(databaseName)
            .setCredentials(DatabaseCredentials.newBuilder()
                .setUsername("root")
                .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
                .build())
            .addRecords(GraphBatchRecord.newBuilder()
                .setKind(GraphBatchRecord.Kind.VERTEX)
                .setTypeName(VERTEX_TYPE)
                .setTempId(tempId)
                .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build()))
            .build());
        observer.onCompleted();

        assertThat(done.await(30, TimeUnit.SECONDS)).as("the load must terminate one way or the other").isTrue();
        return errorRef.get();
      } finally {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
        channel = null;
      }
    }
  }
}

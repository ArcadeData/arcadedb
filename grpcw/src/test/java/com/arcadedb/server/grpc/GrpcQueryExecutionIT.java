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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.google.protobuf.Int32Value;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Query and command execution over gRPC: record metadata, protocol tagging, running statements and projections.
 * Each nested class is the regression of one issue and keeps that issue's own client fixture; they share the server,
 * started with the gRPC plugin and nothing else configured.
 */
public class GrpcQueryExecutionIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #6404
  /**
   * Regression test for issue #6404: {@code RemoteGrpcDatabase.lookupByRID()} threw {@code RecordNotFoundException}
   * for an existing, previously persisted vertex, even though the same RID was returned by a SQL query on the same
   * connection and was loadable through the HTTP client.
   * <p>
   * Root cause, at the wire level pinned here: {@link ArcadeDbGrpcService#convertToGrpcRecord} (and its
   * {@code convertResultToGrpcRecord} sibling used by queries/commands) never sent the {@code @cat} metadata property
   * that {@code JsonSerializer} always sends over HTTP for every Document/Vertex/Edge. The client's
   * {@code grpcRecordToDBRecord()} treats a missing {@code @cat} as "resolve it through the remote schema instead",
   * and {@code lookupByRID()} - unlike the query path, which has a property-only {@code Result} fallback - has no
   * fallback for a {@code Record} it cannot categorize: a schema resolution that is stale, lagging, or simply not
   * yet warm turns a real "found" answer into a spurious "not found" exception.
   * <p>
   * The fix sends {@code @cat} on the wire for every element, exactly as HTTP does, so the client never needs the
   * schema round-trip for this at all. Pinned directly against the RPC response rather than through the schema-cache
   * warm-up timing of the high-level client, which is not itself deterministic enough to fail reliably pre-fix.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LookupByRidCategory {
    private static final Metadata.Key<String> USER_HEADER =
        Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER =
        Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER =
        Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private static final String VERTEX_TYPE = "Issue6404Vertex";

    private ManagedChannel                                    channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
      authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    }

    @AfterEach
    void shutdownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    private class AuthClientInterceptor implements ClientInterceptor {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel next) {
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

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private ExecuteCommandResponse executeCommand(final String command) {
      return authenticatedStub.executeCommand(
          ExecuteCommandRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setCommand(command)
              .setReturnRows(true)
              .build());
    }

    @Test
    void lookupByRidResponseCarriesCatForAVertex() {
      executeCommand("CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
      executeCommand("CREATE PROPERTY `" + VERTEX_TYPE + "`.ldapId IF NOT EXISTS STRING");
      executeCommand("DELETE FROM `" + VERTEX_TYPE + "`");

      final ExecuteCommandResponse inserted = executeCommand("INSERT INTO `" + VERTEX_TYPE + "` SET ldapId = 'heimdall'");
      assertThat(inserted.getRecordsCount()).isEqualTo(1);
      final String rid = inserted.getRecords(0).getRid();
      assertThat(rid).startsWith("#");

      final LookupByRidResponse response = authenticatedStub.lookupByRid(
          LookupByRidRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setRid(rid)
              .build());

      assertThat(response.getFound()).as("the record must be found").isTrue();

      final GrpcRecord record = response.getRecord();
      assertThat(record.getRid()).isEqualTo(rid);
      assertThat(record.getType()).isEqualTo(VERTEX_TYPE);

      // The bug: this property used to be entirely absent, forcing the client to re-derive the category through the
      // remote schema - the only conversion path (lookupByRID) that has no fallback when that re-derivation fails.
      assertThat(record.getPropertiesMap())
          .as("the record must carry its own category rather than relying on the client's schema cache")
          .containsEntry("@cat", GrpcValue.newBuilder().setStringValue("v").build());
    }
  }

  // Regression: #8363
  /**
   * Issue #8363, the gRPC half: on an HA node, {@code RaftReplicatedDatabase} refuses a CLIENT request on a database
   * whose directory is being replaced from the leader's snapshot, and it tells a client from the engine by
   * {@link ProtocolContext}. {@code LookupByRid}, {@code CreateRecord} and the work of a client-managed transaction
   * never set that tag - query metrics were its only reader and none of them runs a query - so on the node being
   * replaced they read as engine work and were served from the copy the cluster was discarding.
   * <p>
   * Observed where the database sees the request: record listeners fire on the thread that does the read or the
   * write, so the tag they see is the tag the gate sees.
   */
  @Nested
  class RequestsTaggedAsClients {
    private final List<String> tagsSeenOnRead   = new CopyOnWriteArrayList<>();
    private final List<String> tagsSeenOnCreate = new CopyOnWriteArrayList<>();

    private final AfterRecordReadListener    readListener   = record -> {
      tagsSeenOnRead.add(ProtocolContext.get());
      return record;
    };
    private final BeforeRecordCreateListener createListener = record -> {
      tagsSeenOnCreate.add(ProtocolContext.get());
      return true;
    };

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void setUpClientAndListeners() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel,
          new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
      final Database database = getServerDatabase(0, getDatabaseName());
      database.getEvents().registerListener(readListener);
      database.getEvents().registerListener(createListener);
    }

    @AfterEach
    void tearDownClientAndListeners() throws InterruptedException {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.getEvents().unregisterListener(readListener);
      database.getEvents().unregisterListener(createListener);
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private GrpcRecord vertex(final String name) {
      return GrpcRecord.newBuilder().setType(VERTEX1_TYPE_NAME)
          .putProperties("name", GrpcValue.newBuilder().setStringValue(name).build()).build();
    }

    @Test
    void createRecordAndLookupByRidOutsideATransactionRunAsAClient() {
      final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setType(VERTEX1_TYPE_NAME).setRecord(vertex("auto")).build()).getRid();

      stub.lookupByRid(LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setRid(rid).build());

      assertThat(tagsSeenOnCreate).as("CreateRecord").isNotEmpty().containsOnly("grpc");
      assertThat(tagsSeenOnRead).as("LookupByRid").isNotEmpty().containsOnly("grpc");
    }

    @Test
    void theWorkOfAClientManagedTransactionRunsAsAClient() {
      final String txId = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId)
          .setDatabase(getDatabaseName()).build();

      final String rid = stub.createRecord(CreateRecordRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setType(VERTEX1_TYPE_NAME).setRecord(vertex("in-tx")).setTransaction(tx)
          .build()).getRid();
      stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials())
          .build());

      final String txId2 = stub.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx2 = TransactionContext.newBuilder().setTransactionId(txId2)
          .setDatabase(getDatabaseName()).build();
      stub.lookupByRid(LookupByRidRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setRid(rid).setTransaction(tx2).build());
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx2).setCredentials(credentials())
          .build());

      assertThat(tagsSeenOnCreate).as("CreateRecord inside a transaction").isNotEmpty().containsOnly("grpc");
      assertThat(tagsSeenOnRead).as("LookupByRid inside a transaction").isNotEmpty().containsOnly("grpc");
    }
  }

  // Regression: #9689
  /**
   * Issue #9689 over gRPC: an RPC that runs a statement is listed in the server's running statements, a terminate stops it
   * with {@code CANCELLED}, a client that gives up on the call (here: its deadline expires) stops the work on the server
   * too, and a statement stopped inside a client transaction ends the transaction, so its writes cannot be committed later.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RunningStatements {
    /** About 16 s on one core when left alone, all of it inside one aggregation. */
    private static final String LONG_CYPHER =
        "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

    private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              headers.put(USER_HEADER, "root");
              headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
              headers.put(DATABASE_HEADER, getDatabaseName());
              super.start(responseListener, headers);
            }
          };
        }
      });
      stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);
    }

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void aTerminatedRpcIsCancelled() throws Exception {
      final CompletableFuture<ExecuteQueryResponse> running = CompletableFuture.supplyAsync(() -> stub.executeQuery(longQuery(null)));

      final RunningQuery entry = awaitRunning();
      assertThat(entry.getProtocol()).isEqualTo("grpc");
      assertThat(entry.getUser()).isEqualTo("root");
      assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
      assertThat(entry.getLanguage()).isEqualTo("opencypher");
      // Issue #9962: listed with the peer of the call
      assertThat(entry.getClientAddress()).as("the peer of the call").isNotBlank().isNotEqualTo("null");

      entry.terminate("root");
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(e -> {
        assertThat(e.getCause()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
        // The trailer the Java client rebuilds the typed exception from
        assertThat(Status.trailersFromThrowable(e.getCause()).get(GrpcErrorMapper.EXCEPTION_CLASS_KEY))
            .isEqualTo(QueryTerminatedException.class.getName());
      });
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    }

    @Test
    void aClientThatGivesUpStopsTheWorkOnTheServer() throws Exception {
      // The client's deadline expires long before the statement would end: the server stops working on it as well
      assertThatThrownBy(() -> stub.withDeadlineAfter(1, TimeUnit.SECONDS).executeQuery(longQuery(null)))
          .isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));

      await().atMost(Duration.ofSeconds(30)).until(() -> findRunning() == null);
    }

    @Test
    void aStatementStoppedInAClientTransactionEndsTheTransaction() throws Exception {
      final String txId = stub.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("CREATE DOCUMENT TYPE Written9689 IF NOT EXISTS").build());
      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("INSERT INTO Written9689 SET x = 1").setTransaction(tx).build());

      final CompletableFuture<ExecuteQueryResponse> running = CompletableFuture.supplyAsync(() -> stub.executeQuery(longQuery(tx)));
      final RunningQuery entry = awaitRunning();
      entry.terminate("root");
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(
          e -> assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED));

      // The transaction went with the statement: a commit finds nothing to commit, and what it wrote is not there
      assertThat(stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build())
          .getCommitted()).isFalse();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
      assertThat(stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setQuery("SELECT count(*) AS c FROM Written9689").build()).getResults(0).getRecords(0).getPropertiesMap().get("c")
          .getInt64Value()).isZero();
    }

    @Test
    void aCommitQueuedBehindATerminatedStatementPublishesNothing() throws Exception {
      final String txId = stub.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("CREATE DOCUMENT TYPE Committed9689 IF NOT EXISTS").build());
      stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setLanguage("sql").setCommand("INSERT INTO Committed9689 SET x = 1").setTransaction(tx).build());

      final CompletableFuture<ExecuteQueryResponse> running = CompletableFuture.supplyAsync(() -> stub.executeQuery(longQuery(tx)));
      final RunningQuery entry = awaitRunning();

      // The commit claims the transaction while the statement still runs: its work waits behind the statement's
      final CompletableFuture<CommitTransactionResponse> commit = CompletableFuture.supplyAsync(
          () -> stub.commitTransaction(CommitTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build()));
      await().atMost(Duration.ofSeconds(30)).until(
          () -> getServer(0).getRunningQueries().getRunning().stream().anyMatch(q -> "CommitTransaction".equals(q.getText())));

      entry.terminate("root");
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(
          e -> assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED));
      // The commit is refused, and the write the transaction held before the statement is not published
      assertThatThrownBy(() -> commit.get(30, TimeUnit.SECONDS)).satisfies(
          e -> assertThat(((StatusRuntimeException) e.getCause()).getStatus().getCode()).isEqualTo(Status.Code.CANCELLED));
      assertThat(stub.executeQuery(ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
          .setQuery("SELECT count(*) AS c FROM Committed9689").build()).getResults(0).getRecords(0).getPropertiesMap().get("c")
          .getInt64Value()).isZero();
    }

    private RunningQuery awaitRunning() {
      final RunningQuery[] found = new RunningQuery[1];
      await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> (found[0] = findRunning()) != null);
      return found[0];
    }

    private RunningQuery findRunning() {
      for (final RunningQuery q : getServer(0).getRunningQueries().getRunning())
        if (LONG_CYPHER.equals(q.getText()))
          return q;
      return null;
    }

    private ExecuteQueryRequest longQuery(final TransactionContext tx) {
      final ExecuteQueryRequest.Builder builder = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(credentials()).setLanguage("opencypher").setQuery(LONG_CYPHER);
      if (tx != null)
        builder.setTransaction(tx);
      return builder.build();
    }

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }
  }

  // Regression: #11067
  /**
   * Issue #11067 (#11044, #11048): a value a projection reaches through a graph function, a list, a map or a nested result is
   * encoded like a top-level one.
   * <ul>
   * <li>#11044: {@code out()}, {@code in()}, {@code outE()}, {@code inE()} return a lazy {@code Iterable} that is not a
   * {@code Collection}, so the encoder sent it as its {@code toString()}: "com.arcadedb.graph.GraphEngine$10@63e44da5", a
   * class name that names no record and changes on every call, where HTTP answers the neighbour records;</li>
   * <li>#11048: the list, map, array and result arms recursed without the request's projection settings, so a record
   * inside them ({@code both()} returns a {@code List}) was always sent as a bare link, and {@code soft_limit_bytes} never
   * saw its bytes.</li>
   * </ul>
   * Each query is checked against the embedded engine's own answer: the RIDs the gRPC value names are the RIDs the
   * engine's value holds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TraversalProjection {
    private static final String VERTEX = "TV11067";
    private static final String EDGE   = "TE11067";

    private ManagedChannel                               channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;
    private String                                       ridA;
    private String                                       ridB;
    private String                                       ridEdge;

    @BeforeEach
    void graphOfTwoVerticesAndOneEdge() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.command("sql", "CREATE VERTEX TYPE " + VERTEX);
      database.command("sql", "CREATE EDGE TYPE " + EDGE);
      database.transaction(() -> {
        final MutableVertex a = database.newVertex(VERTEX).set("name", "a").save();
        final MutableVertex b = database.newVertex(VERTEX).set("name", "b").save();
        ridA = a.getIdentity().toString();
        ridB = b.getIdentity().toString();
        ridEdge = a.newEdge(EDGE, b).getIdentity().toString();
      });

      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      final Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
      headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
      headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
      stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void everyTraversalFunctionAnswersTheRecordsItReaches() {
      assertThat(rids(project("out('" + EDGE + "')", "a", null))).containsExactly(ridB);
      assertThat(rids(project("in('" + EDGE + "')", "b", null))).containsExactly(ridA);
      assertThat(rids(project("both('" + EDGE + "')", "a", null))).containsExactly(ridB);
      assertThat(rids(project("out()", "a", null))).containsExactly(ridB);
      assertThat(rids(project("outE()", "a", null))).containsExactly(ridEdge);
      assertThat(rids(project("inE()", "b", null))).containsExactly(ridEdge);
      assertThat(rids(project("bothE()", "a", null))).containsExactly(ridEdge);
    }

    @Test
    void aTraversalReachesTheRecordsNotItsRids() {
      // both() returns a List, which the encoder did list: each element went out as a bare link where HTTP sends the record
      final GrpcValue value = project("both('" + EDGE + "')", "a", null);
      assertThat(value.getListValue().getValuesCount()).isEqualTo(1);
      final GrpcValue neighbour = value.getListValue().getValues(0);
      assertThat(neighbour.getKindCase()).isEqualTo(GrpcValue.KindCase.BYTES_VALUE);
      assertThat(neighbour.getLogicalType()).isEqualTo("json");
      assertThat(json(neighbour).getString("name")).isEqualTo("b");

      // The same record under the MAP encoding: the request's encoding reaches the element
      final GrpcValue asMap = project("out('" + EDGE + "')", "a", projection(ProjectionEncoding.PROJECTION_AS_MAP, 0));
      final GrpcMap entries = asMap.getListValue().getValues(0).getMapValue();
      assertThat(entries.getEntriesMap().get("name").getStringValue()).isEqualTo("b");
      assertThat(entries.getEntriesMap().get("@rid").getLinkValue().getRid()).isEqualTo(ridB);
    }

    @Test
    void anEdgelessVertexAnswersAnEmptyList() {
      // HTTP answers []: a class name for "no neighbours" could not be told from one for "one neighbour"
      final GrpcValue value = project("out('" + EDGE + "')", "b", null);
      assertThat(value.getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      assertThat(value.getListValue().getValuesCount()).isZero();
    }

    @Test
    void aTraversalNestedInAMapOrAListIsEncodedToo() {
      final GrpcValue inMap = project("{ 'k': out('" + EDGE + "') }", "a", null);
      assertThat(rids(inMap.getMapValue().getEntriesMap().get("k"))).containsExactly(ridB);

      final GrpcValue inList = project("[ out('" + EDGE + "') ]", "a", null);
      assertThat(inList.getListValue().getValuesCount()).isEqualTo(1);
      assertThat(rids(inList.getListValue().getValues(0))).containsExactly(ridB);
    }

    @Test
    void aNestedResultRowIsTheRecordItWraps() {
      final GrpcValue value = project("(SELECT FROM " + VERTEX + " WHERE name = 'b')", "a", null);
      assertThat(rids(value)).containsExactly(ridB);
    }

    @Test
    void aNestedRowWithColumnsOfItsOwnKeepsThem() {
      // SELECT *, name AS alias keeps the record AND publishes a column of its own: collapsing the row to the record lost it
      final GrpcValue value = project("(SELECT *, name AS alias FROM " + VERTEX + " WHERE name = 'b')", "a", null);
      assertThat(value.getListValue().getValuesCount()).isEqualTo(1);
      final GrpcMap row = value.getListValue().getValues(0).getMapValue();
      assertThat(row.getEntriesMap().get("alias").getStringValue()).isEqualTo("b");
      assertThat(row.getEntriesMap().get("@rid").getLinkValue().getRid()).isEqualTo(ridB);
      assertThat(row.getEntriesMap().get("@type").getStringValue()).isEqualTo(VERTEX);
    }

    @Test
    void theSoftLimitIsOneBudgetForEveryRecordOfTheList() {
      final Database database = getServerDatabase(0, getDatabaseName());
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex(VERTEX).set("name", "hub").save();
        hub.newEdge(EDGE, database.newVertex(VERTEX).set("name", "x").save());
        hub.newEdge(EDGE, database.newVertex(VERTEX).set("name", "y").save());
      });

      final GrpcValue unbounded = project("out('" + EDGE + "')", "hub", projection(ProjectionEncoding.PROJECTION_AS_JSON, 0));
      assertThat(unbounded.getListValue().getValuesCount()).isEqualTo(2);
      final int first = unbounded.getListValue().getValues(0).getBytesValue().size();
      final int second = unbounded.getListValue().getValues(1).getBytesValue().size();

      // Room for the first record, not for both: each one fits the limit alone, the two do not fit it together
      final GrpcValue bounded = project("out('" + EDGE + "')", "hub",
          projection(ProjectionEncoding.PROJECTION_AS_JSON, first + second - 1));
      assertThat(bounded.getListValue().getValues(0).getKindCase()).isEqualTo(GrpcValue.KindCase.BYTES_VALUE);
      assertThat(bounded.getListValue().getValues(1).getKindCase()).as("over the budget: its link")
          .isEqualTo(GrpcValue.KindCase.LINK_VALUE);
    }

    @Test
    void theSoftLimitBoundsARecordInsideAList() {
      // A record the budget cannot hold goes out as its link (the JSON encoding's fallback): the budget sees a record nested
      // in a list, which it never did, since the list arm dropped the config the budget lives on
      final GrpcValue bounded = project("both('" + EDGE + "')", "a", projection(ProjectionEncoding.PROJECTION_AS_JSON, 1));
      final GrpcValue neighbour = bounded.getListValue().getValues(0);
      assertThat(neighbour.getKindCase()).isEqualTo(GrpcValue.KindCase.LINK_VALUE);
      assertThat(neighbour.getLinkValue().getRid()).isEqualTo(ridB);
    }

    @Test
    void noValueIsSentAsTheToStringOfAnEngineClass() {
      for (final String expression : List.of("out()", "in()", "both()", "outE()", "inE()", "bothE()", "out('" + EDGE + "')",
          "in('" + EDGE + "')", "outE('" + EDGE + "')", "inE('" + EDGE + "')"))
        for (final String name : List.of("a", "b"))
          assertThat(project(expression, name, null).getKindCase()).as(expression + " of " + name)
              .isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    }

    private GrpcValue project(final String expression, final String name, final ProjectionSettings settings) {
      final ExecuteQueryRequest.Builder request = ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName())
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .setQuery("SELECT " + expression + " AS v FROM " + VERTEX + " WHERE name = '" + name + "'");
      if (settings != null)
        request.setProjectionSettings(settings);
      final ExecuteQueryResponse response = stub.executeQuery(request.build());
      assertThat(response.getResults(0).getRecordsCount()).isEqualTo(1);
      return response.getResults(0).getRecords(0).getPropertiesMap().get("v");
    }

    private static ProjectionSettings projection(final ProjectionEncoding encoding, final int softLimitBytes) {
      final ProjectionSettings.Builder settings = ProjectionSettings.newBuilder().setIncludeProjections(true)
          .setProjectionEncoding(encoding);
      if (softLimitBytes > 0)
        settings.setSoftLimitBytes(Int32Value.of(softLimitBytes));
      return settings.build();
    }

    /** The RIDs a list of records names, whichever of the three encodings carries each record. */
    private static List<String> rids(final GrpcValue list) {
      assertThat(list.getKindCase()).as("a list, not %s", list).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      final List<String> rids = new ArrayList<>();
      for (final GrpcValue element : list.getListValue().getValuesList())
        rids.add(switch (element.getKindCase()) {
          case BYTES_VALUE -> json(element).getString("@rid");
          case MAP_VALUE -> element.getMapValue().getEntriesMap().get("@rid").getLinkValue().getRid();
          case LINK_VALUE -> element.getLinkValue().getRid();
          default -> throw new AssertionError("not a record: " + element);
        });
      return rids;
    }

    private static JSONObject json(final GrpcValue value) {
      return new JSONObject(value.getBytesValue().toString(StandardCharsets.UTF_8));
    }
  }
}

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
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
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
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code GraphBatchLoad} RPC: the single-batch guard it shares with the engine, and the hardening of its options,
 * id mapping, partial-commit trailers and endpoint checks.
 */
public class GrpcGraphBatchLoadIT extends BaseGrpcServerTest {

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;
  private ArcadeDbServiceGrpc.ArcadeDbServiceStub         asyncAuthenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    asyncAuthenticatedStub = ArcadeDbServiceGrpc.newStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // Regression: #5666
  /**
   * Regression tests for the gRPC half of the single-batch guard. A database grants one
   * {@link GraphBatch} at a time, because two overlapping batches silently lose edges. The streaming
   * endpoint therefore has to do two things the engine cannot do for it: report the refusal as a
   * client-side failure, and give the slot back when it drops a batch it never closes.
   * <p>
   * The second one is what matters in production. {@code graphBatchLoad} deliberately skips
   * {@code close()} on the error path so the gRPC thread does not block on the async completion of a
   * batch nobody is waiting for, and a slot left behind there would stop that database from batching
   * for the rest of the server's life. Both assertions run against one long-lived database, which is
   * the only way a leaked slot is visible: a fresh database per test hides it.
   * <p>
   * A load that fails mid-stream is dropped without close(), so the endpoint has to hand the slot
   * back by itself. The load that follows is on the same database and would be refused otherwise.
   */
  @Test
  void aFailedLoadDoesNotKeepTheSlot() throws Exception {
    createBatchTypes();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch failed = new CountDownLatch(1);
    final StreamObserver<GraphBatchChunk> failing = asyncAuthenticatedStub.graphBatchLoad(
        new StreamObserver<>() {
          @Override
          public void onNext(final GraphBatchResult result) {
          }

          @Override
          public void onError(final Throwable t) {
            failure.set(t);
            failed.countDown();
          }

          @Override
          public void onCompleted() {
            failed.countDown();
          }
        });

    // An edge pointing at a temporary id no vertex declared: fails inside onNext, which is the path
    // that drops the batch instead of closing it.
    failing.onNext(GraphBatchChunk.newBuilder()
        .setDatabase(getDatabaseName())
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId("g1")
            .putProperties("name", stringValue("first")))
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.EDGE)
            .setTypeName("GuardLink")
            .setFromRef("g1")
            .setToRef("gNeverDeclared"))
        .build());
    failing.onCompleted();

    assertThat(failed.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
    assertThat(failure.get().getMessage()).contains("Unknown temporary ID");

    // The batch relaxed read-your-writes for the load and was dropped before close() could put it
    // back, so the endpoint has to. Left relaxed, every later reader on this database loses it.
    assertThat(getServerDatabase(0, getDatabaseName()).isReadYourWrites())
        .as("a dropped batch must not leave read-your-writes off")
        .isTrue();

    // Same database, right after: this is the load a leaked slot would refuse forever.
    final GraphBatchResult result = loadTwoVertices("g2", "g3");
    assertThat(result.getVerticesCreated()).isEqualTo(2);
  }

  // Regression: #5666
  /**
   * An overlapping load is a client mistake, not an engine fault, so it has to come back as
   * FAILED_PRECONDITION rather than INTERNAL. The competing batch is taken directly on the server
   * database so the overlap is certain instead of a matter of timing.
   */
  @Test
  void anOverlappingLoadIsRefusedWithFailedPrecondition() throws Exception {
    createBatchTypes();

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch refused = new CountDownLatch(1);

    try (final GraphBatch holder = getServerDatabase(0, getDatabaseName()).batch().build()) {
      assertThat(holder).isNotNull();

      final StreamObserver<GraphBatchChunk> overlapping = asyncAuthenticatedStub.graphBatchLoad(
          new StreamObserver<>() {
            @Override
            public void onNext(final GraphBatchResult result) {
            }

            @Override
            public void onError(final Throwable t) {
              failure.set(t);
              refused.countDown();
            }

            @Override
            public void onCompleted() {
              refused.countDown();
            }
          });

      overlapping.onNext(GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .addRecords(GraphBatchRecord.newBuilder()
              .setKind(GraphBatchRecord.Kind.VERTEX)
              .setTypeName("GuardNode")
              .setTempId("o1")
              .putProperties("name", stringValue("overlapping")))
          .build());
      overlapping.onCompleted();

      assertThat(refused.await(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(failure.get()).isInstanceOf(StatusRuntimeException.class);
    final StatusRuntimeException e = (StatusRuntimeException) failure.get();
    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(e.getStatus().getDescription()).contains("already in progress");

    // The refused load must not have consumed the slot it was denied.
    assertThat(loadTwoVertices("o2", "o3").getVerticesCreated()).isEqualTo(2);
  }

  private void createBatchTypes() {
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE VERTEX TYPE GuardNode IF NOT EXISTS")
        .build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE EDGE TYPE GuardLink IF NOT EXISTS")
        .build());
  }

  private GraphBatchResult loadTwoVertices(final String firstId, final String secondId) throws Exception {
    final AtomicReference<GraphBatchResult> resultRef = new AtomicReference<>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);

    final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
        new StreamObserver<>() {
          @Override
          public void onNext(final GraphBatchResult result) {
            resultRef.set(result);
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
        .setDatabase(getDatabaseName())
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId(firstId)
            .putProperties("name", stringValue(firstId)))
        .addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("GuardNode")
            .setTempId(secondId)
            .putProperties("name", stringValue(secondId)))
        .build());
    observer.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).as("the load after the guard was released must succeed").isNull();
    assertThat(resultRef.get()).isNotNull();
    return resultRef.get();
  }

  private GrpcValue stringValue(final String s) {
    return GrpcValue.newBuilder().setStringValue(s).build();
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

  // Regression: #6070
  /**
   * Hardening of the {@code GraphBatchLoad} RPC, done as part of wiring a real client onto it (issue #6070). The
   * RPC had been implemented and tested but never called by any client, and the gaps that had gone unnoticed are
   * exactly the ones that only show on a load large enough to be worth a streaming transport:
   * <ul>
   *   <li>the temporary-id mapping was always echoed back in full, so a load of a few million vertices built a
   *   response past the 4 MB default message limit and failed at the very end, with everything committed;</li>
   *   <li>the vertex buffer was hardcoded at 10,000, so a caller could not lower it - which a replicated database
   *   needs, one buffer being one Raft entry;</li>
   *   <li>a failure reported nothing about what had already been committed, even though the batch commits
   *   incrementally and an error is not a rollback.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Hardening {

    @BeforeEach
    void createLoadTypes() {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE VERTEX TYPE Issue6070Node IF NOT EXISTS")
          .build());
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE EDGE TYPE Issue6070Link IF NOT EXISTS")
          .build());
    }

    /** Left unset, the mapping still travels: a load small enough to consume it must not lose it to the cap. */
    @Test
    void returnsTheIdMappingByDefaultForASmallLoad() throws Exception {
      final GraphBatchResult result = loadWithIdMappingPreference(null, 5);

      assertThat(result.getVerticesCreated()).isEqualTo(5);
      assertThat(result.getIdMappingOmitted()).as("a mapping this size is well under the cap").isFalse();
      assertThat(result.getIdMappingSize()).isEqualTo(5);
      assertThat(result.getIdMappingMap()).hasSize(5);
      assertThat(result.getIdMappingMap().get("n0")).as("the mapping resolves a temporary id to a RID")
          .startsWith("#");
    }

    /**
     * The whole reason the streaming client sets it: a loader that never reads the mapping should not make the
     * server build one, which is what puts a large load's response over the message limit.
     */
    @Test
    void omitsTheIdMappingWhenTheCallerDeclinesIt() throws Exception {
      final GraphBatchResult result = loadWithIdMappingPreference(false, 5);

      assertThat(result.getVerticesCreated()).isEqualTo(5);
      assertThat(result.getIdMappingMap()).as("declined explicitly, so not sent").isEmpty();
      // Not "omitted": nothing was dropped that the caller wanted. The count is still reported, which is what
      // distinguishes this from a load that used no temporary ids at all.
      assertThat(result.getIdMappingOmitted()).isFalse();
      assertThat(result.getIdMappingSize()).isEqualTo(5);
    }

    /** Demanded explicitly, the mapping travels whatever the cap says. */
    @Test
    void returnsTheIdMappingWhenTheCallerDemandsIt() throws Exception {
      final GraphBatchResult result = loadWithIdMappingPreference(true, 5);

      assertThat(result.getIdMappingMap()).hasSize(5);
      assertThat(result.getIdMappingOmitted()).isFalse();
      assertThat(result.getIdMappingSize()).isEqualTo(5);
    }

    /**
     * A caller lowering the vertex buffer must actually get it - which a replicated database needs, one buffer
     * being one Raft entry. The buffer used to be hardcoded at 10,000 and the option did not exist.
     * <p>
     * A load that succeeds cannot tell the two apart: the same vertices land whether they went in two at a time
     * or all at once. Where the boundary becomes visible is a load that fails in the middle of the vertex phase,
     * because each buffer is committed as it is flushed. Nine vertices at a buffer of two, with a mandatory
     * property missing on the ninth, commits the first eight and fails on the buffer holding the ninth. Ignore
     * the requested buffer and nothing is flushed before that single failing flush at the end, so nothing
     * survives.
     */
    @Test
    void honoursTheRequestedVertexBatchSize() throws Exception {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE PROPERTY Issue6070Node.tag IF NOT EXISTS STRING (MANDATORY TRUE)")
          .build());

      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);
      final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
          observer(null, errorRef, done));

      final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(2).build());
      for (int i = 0; i < 8; i++)
        chunk.addRecords(GraphBatchRecord.newBuilder()
            .setKind(GraphBatchRecord.Kind.VERTEX)
            .setTypeName("Issue6070Node")
            .setTempId("t" + i)
            .putProperties("tag", GrpcValue.newBuilder().setStringValue("t" + i).build())
            .build());
      // The ninth carries no 'tag', so the buffer holding it cannot be created.
      chunk.addRecords(GraphBatchRecord.newBuilder()
          .setKind(GraphBatchRecord.Kind.VERTEX)
          .setTypeName("Issue6070Node")
          .setTempId("t8")
          .build());

      observer.onNext(chunk.build());
      observer.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).as("the mandatory property is missing, so the load must fail").isNotNull();

      assertThat(countOf("Issue6070Node"))
          .as("a buffer of 2 commits the first 8 before the 9th fails; ignoring it commits nothing at all")
          .isEqualTo(8);
    }

    /**
     * A vertex buffer above the server-side cap is clamped, and the caller has to be told (issue #6083 item 1).
     * <p>
     * The cap itself is right - a caller-supplied buffer size must not be able to exhaust the server heap on one
     * request - but it applied with no log line, so a caller who asked for more got something other than what it
     * set and was never told, which reads as compliance. What is asserted here is the WARNING naming both the
     * requested and the effective value; the clamp is not observable any other way on a load that succeeds, since
     * the same vertices land whether they went in one buffer or many.
     */
    @Test
    void warnsWhenTheRequestedVertexBatchSizeIsClamped() throws Exception {
      final CapturingLogger logger = CapturingLogger.install();
      try {
        // Above GRAPH_BATCH_MAX_VERTEX_BUFFER (1,000,000). The load itself is tiny; only the option matters.
        load(GraphBatchOptions.newBuilder().setVertexBatchSize(5_000_000).build(), 2);

        assertThat(logger.findWarning("vertexBatchSize"))
            .as("a clamped buffer must be reported, not applied silently").isNotNull();
        assertThat(logger.findWarning("vertexBatchSize").args)
            .as("the log line must name both the requested and the effective value")
            .contains(5_000_000, 1_000_000);
      } finally {
        logger.uninstall();
      }
    }

    /** The counterpart: a buffer inside the cap is applied verbatim and must produce no warning. */
    @Test
    void doesNotWarnWhenTheRequestedVertexBatchSizeIsHonoured() throws Exception {
      final CapturingLogger logger = CapturingLogger.install();
      try {
        load(GraphBatchOptions.newBuilder().setVertexBatchSize(1_000).build(), 2);

        assertThat(logger.findWarning("vertexBatchSize"))
            .as("nothing was clamped, so there is nothing to report").isNull();
      } finally {
        logger.uninstall();
      }
    }

    /**
     * A load that fails part-way is not rolled back, because the batch commits incrementally. The counters of
     * what is durable have to reach the caller, and a status carries no message, so they ride the trailers.
     */
    @Test
    void reportsPartialCommitCountersOnTheTrailersOfAFailedLoad() throws Exception {
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);

      final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(observer(null, errorRef, done));

      // A first chunk that commits: vertexBatchSize 2 with 4 vertices forces two full buffers through before the
      // failure, so there is something durable to report.
      final GraphBatchChunk.Builder first = GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(2).build());
      for (int i = 0; i < 4; i++)
        first.addRecords(vertex("p" + i));
      observer.onNext(first.build());

      // Then an edge pointing at a temporary id nothing declared, which fails inside onNext.
      observer.onNext(GraphBatchChunk.newBuilder()
          .addRecords(GraphBatchRecord.newBuilder()
              .setKind(GraphBatchRecord.Kind.EDGE)
              .setTypeName("Issue6070Link")
              .setFromRef("p0")
              .setToRef("pNeverDeclared"))
          .build());
      observer.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).isInstanceOf(StatusRuntimeException.class);

      final StatusRuntimeException failure = (StatusRuntimeException) errorRef.get();
      final Metadata trailers = failure.getTrailers();
      assertThat(trailers).as("a failed load must carry its counters").isNotNull();

      final GraphBatchResult partial = trailers.get(GraphBatchProtocol.RESULT_TRAILER);
      assertThat(partial).as("the partial-commit trailer must be present on a failed load").isNotNull();
      assertThat(partial.getPartialCommit()).as("vertices were committed before the failure").isTrue();
      assertThat(partial.getVerticesCreated())
          .as("the counters must say how much of the load is durable, not zero")
          .isEqualTo(4);

      // ...and what the trailer claims is durable really is: the failure did not undo it.
      assertThat(countOf("Issue6070Node")).isEqualTo(4);
    }

    /**
     * Zero commit retries means "fail on the first error", not "no preference", so the two retry fields are
     * {@code optional} on the wire. A plain proto3 int32 defaults to 0 when unset, so a server reading it with
     * the usual {@code > 0} guard would silently ignore the one caller who explicitly asked not to retry - the
     * caller with the strongest opinion about it. The HTTP endpoint gets this right (a query parameter is
     * present or it is not), so the RPC has to as well.
     */
    @Test
    void distinguishesZeroCommitRetriesFromUnset() throws Exception {
      assertThat(GraphBatchOptions.newBuilder().build().hasCommitRetries())
          .as("an untouched options message must not claim a retry setting").isFalse();
      assertThat(GraphBatchOptions.newBuilder().setCommitRetries(0).build().hasCommitRetries())
          .as("asking for zero retries must be distinguishable from asking for nothing").isTrue();
      assertThat(GraphBatchOptions.newBuilder().setCommitRetryDelayMs(0).build().hasCommitRetryDelayMs())
          .as("asking for no back-off must be distinguishable from asking for nothing").isTrue();

      // And a load carrying them still runs.
      final GraphBatchResult result = load(GraphBatchOptions.newBuilder()
          .setCommitRetries(0)
          .setCommitRetryDelayMs(0)
          .build(), 3);

      assertThat(result.getVerticesCreated()).isEqualTo(3);
      assertThat(countOf("Issue6070Node")).isEqualTo(3);
    }

    /**
     * The counters must not claim an edge that never reached the database. An edge is buffered when its record
     * arrives and is written in {@code commitEvery}-sized flushes, with the incoming direction connected at
     * {@code close()}, so a load that dies at either point is still holding edges that did not land. Counting
     * received edges rather than flushed ones would report those as committed, and a caller reconciling against
     * that number re-sends too little and loses them - the exact failure the trailer exists to prevent.
     * <p>
     * The failure is arranged on the flush that {@code close()} performs, past the vertex phase and past a flush that committed: an
     * edge buffer of two writes the first two edges as the second one arrives, and the third, which lacks the MANDATORY property its
     * type declares, fails the flush of the close. (It used to be arranged by pointing an edge at a bucket that does not exist; since
     * issue #12895 such a RID endpoint is refused when its record is read, before anything of the edge is buffered.) The vertices and
     * the first two edges are committed by then, so the trailer has to report that part of the load as durable.
     * <p>
     * Since issue #6083 this also pins the two properties that follow from the flush cleaning up after itself:
     * the edge count is exact rather than a lower bound, and {@code countType} agrees with a traversal because no
     * unconnected edge record is left behind.
     */
    @Test
    void countsConnectedEdgesExactlyOnTheTrailerOfALoadThatFailsClosing() throws Exception {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE EDGE TYPE Issue6070StrictLink IF NOT EXISTS")
          .build());
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE PROPERTY Issue6070StrictLink.tag IF NOT EXISTS STRING (MANDATORY TRUE)")
          .build());

      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);

      final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
          observer(null, errorRef, done));

      final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(2).setBatchSize(2).build());
      for (int i = 0; i < 4; i++)
        chunk.addRecords(vertex("e" + i));

      // The second edge fills the buffer of two: both are written and connected there. The third stays buffered until close(), whose
      // flush fails on it.
      chunk.addRecords(strictEdge("e0", "e1", "first"));
      chunk.addRecords(strictEdge("e1", "e2", "second"));
      chunk.addRecords(GraphBatchRecord.newBuilder()
          .setKind(GraphBatchRecord.Kind.EDGE)
          .setTypeName("Issue6070StrictLink")
          .setFromRef("e2")
          .setToRef("e3"));

      observer.onNext(chunk.build());
      observer.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).as("an edge without its mandatory property must fail the flush of the close")
          .isInstanceOf(StatusRuntimeException.class);

      final Metadata trailers = ((StatusRuntimeException) errorRef.get()).getTrailers();
      assertThat(trailers).isNotNull();
      final GraphBatchResult partial = trailers.get(GraphBatchProtocol.RESULT_TRAILER);
      assertThat(partial).as("a failed load must carry its counters").isNotNull();

      assertThat(partial.getVerticesCreated()).as("the vertex buffers committed before the edges were flushed")
          .isEqualTo(4);
      assertThat(partial.getPartialCommit()).as("vertices are durable, so this is a partial commit").isTrue();

      assertThat(countOf("Issue6070Node")).isEqualTo(4);

      // The invariant that matters, and the one this counter exists to keep: never claim an edge the graph does
      // not hold. Over-reporting costs a caller the edge, silently - counting received records instead of
      // connected ones did exactly that here, claiming all three.
      //
      // Since issue #6083 the counter is EXACT rather than a lower bound: the two edges of the flush that committed are the ones the
      // graph holds, and the trailer says so. A caller reconciling against this re-sends exactly the edge that is missing.
      final long connected = connectedEdgeCount("Issue6070StrictLink");
      assertThat(connected).as("the flush that committed before the failing one keeps its edges, reachable").isEqualTo(2);
      assertThat(partial.getEdgesCreated()).as("the trailer must report exactly the edges the graph holds")
          .isEqualTo(connected);

      // #6083 item 2: countType() agrees with a traversal: no edge record is left that no vertex points at
      assertThat(countOf("Issue6070StrictLink"))
          .as("a failed flush must leave no edge record that no vertex points at").isEqualTo(connected);
    }

    /**
     * Issue #12895: an edge endpoint written as a RID was taken on trust by {@code GraphBatch}. One that does not exist failed at the
     * close of the batch with {@code NOT_FOUND}, one that is not a vertex with {@code INTERNAL} (a {@code ClassCastException}), and
     * either left every edge of the load written but unreachable. It is refused when its record is read now, as
     * {@code INVALID_ARGUMENT}, and the abandoned load leaves no edge behind.
     */
    @Test
    void refusesAnEdgeEndpointRidThatDoesNotExistAndLeavesNoUnreachableEdge() throws Exception {
      assertEndpointRefusedAndNoEdgeLeftBehind("#99:0", "does not exist");
    }

    /** Issue #12895: the same for a RID that exists and is a document. */
    @Test
    void refusesAnEdgeEndpointRidOfADocumentAndLeavesNoUnreachableEdge() throws Exception {
      authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("CREATE DOCUMENT TYPE Issue12895Doc IF NOT EXISTS")
          .build());
      final String documentRid = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCommand("INSERT INTO Issue12895Doc SET x = 1")
          .setReturnRows(true)
          .build()).getRecords(0).getRid();

      assertEndpointRefusedAndNoEdgeLeftBehind(documentRid, "is not a vertex");
    }

    private void assertEndpointRefusedAndNoEdgeLeftBehind(final String badRid, final String reason) throws Exception {
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);
      final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(observer(null, errorRef, done));

      final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder().setDatabase(getDatabaseName());
      chunk.addRecords(vertex("a"));
      chunk.addRecords(vertex("b"));
      chunk.addRecords(GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.EDGE).setTypeName("Issue6070Link").setFromRef("a")
          .setToRef("b"));
      chunk.addRecords(GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.EDGE).setTypeName("Issue6070Link").setFromRef("a")
          .setToRef(badRid));
      observer.onNext(chunk.build());
      observer.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).isInstanceOf(StatusRuntimeException.class);
      final StatusRuntimeException failure = (StatusRuntimeException) errorRef.get();
      assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(failure.getStatus().getDescription()).contains(badRid).contains(reason);

      // the load is abandoned with its buffered edges dropped, and no edge record is left that no vertex points at
      final long connected = connectedEdgeCount();
      assertThat(connected).as("the refused load wrote no edge").isZero();
      assertThat(countOf("Issue6070Link")).as("and no edge record is left that no vertex points at").isEqualTo(connected);
    }

    private GraphBatchResult loadWithIdMappingPreference(final Boolean returnIdMapping, final int vertices)
        throws Exception {
      final GraphBatchOptions.Builder options = GraphBatchOptions.newBuilder();
      if (returnIdMapping != null)
        options.setReturnIdMapping(returnIdMapping);
      return load(options.build(), vertices);
    }

    private GraphBatchResult load(final GraphBatchOptions options, final int vertices) throws Exception {
      final AtomicReference<GraphBatchResult> resultRef = new AtomicReference<>();
      final AtomicReference<Throwable> errorRef = new AtomicReference<>();
      final CountDownLatch done = new CountDownLatch(1);

      final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
          observer(resultRef, errorRef, done));

      final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder()
          .setDatabase(getDatabaseName())
          .setOptions(options);
      for (int i = 0; i < vertices; i++)
        chunk.addRecords(vertex("n" + i));

      observer.onNext(chunk.build());
      observer.onCompleted();

      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(errorRef.get()).as("the load must not fail").isNull();
      return resultRef.get();
    }

    /**
     * Carries {@code tag} as well as {@code name} even though nothing here reads it: the tests share one server
     * and one database, and {@link #honoursTheRequestedVertexBatchSize()} declares {@code tag} MANDATORY to arrange
     * its failure. Whether that has already run depends on method order, so a vertex without it is a load that
     * fails or not depending on which test went first. Setting it always makes every other test independent of
     * that order, and it is harmless before the property exists.
     */
    private GraphBatchRecord vertex(final String tempId) {
      return GraphBatchRecord.newBuilder()
          .setKind(GraphBatchRecord.Kind.VERTEX)
          .setTypeName("Issue6070Node")
          .setTempId(tempId)
          .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build())
          .putProperties("tag", GrpcValue.newBuilder().setStringValue(tempId).build())
          .build();
    }

    private static StreamObserver<GraphBatchResult> observer(final AtomicReference<GraphBatchResult> resultRef,
        final AtomicReference<Throwable> errorRef, final CountDownLatch done) {
      return new StreamObserver<>() {
        @Override
        public void onNext(final GraphBatchResult result) {
          if (resultRef != null)
            resultRef.set(result);
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
      };
    }

    /** Edges actually reachable from the vertices, as opposed to edge records the failed flush left orphaned. */
    private long connectedEdgeCount() {
      return connectedEdgeCount("Issue6070Link");
    }

    private long connectedEdgeCount(final String edgeTypeName) {
      final long[] connected = { 0 };
      final Database db = getServerDatabase(0, getDatabaseName());
      db.transaction(() -> db.iterateType("Issue6070Node", true)
          .forEachRemaining(r -> connected[0] += r.asVertex().countEdges(Vertex.DIRECTION.OUT, edgeTypeName)));
      return connected[0];
    }

    private static GraphBatchRecord strictEdge(final String from, final String to, final String tag) {
      return GraphBatchRecord.newBuilder()
          .setKind(GraphBatchRecord.Kind.EDGE)
          .setTypeName("Issue6070StrictLink")
          .setFromRef(from)
          .setToRef(to)
          .putProperties("tag", GrpcValue.newBuilder().setStringValue(tag).build())
          .build();
    }

    private long countOf(final String typeName) {
      return getServerDatabase(0, getDatabaseName()).countType(typeName, true);
    }

    /**
     * Captures ArcadeDB log records by swapping the global logger for the duration of a test, keeping the message
     * template and its arguments UNSUBSTITUTED so an assertion can check the values the line was given rather
     * than a formatted string.
     * <p>
     * Every record is also forwarded to a real {@link DefaultLogger}. The logger is a process-wide singleton
     * shared with the embedded server under test, so a capturing logger that only captured would silently swallow
     * anything the server logged while it was installed - including the diagnostics needed to understand a failure
     * inside that window. Tee-ing keeps the swap invisible to everything except the assertions.
     * <p>
     * <b>The swap is still global, and that bounds where this helper is usable.</b> It is sound here because
     * these ITs run sequentially within one class and one forked JVM, so nothing else is logging into the capture
     * window. Run this suite with cross-class parallelism in a shared JVM and a capture could pick up lines from
     * unrelated concurrent activity - {@link #findWarning} would still match on the template, but a test asserting
     * the ABSENCE of a warning could see another test's. Anything relying on that would need per-test isolation of
     * the logger rather than a singleton swap.
     */
    private static final class CapturingLogger implements Logger {

      record Record(Level level, String message, List<Object> args) {
      }

      private final List<Record> records  = new CopyOnWriteArrayList<>();
      private final Logger       delegate = new DefaultLogger();

      static CapturingLogger install() {
        final CapturingLogger logger = new CapturingLogger();
        LogManager.instance().setLogger(logger);
        return logger;
      }

      void uninstall() {
        LogManager.instance().setLogger(new DefaultLogger());
      }

      /** The first WARNING whose template contains {@code needle}, or null. */
      Record findWarning(final String needle) {
        for (final Record r : records)
          if (r.level() == Level.WARNING && r.message().contains(needle))
            return r;
        return null;
      }

      // Records directly rather than delegating to the varargs overload below: an Object[] of exactly these 17
      // arguments matches the fixed-arity signature, so the delegation would resolve back to this method.
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        record(level, message, Arrays.asList(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
            arg12, arg13, arg14, arg15, arg16, arg17));
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8,
            arg9, arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        record(level, message, args == null ? List.of() : Arrays.asList(args));
        delegate.log(requester, level, message, exception, context, args);
      }

      private void record(final Level level, final String message, final List<Object> args) {
        if (message != null)
          records.add(new Record(level, message, args));
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    }

  }
}

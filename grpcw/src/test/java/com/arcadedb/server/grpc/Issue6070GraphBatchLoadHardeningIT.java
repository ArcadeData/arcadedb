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
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.server.BaseGraphServerTest;
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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
public class Issue6070GraphBatchLoadHardeningIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

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
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    asyncAuthenticatedStub = ArcadeDbServiceGrpc.newStub(authenticatedChannel);

    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE VERTEX TYPE Issue6070Node IF NOT EXISTS")
        .build());
    authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCommand("CREATE EDGE TYPE Issue6070Link IF NOT EXISTS")
        .build());
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
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
   * The failure is arranged where the previous test cannot reach: past the vertex phase, on the edge flush that
   * {@code close()} performs, by pointing an edge at a bucket that does not exist. The vertices are committed
   * by then, so the trailer has to report some of the load as durable.
   * <p>
   * Since issue #6083 this also pins the two properties that follow from the flush cleaning up after itself:
   * the edge count is exact rather than a lower bound, and {@code countType} agrees with a traversal because no
   * unconnected edge record is left behind.
   */
  @Test
  void countsConnectedEdgesExactlyOnTheTrailerOfALoadThatFailsClosing() throws Exception {
    final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    final CountDownLatch done = new CountDownLatch(1);

    final StreamObserver<GraphBatchChunk> observer = asyncAuthenticatedStub.graphBatchLoad(
        observer(null, errorRef, done));

    final GraphBatchChunk.Builder chunk = GraphBatchChunk.newBuilder()
        .setDatabase(getDatabaseName())
        .setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(2).build());
    for (int i = 0; i < 4; i++)
      chunk.addRecords(vertex("e" + i));

    // Both edges are buffered and neither is flushed before close(). The second originates from a bucket that
    // does not exist, so the flush that close() runs fails on it while the first edge, whose source bucket is a
    // different one, is connected and committed by its own task. (A bad destination would not do: the outgoing
    // flush commits regardless and only the incoming connect fails afterwards, at which point those edges
    // genuinely are created and counting them is right.)
    chunk.addRecords(GraphBatchRecord.newBuilder()
        .setKind(GraphBatchRecord.Kind.EDGE)
        .setTypeName("Issue6070Link")
        .setFromRef("e0")
        .setToRef("e1"));
    chunk.addRecords(GraphBatchRecord.newBuilder()
        .setKind(GraphBatchRecord.Kind.EDGE)
        .setTypeName("Issue6070Link")
        .setFromRef("#31212:0")
        .setToRef("e2"));

    observer.onNext(chunk.build());
    observer.onCompleted();

    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(errorRef.get()).as("an edge pointing at a bucket that does not exist must fail the load")
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
    // connected ones did exactly that here, claiming both.
    //
    // Since issue #6083 the counter is EXACT rather than a lower bound: the connect pass advances it one durable
    // commit at a time instead of a whole flush at a time, so the one edge that was in fact connected is
    // reported as one rather than rounded down to zero. A caller reconciling against this re-sends exactly the
    // edge that is missing.
    final long connected = connectedEdgeCount();
    assertThat(connected).as("the task for the good edge's source bucket commits independently of the bad one")
        .isEqualTo(1);
    assertThat(partial.getEdgesCreated()).as("the trailer must report exactly the edges the graph holds")
        .isEqualTo(connected);

    // #6083 item 2: countType() may be used here now. It could not before - the failed flush left the doomed
    // edge's RECORD behind, connected to no vertex, so countType() answered 2 while a traversal reached 1. The
    // flush now reclaims the records it could not connect, which is what makes the two agree.
    assertThat(countOf("Issue6070Link"))
        .as("a failed flush must leave no edge record that no vertex points at").isEqualTo(connected);
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
    final long[] connected = { 0 };
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> db.iterateType("Issue6070Node", true)
        .forEachRemaining(r -> connected[0] += r.asVertex().countEdges(Vertex.DIRECTION.OUT, "Issue6070Link")));
    return connected[0];
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

  private final class AuthClientInterceptor implements ClientInterceptor {
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
  }
}

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

  private GraphBatchRecord vertex(final String tempId) {
    return GraphBatchRecord.newBuilder()
        .setKind(GraphBatchRecord.Kind.VERTEX)
        .setTypeName("Issue6070Node")
        .setTempId(tempId)
        .putProperties("name", GrpcValue.newBuilder().setStringValue(tempId).build())
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

  private long countOf(final String typeName) {
    return getServerDatabase(0, getDatabaseName()).countType(typeName, true);
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

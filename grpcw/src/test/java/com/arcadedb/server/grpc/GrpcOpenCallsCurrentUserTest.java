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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * An open client transaction and a client stream are authenticated when they start and go on for as long as the client
 * uses them, so the user they run as can change under them: dropped, given another password, or given other groups. Each
 * statement of a transaction and each chunk of a stream has to run with the user as it stands then, not as it stood when the
 * work began.
 * <p>
 * The service is driven in process and every callback of a stream runs on a thread of its own, so the thread switch that the
 * shared executor makes by chance happens on every call.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GrpcOpenCallsCurrentUserTest extends BaseGraphServerTest {
  private static final String NAME         = "GrpcOpenCallsCurrentUser";
  private static final String WRITER_GROUP = "openCallsWriter";
  private static final String READER_GROUP = "openCallsReader";
  private static final String USER         = "opencallsuser";
  private static final String PASSWORD     = "opencallsuser-pwd";
  private static final String OTHER_PWD    = "opencallsuser-other-pwd";

  private ArcadeDbGrpcService service;

  @BeforeEach
  void createDatabase() {
    service = new ArcadeDbGrpcService(getDatabaseName(), getServer(0));

    final ServerDatabase database = getServer(0).createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    database.getSchema().createDocumentType("Open");
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("Link");
    database.command("sql", "CREATE TIMESERIES TYPE Metric TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(NAME, WRITER_GROUP, new JSONObject().put("access", new JSONArray()).put("types", new JSONObject()
        .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
        .put("Open", writerAccess())
        .put("Node", writerAccess())
        .put("Link", writerAccess())
        .put("Metric", new JSONObject().put("access", new JSONArray().put("createRecord").put("readRecord")))));
    security.saveGroup(NAME, READER_GROUP, new JSONObject().put("access", new JSONArray()).put("types",
        new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));

    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(NAME, new JSONArray().put(WRITER_GROUP))));
  }

  @AfterEach
  void dropDatabase() {
    if (service != null)
      service.close();
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
    security.deleteGroup(NAME, WRITER_GROUP);
    security.deleteGroup(NAME, READER_GROUP);
    if (getServer(0).existsDatabase(NAME))
      getServer(0).dropLocalDatabase(getServer(0).getDatabaseForOperator(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(
        new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  // ---- the user of a call, resolved as it stands now ----

  @Test
  void theUserOfACallIsResolvedAsItStandsNow() {
    final GrpcCallUser callUser = GrpcCallUser.of(getServer(0), USER);
    assertThat(callUser).isNotNull();
    final ServerSecurityUser first = callUser.live();
    assertThat(callUser.live()).isSameAs(first);

    narrowUser();

    // the user object published by the change is the one in force, with the rights of its new groups
    assertThat(callUser.live()).isNotSameAs(first);
    assertThat(callUser.isValid()).isTrue();
    assertThat(callUser.live().getName()).isEqualTo(USER);
  }

  @Test
  void aDroppedUserOfACallIsRefused() {
    final GrpcCallUser callUser = GrpcCallUser.of(getServer(0), USER);
    getServer(0).getSecurity().dropUser(USER);

    assertThat(callUser.isValid()).isFalse();
    assertThatThrownBy(callUser::live).isInstanceOfSatisfying(StatusRuntimeException.class,
        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
  }

  @Test
  void aUserOfACallWhoseCredentialsChangedIsRefused() {
    final GrpcCallUser callUser = GrpcCallUser.of(getServer(0), USER);
    changePassword();

    assertThat(callUser.isValid()).isFalse();
  }

  @Test
  void aCallWithNoUserToCheckIsLeftAlone() {
    assertThat(GrpcCallUser.of(null, USER)).isNull();
    assertThat(GrpcCallUser.of(getServer(0), null)).isNull();
    assertThatThrownBy(() -> GrpcCallUser.of(getServer(0), "nosuchopencallsuser")).isInstanceOf(StatusRuntimeException.class);
  }

  // ---- an open client transaction ----

  @Test
  void aTransactionStatementRunsWithTheGrantsTheOwnerHoldsNow() {
    final String tx = begin();
    assertThat(command(tx, "INSERT INTO Open SET name = 'before'").error).isNull();

    narrowUser();

    final Outcome<ExecuteCommandResponse> refused = command(tx, "INSERT INTO Open SET name = 'after'");
    assertThat(refused.error).as("the statement must be refused").isNotNull();
    assertThat(Status.fromThrowable(refused.error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

    // the refused statement ends the transaction like any failed one: nothing of it is published
    assertThat(commit(tx).getCommitted()).isFalse();
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void aTransactionStatementOfAnOwnerWithTheSameGrantsStillRuns() {
    final String tx = begin();
    assertThat(command(tx, "INSERT INTO Open SET name = 'one'").error).isNull();
    // a change that leaves the rights as they were
    final ServerSecurity security = getServer(0).getSecurity();
    security.updateUser(security.getUser(USER).toJSON().copy());
    assertThat(command(tx, "INSERT INTO Open SET name = 'two'").error).isNull();

    assertThat(commit(tx).getCommitted()).isTrue();
    assertThat(countOf("Open")).isEqualTo(2);
  }

  @Test
  void aTaskOnTheTransactionThreadOfADroppedOwnerIsRefusedAndEndsTheTransaction() {
    final String tx = begin();
    final ArcadeDbGrpcService.TransactionContext txCtx = service.getActiveTransactionForTesting(tx);
    assertThat(txCtx).isNotNull();
    assertThat(command(tx, "INSERT INTO Open SET name = 'before'").error).isNull();

    getServer(0).getSecurity().dropUser(USER);

    assertThatThrownBy(() -> service.submitToActiveTransaction(txCtx, () -> "ran").get()).isInstanceOf(ExecutionException.class)
        .cause().isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));

    // rolled back and released, not left holding its thread, its slot and what it wrote
    assertThat(service.getActiveTransactionForTesting(tx)).isNull();
    assertThat(service.getActiveTransactionCount()).isZero();
    assertThat(service.getTransactionCountForPrincipal(USER)).isZero();
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void aTaskOnTheTransactionThreadOfAnOwnerWhoseCredentialsChangedIsRefused() {
    final String tx = begin();
    final ArcadeDbGrpcService.TransactionContext txCtx = service.getActiveTransactionForTesting(tx);

    changePassword();

    assertThatThrownBy(() -> service.submitToActiveTransaction(txCtx, () -> "ran").get()).isInstanceOf(ExecutionException.class)
        .cause().isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    assertThat(service.getActiveTransactionForTesting(tx)).isNull();
  }

  @Test
  void aTransactionOfADroppedOwnerEndsAtTheNextCallItReceives() {
    final String tx = begin();
    assertThat(command(tx, "INSERT INTO Open SET name = 'before'").error).isNull();

    getServer(0).getSecurity().dropUser(USER);

    // the call is refused before it reaches the transaction, and the refusal ends the transaction of the owner that is gone
    final Outcome<ExecuteCommandResponse> refused = command(tx, "INSERT INTO Open SET name = 'after'");
    assertThat(refused.error).as("the call must be refused").isNotNull();

    await().atMost(Duration.ofSeconds(30)).until(() -> service.getActiveTransactionForTesting(tx) == null);
    assertThat(service.getActiveTransactionCount()).isZero();
    assertThat(service.getTransactionCountForPrincipal(USER)).isZero();
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void aRefusedCallOfAnotherUserLeavesTheTransactionOfAValidOwnerAlone() {
    final String tx = begin();
    assertThat(command(tx, "INSERT INTO Open SET name = 'one'").error).isNull();

    final Outcome<ExecuteCommandResponse> other = new Outcome<>();
    service.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(NAME).setCommand("INSERT INTO Open SET name = 'two'")
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS))
        .setTransaction(TransactionContext.newBuilder().setTransactionId(tx)).build(), other);
    assertThat(Status.fromThrowable(other.error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);

    assertThat(service.getActiveTransactionForTesting(tx)).isNotNull();
    assertThat(commit(tx).getCommitted()).isTrue();
    assertThat(countOf("Open")).isEqualTo(1);
  }

  // ---- InsertStream ----

  @Test
  void anInsertStreamChunkRunsWithTheGrantsTheUserHoldsNow() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a")));
    assertThat(outcome.error).isNull();
    narrowUser();
    runOnNewThread(() -> stream.onNext(chunk("b")));
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).isNull();
    assertThat(outcome.completed).isTrue();
    // what the stream wrote before the change stands; the row after it was refused
    assertThat(outcome.last.getFailed()).isEqualTo(1);
    assertThat(countOf("Open")).isEqualTo(1);
  }

  @Test
  void anInsertStreamChunkOnTheSameThreadRunsWithTheGrantsTheUserHoldsNow() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final ExecutorService thread = Executors.newSingleThreadExecutor();
    try {
      final StreamObserver<InsertChunk> stream = service.insertStream(outcome);
      runOn(thread, () -> stream.onNext(firstChunk("a")));
      assertThat(outcome.error).isNull();
      narrowUser();
      runOn(thread, () -> stream.onNext(chunk("b")));
      runOn(thread, stream::onCompleted);
    } finally {
      thread.shutdownNow();
    }

    assertThat(outcome.error).isNull();
    assertThat(outcome.last.getFailed()).isEqualTo(1);
    assertThat(countOf("Open")).isEqualTo(1);
  }

  @Test
  void anInsertStreamEndsWhenItsUserWasDropped() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a")));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(() -> stream.onNext(chunk("b")));

    assertThat(outcome.error).as("the stream must end with a refusal").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    // nothing of the stream is committed
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void anInsertStreamDoesNotCommitWhenItsUserWasDroppedBeforeTheEnd() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a")));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).as("the stream must end with a refusal").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void anInsertStreamEndsWhenItsUserChangedPassword() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a")));
    assertThat(outcome.error).isNull();
    changePassword();
    runOnNewThread(() -> stream.onNext(chunk("b")));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void anInsertStreamIntoATransactionEndsWhenItsUserWasDropped() throws Exception {
    final String tx = begin();
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a").toBuilder().setTransaction(TransactionContext.newBuilder().setTransactionId(tx)).build()));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(() -> stream.onNext(chunk("b")));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    // the transaction of the dropped user is over with it
    assertThat(service.getActiveTransactionForTesting(tx)).isNull();
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void anInsertStreamWithNoTransactionOfItsOwnRunsWithTheGrantsTheUserHoldsNow() throws Exception {
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    // the rows are stored as they come: the stream holds no transaction between its chunks
    runOnNewThread(() -> stream.onNext(firstChunk("a", InsertOptions.TransactionMode.NONE)));
    assertThat(outcome.error).isNull();
    narrowUser();
    runOnNewThread(() -> stream.onNext(chunk("b")));
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).isNull();
    assertThat(outcome.last.getFailed()).isEqualTo(1);
    assertThat(countOf("Open")).isEqualTo(1);
  }

  @Test
  void anInsertStreamIntoATransactionEndsWhenTheTransactionThreadFindsItsUserDropped() throws Exception {
    final String tx = begin();
    final ArcadeDbGrpcService.TransactionContext txCtx = service.getActiveTransactionForTesting(tx);
    final Outcome<InsertSummary> outcome = new Outcome<>();
    final StreamObserver<InsertChunk> stream = service.insertStream(outcome);

    runOnNewThread(() -> stream.onNext(firstChunk("a").toBuilder().setTransaction(TransactionContext.newBuilder().setTransactionId(tx)).build()));
    assertThat(outcome.error).isNull();

    // The second chunk passes the check of the call and waits behind a task that holds the transaction's thread: the user is
    // dropped after that check and before the chunk runs there
    final CountDownLatch release = new CountDownLatch(1);
    txCtx.executor.submit(() -> {
      release.await(60, TimeUnit.SECONDS);
      return null;
    });
    final Thread second = new Thread(() -> stream.onNext(chunk("b")), "open-calls-second-chunk");
    second.start();
    await().atMost(Duration.ofSeconds(30)).until(() -> txCtx.executor.getQueue().size() == 1);
    getServer(0).getSecurity().dropUser(USER);
    release.countDown();
    second.join(60_000);

    assertThat(outcome.error).as("the stream must end with a refusal, not with a summary").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(service.getActiveTransactionForTesting(tx)).isNull();
    assertThat(countOf("Open")).isZero();
  }

  // ---- InsertBidirectional ----

  @Test
  void aBidirectionalInsertChunkRunsWithTheGrantsTheUserHoldsNow() {
    final BidiOutcome outcome = new BidiOutcome();
    final StreamObserver<InsertRequest> stream = service.insertBidirectional(outcome);

    stream.onNext(start());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.started);
    stream.onNext(bidiChunk("a", 1));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.acknowledged >= 1);
    narrowUser();
    stream.onNext(bidiChunk("b", 2));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.acknowledged >= 2);

    assertThat(outcome.error).isNull();
    assertThat(outcome.failedRows).isEqualTo(1);

    stream.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setCommit(true)).build());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.completed || outcome.error != null);
    assertThat(countOf("Open")).isEqualTo(1);
  }

  @Test
  void aBidirectionalInsertEndsWhenItsUserWasDropped() {
    final BidiOutcome outcome = new BidiOutcome();
    final StreamObserver<InsertRequest> stream = service.insertBidirectional(outcome);

    stream.onNext(start());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.started);
    stream.onNext(bidiChunk("a", 1));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.acknowledged >= 1);
    getServer(0).getSecurity().dropUser(USER);
    stream.onNext(bidiChunk("b", 2));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.error != null);

    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(outcome.acknowledged).isEqualTo(1);
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void aBidirectionalInsertEndsWhenItsUserWasDroppedBeforeAReplayIsAcknowledged() {
    final BidiOutcome outcome = new BidiOutcome();
    final StreamObserver<InsertRequest> stream = service.insertBidirectional(outcome);

    stream.onNext(start());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.started);
    stream.onNext(bidiChunk("a", 1));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.acknowledged >= 1);
    getServer(0).getSecurity().dropUser(USER);
    // the same sequence again: a replay of what was acknowledged
    stream.onNext(bidiChunk("a", 1));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.error != null);

    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(outcome.acknowledged).isEqualTo(1);
    assertThat(countOf("Open")).isZero();
  }

  @Test
  void aBidirectionalInsertDoesNotCommitWhenItsUserWasDropped() {
    final BidiOutcome outcome = new BidiOutcome();
    final StreamObserver<InsertRequest> stream = service.insertBidirectional(outcome);

    stream.onNext(start());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.started);
    stream.onNext(bidiChunk("a", 1));
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.acknowledged >= 1);
    getServer(0).getSecurity().dropUser(USER);
    stream.onNext(InsertRequest.newBuilder().setCommit(Commit.newBuilder().setCommit(true)).build());
    await().atMost(Duration.ofSeconds(30)).until(() -> outcome.error != null);

    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(outcome.completed).isFalse();
    assertThat(countOf("Open")).isZero();
  }

  // ---- GraphBatchLoad and TimeSeriesWriteStream ----

  @Test
  void aGraphLoadChunkRunsWithTheGrantsTheUserHoldsNow() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final ExecutorService thread = Executors.newSingleThreadExecutor();
    try {
      final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);
      // a buffer of one vertex: each vertex is written by the chunk that carries it
      runOn(thread, () -> stream.onNext(firstGraphChunk("a", 1)));
      assertThat(outcome.error).isNull();
      narrowUser();
      runOn(thread, () -> stream.onNext(GraphBatchChunk.newBuilder().addRecords(vertex("b")).build()));
    } finally {
      thread.shutdownNow();
    }

    assertThat(outcome.error).as("the chunk must be refused").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(countOf("Node")).isEqualTo(1);
  }

  @Test
  void aTimeSeriesChunkRunsWithTheGrantsTheUserHoldsNow() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final ExecutorService thread = Executors.newSingleThreadExecutor();
    try {
      final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);
      runOn(thread, () -> stream.onNext(firstSeriesChunk()));
      assertThat(outcome.error).isNull();
      narrowUser();
      runOn(thread, () -> stream.onNext(seriesChunk()));
    } finally {
      thread.shutdownNow();
    }

    assertThat(outcome.error).as("the chunk must be refused").isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    assertThat(countOf("Metric")).isEqualTo(1);
  }

  @Test
  void aGraphLoadEndsWhenItsUserWasDropped() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    runOnNewThread(() -> stream.onNext(firstGraphChunk("a")));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(() -> stream.onNext(GraphBatchChunk.newBuilder().addRecords(vertex("b")).build()));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void aGraphLoadDoesNotFlushWhenItsUserWasDroppedBeforeTheEnd() throws Exception {
    final Outcome<GraphBatchResult> outcome = new Outcome<>();
    final StreamObserver<GraphBatchChunk> stream = service.graphBatchLoad(outcome);

    // the vertices wait in the buffer of the stream until the end of the call writes them
    runOnNewThread(() -> stream.onNext(firstGraphChunk("a")));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(stream::onCompleted);

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("Node")).isZero();
  }

  @Test
  void aTimeSeriesWriteStreamEndsWhenItsUserWasDropped() throws Exception {
    final Outcome<TimeSeriesWriteSummary> outcome = new Outcome<>();
    final StreamObserver<TimeSeriesWriteChunk> stream = service.timeSeriesWriteStream(outcome);

    runOnNewThread(() -> stream.onNext(firstSeriesChunk()));
    assertThat(outcome.error).isNull();
    getServer(0).getSecurity().dropUser(USER);
    runOnNewThread(() -> stream.onNext(seriesChunk()));

    assertThat(outcome.error).isNotNull();
    assertThat(Status.fromThrowable(outcome.error).getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    assertThat(countOf("Metric")).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------------------------

  private static JSONObject writerAccess() {
    return new JSONObject().put("access",
        new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"));
  }

  /** The user keeps its database and gets the group that only reads. */
  private void narrowUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    final JSONObject configuration = security.getUser(USER).toJSON().copy();
    configuration.put("databases", new JSONObject().put(NAME, new JSONArray().put(READER_GROUP)));
    security.updateUser(configuration);
  }

  private void changePassword() {
    final ServerSecurity security = getServer(0).getSecurity();
    final JSONObject configuration = security.getUser(USER).toJSON().copy();
    configuration.put("password", security.encodePassword(OTHER_PWD));
    security.updateUser(configuration);
  }

  private static DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername(USER).setPassword(PASSWORD).build();
  }

  private String begin() {
    final Outcome<BeginTransactionResponse> outcome = new Outcome<>();
    service.beginTransaction(BeginTransactionRequest.newBuilder().setDatabase(NAME).setCredentials(credentials()).build(), outcome);
    assertThat(outcome.error).isNull();
    return outcome.last.getTransactionId();
  }

  private Outcome<ExecuteCommandResponse> command(final String tx, final String sql) {
    final Outcome<ExecuteCommandResponse> outcome = new Outcome<>();
    service.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(NAME).setCommand(sql).setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(tx)).build(), outcome);
    return outcome;
  }

  private CommitTransactionResponse commit(final String tx) {
    final Outcome<CommitTransactionResponse> outcome = new Outcome<>();
    service.commitTransaction(CommitTransactionRequest.newBuilder().setCredentials(credentials())
        .setTransaction(TransactionContext.newBuilder().setTransactionId(tx)).build(), outcome);
    assertThat(outcome.error).isNull();
    return outcome.last;
  }

  private long countOf(final String type) {
    return getServer(0).getDatabaseForOperator(NAME).query("sql", "SELECT count(*) AS c FROM " + type).next().<Long>getProperty("c");
  }

  private static InsertOptions insertOptions() {
    return insertOptions(InsertOptions.TransactionMode.PER_STREAM);
  }

  private static InsertOptions insertOptions(final InsertOptions.TransactionMode mode) {
    return InsertOptions.newBuilder().setTargetClass("Open").setTransactionMode(mode).setDatabase(NAME)
        .setCredentials(credentials()).build();
  }

  private static GrpcRecord row(final String name) {
    return GrpcRecord.newBuilder().putProperties("name", GrpcValue.newBuilder().setStringValue(name).build()).build();
  }

  /** The first chunk of an insert stream, sent by the user of the test. */
  private static InsertChunk firstChunk(final String name) {
    return firstChunk(name, InsertOptions.TransactionMode.PER_STREAM);
  }

  private static InsertChunk firstChunk(final String name, final InsertOptions.TransactionMode mode) {
    return InsertChunk.newBuilder().setDatabase(NAME).setCredentials(credentials()).setOptions(insertOptions(mode)).setChunkSeq(1)
        .addRows(row(name)).build();
  }

  /** A later chunk: no database, no credentials. */
  private static InsertChunk chunk(final String name) {
    return InsertChunk.newBuilder().setChunkSeq(2).addRows(row(name)).build();
  }

  private static InsertRequest start() {
    return InsertRequest.newBuilder()
        .setStart(Start.newBuilder().setDatabase(NAME).setCredentials(credentials()).setOptions(insertOptions())).build();
  }

  private static InsertRequest bidiChunk(final String name, final long seq) {
    return InsertRequest.newBuilder().setChunk(InsertChunk.newBuilder().setChunkSeq(seq).addRows(row(name))).build();
  }

  private static GraphBatchChunk firstGraphChunk(final String name) {
    return GraphBatchChunk.newBuilder().setDatabase(NAME).setCredentials(credentials()).addRecords(vertex(name)).build();
  }

  /** The first chunk of a graph load that holds {@code bufferSize} vertices before it writes them. */
  private static GraphBatchChunk firstGraphChunk(final String name, final int bufferSize) {
    return firstGraphChunk(name).toBuilder().setOptions(GraphBatchOptions.newBuilder().setVertexBatchSize(bufferSize)).build();
  }

  private static GraphBatchRecord vertex(final String name) {
    return GraphBatchRecord.newBuilder().setKind(GraphBatchRecord.Kind.VERTEX).setTypeName("Node").setTempId(name)
        .putProperties("name", GrpcValue.newBuilder().setStringValue(name).build()).build();
  }

  private static TimeSeriesWriteChunk firstSeriesChunk() {
    return seriesChunk().toBuilder().setDatabase(NAME).setCredentials(credentials()).build();
  }

  private static TimeSeriesWriteChunk seriesChunk() {
    return TimeSeriesWriteChunk.newBuilder().setType("Metric").addPoints(TimeSeriesPoint.newBuilder().setTimestamp(System.nanoTime())
        .putTags("host", GrpcValue.newBuilder().setStringValue("h1").build())
        .putFields("v", GrpcValue.newBuilder().setDoubleValue(1.5).build())).build();
  }

  /** Runs the callback on a thread that never served a request: the case of a call whose next message lands on a new thread. */
  private static void runOnNewThread(final Runnable callback) throws InterruptedException {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        callback.run();
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "open-calls-callback");
    thread.start();
    thread.join(60_000);
    assertThat(thread.isAlive()).as("the callback must return").isFalse();
    assertThat(failure.get()).as("a callback reports its failures through the observer").isNull();
  }

  private static void runOn(final ExecutorService thread, final Runnable callback) throws Exception {
    thread.submit(callback).get(60, TimeUnit.SECONDS);
  }

  /** What the client of a call sees: the last message, the end of the call and its failure. */
  private static class Outcome<T> extends ServerCallStreamObserver<T> {
    volatile T         last;
    volatile Throwable error;
    volatile boolean   completed;

    @Override
    public void onNext(final T value) {
      last = value;
    }

    @Override
    public void onError(final Throwable t) {
      error = t;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public void setOnCancelHandler(final Runnable onCancelHandler) {
    }

    @Override
    public void setCompression(final String compression) {
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(final Runnable onReadyHandler) {
    }

    @Override
    public void request(final int count) {
    }

    @Override
    public void setMessageCompression(final boolean enable) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }
  }

  /** What the client of an InsertBidirectional call sees. */
  private static final class BidiOutcome extends Outcome<InsertResponse> {
    volatile boolean started;
    volatile int     acknowledged;
    volatile long    failedRows;

    @Override
    public void onNext(final InsertResponse value) {
      if (value.hasStarted())
        started = true;
      else if (value.hasBatchAck()) {
        failedRows += value.getBatchAck().getFailed();
        acknowledged++;
      }
    }
  }
}

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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.security.ServerSecurity;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Client-managed transactions over gRPC: ownership, finalization races, isolation, reaping and the transaction messages
 * of a statement.
 * Each nested class is the regression of one issue and keeps that issue's own client fixture; they share the server,
 * started with the gRPC plugin and nothing else configured.
 */
public class GrpcTransactionIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #5040
  /**
   * Regression test for issue #5040.
   * <p>
   * Server-side gRPC transactions were identified by a predictable, monotonic id
   * ({@code "tx_" + System.nanoTime()}) and every transaction-scoped RPC acted on the transaction
   * found by that id without re-checking per-database authorization or transaction ownership. Together
   * these allowed an authenticated user to hijack another user's in-flight transaction on a database
   * they have no rights to.
   * <p>
   * These tests set up an owner authorized only for the owner's database and an attacker authorized
   * only for a different database. The attacker guesses the owner's transaction id and tries to drive
   * it via {@code commitTransaction}, {@code rollbackTransaction} and {@code executeCommand}; every
   * attempt must be denied while the owner's own operations still succeed. A third user, authorized for
   * the owner's database but NOT the transaction owner, is also denied, proving the ownership binding is
   * enforced independently of per-database authorization. Finally, transaction ids must be UUID-based
   * (high-entropy, collision-free).
   */
  @Nested
  class TransactionHijack {
    private static final String OWNER_DB      = "hijackowner5040db";
    private static final String ATTACKER_DB   = "hijackattacker5040db";
    private static final String OWNER_USER    = "owner5040";
    private static final String OWNER_PASS    = "owner5040pass";
    private static final String ATTACKER_USER = "attacker5040";
    private static final String ATTACKER_PASS = "attacker5040pass";
    private static final String NONOWNER_USER = "nonowner5040";
    private static final String NONOWNER_PASS = "nonowner5040pass";

    private static final Pattern TX_ID_PATTERN =
        Pattern.compile("tx_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static final Metadata.Key<String> USER_HEADER     =
        Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER =
        Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER =
        Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel channel;

    @BeforeEach
    void setup() {
      final ServerSecurity security = getServer(0).getSecurity();

      getServer(0).getOrCreateDatabase(OWNER_DB);
      getServer(0).getOrCreateDatabase(ATTACKER_DB);

      createUserIfMissing(security, OWNER_USER, OWNER_PASS, OWNER_DB);
      createUserIfMissing(security, ATTACKER_USER, ATTACKER_PASS, ATTACKER_DB);
      createUserIfMissing(security, NONOWNER_USER, NONOWNER_PASS, OWNER_DB);

      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();

      // Ensure a target type exists in the owner's database for the in-transaction inserts.
      final ExecuteCommandResponse createType = ownerStub().executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(OWNER_DB)
          .setCredentials(creds(OWNER_USER, OWNER_PASS))
          .setCommand("CREATE DOCUMENT TYPE Doc5040 IF NOT EXISTS")
          .build());
      assertThat(createType.getSuccess()).isTrue();
    }

    @AfterEach
    void teardown() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    private void createUserIfMissing(final ServerSecurity security, final String user, final String pass,
        final String db) {
      if (!security.existsUser(user)) {
        final JSONObject config = new JSONObject();
        config.put("name", user);
        config.put("password", security.encodePassword(pass));
        config.put("databases", new JSONObject().put(db, new JSONArray().put("admin")));
        security.createUser(config);
      }
    }

    private DatabaseCredentials creds(final String user, final String pass) {
      return DatabaseCredentials.newBuilder().setUsername(user).setPassword(pass).build();
    }

    /**
     * Builds a stub whose metadata carries {@code user}/{@code pass} basic-auth credentials and names
     * {@code headerDb} (a database the user is authorized for, so the auth interceptor lets the call
     * through and the service-layer authorization is what is under test).
     */
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub(final String user, final String pass,
        final String headerDb) {
      final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              headers.put(USER_HEADER, user);
              headers.put(PASSWORD_HEADER, pass);
              headers.put(DATABASE_HEADER, headerDb);
              super.start(responseListener, headers);
            }
          };
        }
      });
      return ArcadeDbServiceGrpc.newBlockingStub(authenticated);
    }

    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub ownerStub() {
      return stub(OWNER_USER, OWNER_PASS, OWNER_DB);
    }

    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub attackerStub() {
      return stub(ATTACKER_USER, ATTACKER_PASS, ATTACKER_DB);
    }

    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub nonOwnerStub() {
      return stub(NONOWNER_USER, NONOWNER_PASS, OWNER_DB);
    }

    private String beginOwnerTransaction() {
      final BeginTransactionResponse begin = ownerStub().beginTransaction(BeginTransactionRequest.newBuilder()
          .setDatabase(OWNER_DB)
          .setCredentials(creds(OWNER_USER, OWNER_PASS))
          .build());
      assertThat(begin.getTransactionId()).isNotEmpty();
      return begin.getTransactionId();
    }

    private CommitTransactionRequest commitAs(final String txId, final String user, final String pass) {
      return CommitTransactionRequest.newBuilder()
          .setCredentials(creds(user, pass))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(OWNER_DB).build())
          .build();
    }

    private RollbackTransactionRequest rollbackAs(final String txId, final String user, final String pass) {
      return RollbackTransactionRequest.newBuilder()
          .setCredentials(creds(user, pass))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(OWNER_DB).build())
          .build();
    }

    @Test
    void crossTenantCommitIsDeniedAndOwnerStillSucceeds() {
      final String txId = beginOwnerTransaction();

      // Attacker (authorized only for ATTACKER_DB) guesses the owner's tx id and tries to commit it.
      final CommitTransactionRequest attackerCommit = commitAs(txId, ATTACKER_USER, ATTACKER_PASS);
      assertThatThrownBy(() -> attackerStub().commitTransaction(attackerCommit))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");

      // The owner's transaction must be untouched: the owner can still commit it.
      final CommitTransactionResponse ownerCommit = ownerStub().commitTransaction(commitAs(txId, OWNER_USER, OWNER_PASS));
      assertThat(ownerCommit.getSuccess()).isTrue();
      assertThat(ownerCommit.getCommitted()).isTrue();
    }

    @Test
    void crossTenantRollbackIsDeniedAndOwnerStillSucceeds() {
      final String txId = beginOwnerTransaction();

      final RollbackTransactionRequest attackerRollback = rollbackAs(txId, ATTACKER_USER, ATTACKER_PASS);
      assertThatThrownBy(() -> attackerStub().rollbackTransaction(attackerRollback))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");

      // Owner's transaction is intact and can be rolled back by the owner.
      final RollbackTransactionResponse ownerRollback =
          ownerStub().rollbackTransaction(rollbackAs(txId, OWNER_USER, OWNER_PASS));
      assertThat(ownerRollback.getSuccess()).isTrue();
      assertThat(ownerRollback.getRolledBack()).isTrue();
    }

    @Test
    void crossTenantExecuteCommandIsDenied() {
      final String txId = beginOwnerTransaction();

      // executeCommand now surfaces execution/authorization errors as a gRPC error status through
      // GrpcErrorMapper, the same as every other RPC (issue #6192). The denial must come from the
      // transaction-scoped authorization gate.
      final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
          .setDatabase(OWNER_DB)
          .setCredentials(creds(ATTACKER_USER, ATTACKER_PASS))
          .setCommand("INSERT INTO Doc5040 SET name = 'intruder'")
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .build();

      assertThatThrownBy(() -> attackerStub().executeCommand(request))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("has not access to database '" + OWNER_DB + "'");

      // Nothing was written into the owner's transaction; the owner can still roll back cleanly.
      final RollbackTransactionResponse ownerRollback =
          ownerStub().rollbackTransaction(rollbackAs(txId, OWNER_USER, OWNER_PASS));
      assertThat(ownerRollback.getRolledBack()).isTrue();
    }

    @Test
    void authorizedNonOwnerCannotDriveTransaction() {
      final String txId = beginOwnerTransaction();

      // nonOwner IS authorized for OWNER_DB but did not open this transaction: the ownership binding must
      // still reject the commit, independently of the per-database authorization check.
      final CommitTransactionRequest nonOwnerCommit = commitAs(txId, NONOWNER_USER, NONOWNER_PASS);
      assertThatThrownBy(() -> nonOwnerStub().commitTransaction(nonOwnerCommit))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("owned by another user");

      // The owner can still commit its own transaction.
      final CommitTransactionResponse ownerCommit = ownerStub().commitTransaction(commitAs(txId, OWNER_USER, OWNER_PASS));
      assertThat(ownerCommit.getCommitted()).isTrue();
    }

    @Test
    void transactionIdsAreUuidBasedAndUnique() {
      final Set<String> ids = new HashSet<>();
      for (int i = 0; i < 5; i++) {
        final String txId = beginOwnerTransaction();
        assertThat(TX_ID_PATTERN.matcher(txId).matches())
            .as("transaction id must be UUID-based: %s", txId)
            .isTrue();
        ids.add(txId);
      }
      assertThat(ids).hasSize(5);

      // Clean up the open transactions.
      for (final String txId : ids)
        ownerStub().rollbackTransaction(rollbackAs(txId, OWNER_USER, OWNER_PASS));
    }
  }

  // Regression: #6709
  /**
   * End-to-end regression test for issue #6709: a concurrent commitTransaction/rollbackTransaction/idle-reap
   * can finalize a transaction (removing it from the server's active-transaction map and shutting down its
   * dedicated executor) in the window between a transaction-scoped RPC's resolve and its dispatched task
   * actually running on that executor. Unlike {@link GrpcTransactionIT.ReapedTransaction} (which covers the transaction
   * already being gone at resolve time), this drives the RPC through the finalize-races-dispatch window itself
   * and asserts the client still sees {@code FAILED_PRECONDITION}, not an opaque {@code INTERNAL}.
   *
   * <p>The race is made deterministic (no real timing dependency on outcome) by occupying the transaction's
   * single dedicated executor thread with a synthetic blocking task before the RPC's own task can run, then
   * finalizing the transaction (via {@link ArcadeDbGrpcService#finalizeTransactionForTesting}) while the RPC's
   * task is still queued behind it, and only then releasing the blocker.
   */
  @Nested
  class FinalizeRace {
    private static final Metadata.Key<String> USER_HEADER     =
        Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER =
        Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER =
        Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

    @AfterEach
    void clearServerPlugins() {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
      authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    }

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    private class AuthClientInterceptor implements ClientInterceptor {
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

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    private ArcadeDbGrpcService grpcService() {
      for (final ServerPlugin plugin : getServer(0).getPlugins()) {
        if (plugin instanceof GrpcServerPlugin grpcPlugin)
          return grpcPlugin.getService();
      }
      throw new IllegalStateException("GrpcServerPlugin not found");
    }

    /**
     * Begins a transaction, occupies its dedicated executor with a blocking task, runs {@code rpcCall} (given
     * the transaction id) on a background thread so it queues behind the blocker, finalizes the transaction
     * while that task is still queued, then releases the blocker so the RPC's task runs against the
     * now-finalized transaction. Returns the {@link StatusRuntimeException} the client observed.
     */
    private StatusRuntimeException raceFinalizeAgainstDispatch(final Function<String, StatusRuntimeException> rpcCall)
        throws Exception {
      final ArcadeDbGrpcService service = grpcService();

      final BeginTransactionResponse begin = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .build());
      final String txId = begin.getTransactionId();
      assertThat(txId).isNotEmpty();

      final var txCtx = service.getActiveTransactionForTesting(txId);
      assertThat(txCtx).as("beginTransaction must register the transaction").isNotNull();

      final CountDownLatch blockerRunning = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      // Occupies the transaction's single dedicated thread so the RPC's own dispatched task queues behind it,
      // giving a deterministic window to finalize the transaction before that task actually runs.
      txCtx.executor.submit(() -> {
        blockerRunning.countDown();
        release.await();
        return null;
      });
      assertThat(blockerRunning.await(5, TimeUnit.SECONDS)).as("blocker must start").isTrue();

      final ExecutorService clientExecutor = Executors.newSingleThreadExecutor();
      // Tracks whether the slot below was already released on the success path, so the finally block releases
      // it exactly once regardless of which path exits (cycle-4 review: the queue-wait or finalize call itself
      // could throw before the old inline release ran, leaking the reservation on that exit path too).
      boolean slotReleased = false;
      try {
        final Future<StatusRuntimeException> rpcResult = clientExecutor.submit(() -> rpcCall.apply(txId));

        // Deterministically wait for the RPC's own task to actually be enqueued behind the blocker, rather
        // than a fixed sleep: since the blocker occupies the executor's single core thread, a second submitted
        // task can only land in the queue, so a non-empty queue proves resolveAuthorizedTransaction already
        // succeeded and submitToActiveTransaction was called - i.e. that finalizing now genuinely races the
        // dispatched task's requireTransactionStillActive check, not the earlier isUnknownSuppliedTransaction
        // check at resolve time (cycle-2 review: a fixed sleep could pass without exercising that path at all).
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (txCtx.executor.getQueue().isEmpty()) {
          if (System.nanoTime() > deadlineNanos)
            throw new AssertionError("RPC task was never enqueued behind the blocker within 5s");
          Thread.sleep(5);
        }
        service.finalizeTransactionForTesting(txId);
        // finalizeTransactionForTesting only removes the map entry and shuts down the executor (correct for a
        // registerTransactionForTesting synthetic context, which never reserved a slot). This transaction is a
        // real one from beginTransaction, which did reserve a global/per-principal slot - release it here the
        // same way commitTransaction/rollbackTransaction/reapIdleTransactions do, or every race test in this
        // class leaks one reservation (CodeRabbit cycle-3 review).
        service.releaseTransactionSlot(txCtx.owner);
        slotReleased = true;
        release.countDown();

        return rpcResult.get(5, TimeUnit.SECONDS);
      } finally {
        // Idempotent: a no-op if the try block already finalized it (activeTransactions.remove returns null).
        service.finalizeTransactionForTesting(txId);
        if (!slotReleased)
          service.releaseTransactionSlot(txCtx.owner);
        release.countDown();
        clientExecutor.shutdownNow();
      }
    }

    @Test
    void lookupByRidFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
      final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
          catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.lookupByRid(
              LookupByRidRequest.newBuilder()
                  .setDatabase(getDatabaseName())
                  .setCredentials(credentials())
                  .setRid("#1:0")
                  .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                  .build())));

      assertThat(err).as("lookupByRid must reject the finalize-race, not hang or succeed").isNotNull();
      assertThat(err.getStatus().getCode())
          .as("lookupByRid must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
              + "finalized while its dispatched task is queued")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void updateRecordFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
      final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
          catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.updateRecord(
              UpdateRecordRequest.newBuilder()
                  .setDatabase(getDatabaseName())
                  .setCredentials(credentials())
                  .setRid("#1:0")
                  .setPartial(PropertiesUpdate.newBuilder()
                      .putProperties("name", GrpcValue.newBuilder().setStringValue("raced-update").build()).build())
                  .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                  .build())));

      assertThat(err).as("updateRecord must reject the finalize-race, not hang or succeed").isNotNull();
      assertThat(err.getStatus().getCode())
          .as("updateRecord must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
              + "finalized while its dispatched task is queued")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void deleteRecordFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
      final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
          catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.deleteRecord(
              DeleteRecordRequest.newBuilder()
                  .setDatabase(getDatabaseName())
                  .setCredentials(credentials())
                  .setRid("#1:0")
                  .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                  .build())));

      assertThat(err).as("deleteRecord must reject the finalize-race, not hang or succeed").isNotNull();
      assertThat(err.getStatus().getCode())
          .as("deleteRecord must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
              + "finalized while its dispatched task is queued")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void executeCommandFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
      final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
          catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.executeCommand(
              ExecuteCommandRequest.newBuilder()
                  .setDatabase(getDatabaseName())
                  .setCredentials(credentials())
                  .setCommand("INSERT INTO Person SET name = 'raced-executeCommand'")
                  .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                  .build())));

      assertThat(err).as("executeCommand must reject the finalize-race, not hang or succeed").isNotNull();
      assertThat(err.getStatus().getCode())
          .as("executeCommand must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
              + "finalized while its dispatched task is queued")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void bulkInsertFailsPreconditionWhenFinalizeRacesDispatch() throws Exception {
      final GrpcRecord record = GrpcRecord.newBuilder()
          .setType("Person")
          .putProperties("name", GrpcValue.newBuilder().setStringValue("raced-bulkInsert").build())
          .build();

      final StatusRuntimeException err = raceFinalizeAgainstDispatch(txId ->
          catchThrowableOfType(StatusRuntimeException.class, () -> authenticatedStub.bulkInsert(
              BulkInsertRequest.newBuilder()
                  .setOptions(InsertOptions.newBuilder()
                      .setDatabase(getDatabaseName())
                      .setCredentials(credentials())
                      .setTargetClass("Person")
                      .setTransactionMode(InsertOptions.TransactionMode.PER_BATCH)
                      .build())
                  .addRows(record)
                  .setCredentials(credentials())
                  .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
                  .build())));

      assertThat(err).as("bulkInsert must reject the finalize-race, not hang or succeed").isNotNull();
      assertThat(err.getStatus().getCode())
          .as("bulkInsert must surface FAILED_PRECONDITION, not an opaque INTERNAL, when the transaction is "
              + "finalized while its dispatched task is queued")
          .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }
  }

  // Regression: #5042
  /**
   * Regression test for issue #5042: reaped/unknown gRPC transactions must not cause silent data loss or
   * partial commits.
   *
   * <p>Covers the server-side guarantees:
   * <ul>
   *   <li>Write RPCs ({@code executeCommand}, {@code createRecord}, {@code updateRecord},
   *       {@code deleteRecord}) reject a non-blank-but-unknown transaction id with
   *       {@code FAILED_PRECONDITION} instead of silently falling through to a per-call auto-commit.</li>
   *   <li>The {@code lookupByRid} read RPC applies the same rejection: a non-blank-but-unknown transaction id
   *       fails with {@code FAILED_PRECONDITION} rather than silently serving an out-of-transaction read.</li>
   *   <li>A genuinely absent/blank transaction id still takes the auto-transaction path.</li>
   *   <li>{@code streamQuery} honors the supplied transaction id and runs inside the transaction, so a
   *       streamed read sees the transaction's own uncommitted writes.</li>
   *   <li>{@code streamQuery} rejects a non-blank-but-unknown transaction id with {@code FAILED_PRECONDITION}
   *       instead of silently running as a throwaway read.</li>
   * </ul>
   */
  @Nested
  class ReapedTransaction {
    private static final Metadata.Key<String> USER_HEADER     =
        Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER =
        Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER =
        Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

    @AfterEach
    void clearServerPlugins() {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
      authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
    }

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    private class AuthClientInterceptor implements ClientInterceptor {
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

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }

    @Test
    void writeRpcsRejectUnknownNonBlankTransactionId() {
      // A non-blank transaction id the server never registered (the reaped/unknown case).
      final String unknownTxId = "reaped-" + UUID.randomUUID();
      final TransactionContext unknownTx = TransactionContext.newBuilder()
          .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

      // executeCommand
      final StatusRuntimeException cmdErr = catchThrowableOfType(StatusRuntimeException.class, () ->
          authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setCommand("INSERT INTO Person SET name = 'reaped-write'")
              .setTransaction(unknownTx)
              .build()));
      assertThat(cmdErr).as("executeCommand must reject an unknown non-blank txId").isNotNull();
      assertThat(cmdErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

      // createRecord
      final StatusRuntimeException createErr = catchThrowableOfType(StatusRuntimeException.class, () ->
          authenticatedStub.createRecord(CreateRecordRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setType("Person")
              .setRecord(GrpcRecord.newBuilder()
                  .putProperties("name", GrpcValue.newBuilder().setStringValue("reaped-create").build()).build())
              .setTransaction(unknownTx)
              .build()));
      assertThat(createErr).as("createRecord must reject an unknown non-blank txId").isNotNull();
      assertThat(createErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

      // updateRecord
      final StatusRuntimeException updateErr = catchThrowableOfType(StatusRuntimeException.class, () ->
          authenticatedStub.updateRecord(UpdateRecordRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setRid("#1:0")
              .setPartial(PropertiesUpdate.newBuilder()
                  .putProperties("name", GrpcValue.newBuilder().setStringValue("reaped-update").build()).build())
              .setTransaction(unknownTx)
              .build()));
      assertThat(updateErr).as("updateRecord must reject an unknown non-blank txId").isNotNull();
      assertThat(updateErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

      // deleteRecord
      final StatusRuntimeException deleteErr = catchThrowableOfType(StatusRuntimeException.class, () ->
          authenticatedStub.deleteRecord(DeleteRecordRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setRid("#1:0")
              .setTransaction(unknownTx)
              .build()));
      assertThat(deleteErr).as("deleteRecord must reject an unknown non-blank txId").isNotNull();
      assertThat(deleteErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);

      // Nothing must have been persisted by any of the rejected writes.
      final ExecuteQueryResponse q = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT FROM Person WHERE name IN ['reaped-write','reaped-create','reaped-update']")
          .build());
      assertThat(q.getResultsList().get(0).getRecordsList()).isEmpty();
    }

    @Test
    void lookupByRidRejectsUnknownNonBlankTransactionId() {
      // A non-blank transaction id the server never registered (the reaped/unknown case).
      final String unknownTxId = "reaped-" + UUID.randomUUID();
      final TransactionContext unknownTx = TransactionContext.newBuilder()
          .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

      final StatusRuntimeException lookupErr = catchThrowableOfType(StatusRuntimeException.class, () ->
          authenticatedStub.lookupByRid(LookupByRidRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCredentials(credentials())
              .setRid("#1:0")
              .setTransaction(unknownTx)
              .build()));
      assertThat(lookupErr).as("lookupByRid must reject an unknown non-blank txId").isNotNull();
      assertThat(lookupErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void streamQueryRejectsUnknownNonBlankTransactionId() {
      // A non-blank transaction id the server never registered (the reaped/unknown case).
      final String unknownTxId = "reaped-" + UUID.randomUUID();
      final TransactionContext unknownTx = TransactionContext.newBuilder()
          .setTransactionId(unknownTxId).setDatabase(getDatabaseName()).build();

      // The streamed read must fail loudly instead of silently running as a throwaway read. The error surfaces
      // when the stream is consumed, so drive the iterator inside the assertion.
      final StatusRuntimeException streamErr = catchThrowableOfType(StatusRuntimeException.class, () -> {
        final Iterator<QueryResult> it = authenticatedStub.streamQuery(StreamQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT FROM Person")
            .setBatchSize(10)
            .setRetrievalMode(StreamQueryRequest.RetrievalMode.CURSOR)
            .setTransaction(unknownTx)
            .build());
        while (it.hasNext())
          it.next();
      });
      assertThat(streamErr).as("streamQuery must reject an unknown non-blank txId").isNotNull();
      assertThat(streamErr.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void blankTransactionIdStillAutoCommits() {
      final String name = "auto-commit-" + UUID.randomUUID();

      // No transaction supplied at all: the write must auto-commit (genuinely absent txId path).
      final ExecuteCommandResponse resp = authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setCommand("INSERT INTO Person SET name = '" + name + "'")
          .build());
      assertThat(resp.getSuccess()).isTrue();

      final ExecuteQueryResponse q = authenticatedStub.executeQuery(ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .setQuery("SELECT FROM Person WHERE name = '" + name + "'")
          .build());
      assertThat(q.getResultsList().get(0).getRecordsList())
          .as("blank/absent txId must still auto-commit the write").hasSize(1);
    }

    @Test
    void streamQueryHonorsTransactionIdAndSeesUncommittedWrites() {
      // Begin an externally-managed transaction.
      final BeginTransactionResponse begin = authenticatedStub.beginTransaction(BeginTransactionRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials())
          .build());
      final String txId = begin.getTransactionId();
      assertThat(txId).isNotEmpty();

      final TransactionContext txCtx = TransactionContext.newBuilder()
          .setTransactionId(txId).setDatabase(getDatabaseName()).build();

      try {
        final String name = "stream-tx-" + txId;

        // Write inside the transaction but DO NOT commit.
        authenticatedStub.executeCommand(ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand("INSERT INTO Person SET name = '" + name + "'")
            .setTransaction(txCtx)
            .build());

        // A streamed query bound to the same transaction must see the uncommitted write.
        final Iterator<QueryResult> it = authenticatedStub.streamQuery(StreamQueryRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setQuery("SELECT FROM Person WHERE name = '" + name + "'")
            .setBatchSize(10)
            .setRetrievalMode(StreamQueryRequest.RetrievalMode.CURSOR)
            .setTransaction(txCtx)
            .build());

        int seen = 0;
        while (it.hasNext())
          seen += it.next().getRecordsList().size();

        assertThat(seen).as("streamed query inside the transaction must see its uncommitted write").isEqualTo(1);
      } finally {
        authenticatedStub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
            .setCredentials(credentials())
            .setTransaction(txCtx)
            .build());
      }
    }
  }

  // Regression: #10386
  /**
   * Issue #10386: {@code beginTransaction} accepted {@code SERIALIZABLE} and answered success at {@code REPEATABLE_READ}, the
   * one failure a client cannot detect: it asked for write-skew prevention and was told its transaction started. HTTP, SQL,
   * openCypher and Postgres all refuse a level the engine cannot provide, so gRPC does too, with {@code INVALID_ARGUMENT} and
   * the message the other surfaces use. A value this server does not know - a newer client's enum constant - is refused for
   * the same reason, where it used to begin silently at {@code READ_COMMITTED}.
   * {@code READ_UNCOMMITTED} stays accepted: it is the proto default, every client that leaves the field unset sends it, and
   * the engine gives it a STRONGER level than asked for, which {@code effective_isolation} reports (issue #10322).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnsupportedIsolation {
    private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void setupGrpcClient() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()));
    }

    @AfterEach
    void teardownGrpcClient() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void aSerializableBeginIsRefusedWithInvalidArgument() {
      assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolation(TransactionIsolation.SERIALIZABLE).build()))
          .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            // The words the other surfaces use: what was asked for, and what the engine provides
            assertThat(e.getStatus().getDescription()).contains("'SERIALIZABLE'").contains("READ_COMMITTED").contains("REPEATABLE_READ");
          });
    }

    @Test
    void aSerializableBeginLeavesNoTransactionBehind() {
      final int before = grpcService().getActiveTransactionCount();

      assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolation(TransactionIsolation.SERIALIZABLE).build()))
          .isInstanceOf(StatusRuntimeException.class);

      assertThat(grpcService().getActiveTransactionCount()).as("a refused begin holds no transaction slot").isEqualTo(before);
    }

    @Test
    void anIsolationThisServerDoesNotKnowIsRefusedToo() {
      // A newer client's enum constant parses as UNRECOGNIZED: starting it at READ_COMMITTED would be the same silent downgrade
      assertThatThrownBy(() -> stub.beginTransaction(begin().setIsolationValue(99).build()))
          .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(e.getStatus().getDescription()).contains("99").contains("READ_COMMITTED").contains("REPEATABLE_READ");
          });
    }

    @Test
    void theLevelsTheEngineProvidesStillBeginAndReportTheirOwnLevel() {
      assertThat(beginAndRollback(TransactionIsolation.READ_COMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);
      assertThat(beginAndRollback(TransactionIsolation.REPEATABLE_READ)).isEqualTo(TransactionIsolation.REPEATABLE_READ);
    }

    @Test
    void readUncommittedIsTheProtoDefaultAndRunsAtTheStrongerReadCommitted() {
      assertThat(beginAndRollback(TransactionIsolation.READ_UNCOMMITTED)).isEqualTo(TransactionIsolation.READ_COMMITTED);

      // An unset field is the same value, so a client that never heard of isolation keeps beginning
      final BeginTransactionResponse begun = stub.beginTransaction(begin().build());
      try {
        assertThat(begun.getEffectiveIsolation()).isEqualTo(TransactionIsolation.READ_COMMITTED);
      } finally {
        rollback(begun.getTransactionId());
      }
    }

    private ArcadeDbGrpcService grpcService() {
      for (final ServerPlugin plugin : getServer(0).getPlugins())
        if (plugin instanceof GrpcServerPlugin grpcPlugin)
          return grpcPlugin.getService();
      throw new IllegalStateException("GrpcServerPlugin not found");
    }

    private BeginTransactionRequest.Builder begin() {
      return BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials());
    }

    private TransactionIsolation beginAndRollback(final TransactionIsolation requested) {
      final BeginTransactionResponse begun = stub.beginTransaction(begin().setIsolation(requested).build());
      rollback(begun.getTransactionId());
      return begun.getEffectiveIsolation();
    }

    private void rollback(final String txId) {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setCredentials(credentials())
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build())
          .build());
    }

    private class AuthClientInterceptor implements ClientInterceptor {
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

    private DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }
  }

  // Regression: #11588
  /**
   * Issue #11588 over gRPC: CALL { ... } IN TRANSACTIONS keeps running in batches in the transaction
   * {@code executeCommandWithRetries} opens around an auto-commit ExecuteCommand, an implicit transaction, and is refused
   * inside a transaction the client opened with BeginTransaction, whose rollback could not undo the batches.
   */
  @Nested
  class CallInTransactions {
    private static final String STATEMENT = "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

    private ManagedChannel                                  channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void openChannel() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      stub = ArcadeDbServiceGrpc.newBlockingStub(
          ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void runsInBatchesInTheAutoCommitTransactionOfExecuteCommand() {
      final String label = "GrpcAutoCommit11588";
      final ExecuteCommandResponse response = stub.executeCommand(request(STATEMENT.formatted(label)).build());

      assertThat(response.getSuccess()).isTrue();
      assertThat(count(label)).isEqualTo(25);
    }

    @Test
    void refusedInTheTransactionTheClientBegan() {
      final String label = "GrpcClientTx11588";
      final String txId = stub.beginTransaction(
          BeginTransactionRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).build()).getTransactionId();
      final TransactionContext tx = TransactionContext.newBuilder().setTransactionId(txId).setDatabase(getDatabaseName()).build();

      assertThatThrownBy(() -> stub.executeCommand(request(STATEMENT.formatted(label)).setTransaction(tx).build()))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("can only be executed in an implicit transaction");

      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder().setTransaction(tx).setCredentials(credentials()).build());
      assertThat(count(label)).isZero();
    }

    private long count(final String label) {
      return getServerDatabase(0, getDatabaseName()).query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next()
          .<Number>getProperty("c").longValue();
    }

    private ExecuteCommandRequest.Builder request(final String command) {
      return ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setLanguage("opencypher").setCommand(command)
          .setCredentials(credentials());
    }

    private static DatabaseCredentials credentials() {
      return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
    }
  }

  // Regression: #12202
  /**
   * Issue #12202 (review of PR #12230): an ExecuteCommand whose transaction message names no registered transaction and does not set
   * {@code begin} runs in no transaction of the client, so a standalone transaction control is refused there too. It ran without the
   * auto-commit wrapper and was answered OK.
   */
  @Nested
  class TransactionMessageWithoutBegin {
    private ManagedChannel                              channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void open() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions, final Channel next) {
          return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
              headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
              headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
              super.start(responseListener, headers);
            }
          };
        }
      }));
    }

    @AfterEach
    void close() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @ParameterizedTest
    @CsvSource({ "sql,BEGIN,false,false", "sql,ROLLBACK,false,true", "sql,COMMIT,true,false", "opencypher,START TRANSACTION,false,false" })
    void aTransactionControlUnderATransactionMessageWithoutBeginIsRefused(final String language, final String command,
        final boolean commit, final boolean rollback) {
      final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .setLanguage(language)
          .setCommand(command)
          .setTransaction(TransactionContext.newBuilder().setCommit(commit).setRollback(rollback).build())
          .build();

      assertThatThrownBy(() -> stub.executeCommand(request)).isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT))
          .hasMessageContaining("RemoteGrpcDatabase.begin()");
    }
  }
}

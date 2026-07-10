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
import com.arcadedb.server.BaseGraphServerTest;
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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
public class Issue5040GrpcTransactionHijackIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT     = 50051;
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

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setup() {
    final ServerSecurity security = getServer(0).getSecurity();

    getServer(0).getOrCreateDatabase(OWNER_DB);
    getServer(0).getOrCreateDatabase(ATTACKER_DB);

    createUserIfMissing(security, OWNER_USER, OWNER_PASS, OWNER_DB);
    createUserIfMissing(security, ATTACKER_USER, ATTACKER_PASS, ATTACKER_DB);
    createUserIfMissing(security, NONOWNER_USER, NONOWNER_PASS, OWNER_DB);

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();

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

    // executeCommand surfaces execution/authorization errors in-band (success=false) rather than a gRPC
    // error status. The denial must come from the transaction-scoped authorization gate.
    final ExecuteCommandResponse response = attackerStub().executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(OWNER_DB)
        .setCredentials(creds(ATTACKER_USER, ATTACKER_PASS))
        .setCommand("INSERT INTO Doc5040 SET name = 'intruder'")
        .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
        .build());

    assertThat(response.getSuccess()).isFalse();
    assertThat(response.getMessage()).contains("has not access to database '" + OWNER_DB + "'");

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

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
package com.arcadedb.server.security;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.CommitTransactionRequest;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.TransactionContext;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-p29f-345w-4qwf: the gRPC external-transaction command path let a read-only user escalate to
 * server-wide administrator.
 * <p>
 * {@code beginTransaction} allocates a dedicated single-thread executor for the transaction and begins the transaction
 * on it, but the submitted lambda only called {@code DatabaseContext.INSTANCE.init(...)} - it never bound the
 * authenticated principal. Every subsequent transaction-scoped RPC ({@code executeCommand}, {@code executeQuery},
 * {@code createRecord}, ...) is dispatched onto that same thread, where {@code getCurrentUser()} was null. The engine
 * permission gates are deliberate no-ops when no user is bound, so both the polyglot scripting gate and the
 * record-level gates silently passed and {@code LANGUAGE js} reached {@code database.getSecurity().createUser(...)}.
 * <p>
 * This is the same class of defect as GHSA-5j4x-3jfw-8xv3 (HTTP async worker) and GHSA-6x73-v3rc-f57c (MCP), on a third
 * transport. The fix binds the principal once on the transaction's dedicated thread at begin time, so it covers every
 * RPC later dispatched onto it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GrpcTransactionScriptingAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final String READER_USER = "grpc-tx-reader";
  private static final String READER_PWD  = "readerpass1";

  // The published proof-of-concept: escalate to a server-wide admin from a JS script run inside an external transaction.
  private static final String PWN_USER    = "pwnp29f";
  private static final String PoC_SCRIPT  = "database.getSecurity().createUser('" + PWN_USER + "', 'pwn'); true";

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
  void setupReaderAndChannel() {
    final ServerSecurity security = getServer(0).getSecurity();

    // A group that grants record reads but no database-level (admin) permission.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("grpcTxReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    security.createUser(new JSONObject()
        .put("name", READER_USER)
        .put("password", security.encodePassword(READER_PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("grpcTxReader"))));

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
  }

  @AfterEach
  void teardown() throws InterruptedException {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(PWN_USER))
      security.dropUser(PWN_USER);
    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void readerCannotEscalateViaJsInsideExternalTransaction() {
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubAs(READER_USER, READER_PWD);
    final ServerSecurity security = getServer(0).getSecurity();

    // Step 1 of the PoC: any authenticated user may open a transaction; ownership is bound to the reader.
    final BeginTransactionResponse tx = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials(READER_USER, READER_PWD))
        .build());
    assertThat(tx.getTransactionId()).as("reader can open an external transaction").isNotEmpty();

    try {
      // Step 2: run the escalation script on the transaction's dedicated thread.
      final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials(READER_USER, READER_PWD))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .setLanguage("js")
          .setCommand(PoC_SCRIPT)
          .build());

      assertThat(response.getSuccess())
          .as("scripting gate must reject LANGUAGE js for a reader inside an external transaction").isFalse();
    } finally {
      rollbackQuietly(stub, tx.getTransactionId());
    }

    // The escalation must NOT have happened.
    assertThat(security.existsUser(PWN_USER))
        .as("reader must NOT be able to create a server admin via js inside a gRPC transaction").isFalse();
  }

  @Test
  void readerCannotWriteViaSqlInsideExternalTransaction() {
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubAs(READER_USER, READER_PWD);
    final String marker = "p29f-sql-marker";

    final BeginTransactionResponse tx = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials(READER_USER, READER_PWD))
        .build());

    try {
      // The record-level gate is the same null-user no-op, so an ordinary SQL write inside the transaction must be
      // denied too - the escalation above is only the most severe consequence of the missing binding.
      final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials(READER_USER, READER_PWD))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .setCommand("INSERT INTO " + VERTEX1_TYPE_NAME + " SET tag = '" + marker + "'")
          .build());

      assertThat(response.getSuccess()).as("reader must be denied a write inside an external transaction").isFalse();
    } finally {
      rollbackQuietly(stub, tx.getTransactionId());
    }

    assertThat(countMarker(marker)).as("no record must have been inserted by the reader").isZero();
  }

  @Test
  void adminRetainsFullAccessInsideExternalTransaction() {
    // Positive control: binding the principal must not break the legitimate privileged path. Without this, a fix that
    // simply denied everything on the transaction thread would pass the two tests above.
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubAs("root", DEFAULT_PASSWORD_FOR_TESTS);
    final String marker = "p29f-admin-marker";

    final BeginTransactionResponse tx = stub.beginTransaction(BeginTransactionRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials("root", DEFAULT_PASSWORD_FOR_TESTS))
        .build());

    try {
      final ExecuteCommandResponse response = stub.executeCommand(ExecuteCommandRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(credentials("root", DEFAULT_PASSWORD_FOR_TESTS))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .setCommand("INSERT INTO " + VERTEX1_TYPE_NAME + " SET tag = '" + marker + "'")
          .build());

      assertThat(response.getSuccess()).as("root must still be able to write inside an external transaction").isTrue();

      stub.commitTransaction(CommitTransactionRequest.newBuilder()
          .setCredentials(credentials("root", DEFAULT_PASSWORD_FOR_TESTS))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(tx.getTransactionId()).build())
          .build());

      assertThat(countMarker(marker)).as("the committed admin write is visible").isEqualTo(1L);
    } finally {
      getServer(0).getDatabase(getDatabaseName())
          .command("sql", "DELETE FROM " + VERTEX1_TYPE_NAME + " WHERE tag = ?", marker);
    }
  }

  private void rollbackQuietly(final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub, final String txId) {
    try {
      stub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
          .setCredentials(credentials(READER_USER, READER_PWD))
          .setTransaction(TransactionContext.newBuilder().setTransactionId(txId).build())
          .build());
    } catch (final Exception e) {
      // The transaction may already be gone; the assertions below do not depend on the rollback succeeding.
    }
  }

  /**
   * The gRPC auth interceptor authenticates from request metadata, so every stub must carry the caller's credentials in
   * the headers as well as in the request payload.
   */
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stubAs(final String user, final String password) {
    final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, user);
            headers.put(PASSWORD_HEADER, password);
            headers.put(DATABASE_HEADER, getDatabaseName());
            super.start(responseListener, headers);
          }
        };
      }
    });
    return ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  private long countMarker(final String marker) {
    return getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) AS c FROM " + VERTEX1_TYPE_NAME + " WHERE tag = ?", marker)
        .next().<Number>getProperty("c").longValue();
  }

  private DatabaseCredentials credentials(final String user, final String password) {
    return DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build();
  }
}

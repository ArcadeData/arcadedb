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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4794.
 * <p>
 * The gRPC auth interceptor authenticates the caller against the database named in the
 * {@code x-arcade-database} metadata header, but the service methods operate on the database named
 * in the request body. Without per-database authorization at the body-database resolution point a
 * caller can authenticate with a header naming a database it may access and then read/write a
 * different database it is NOT authorized for.
 * <p>
 * This test creates a user authorized only for a dedicated database, then issues operations whose
 * request body targets a different (forbidden) database while the header still names the allowed
 * one. The forbidden operations must be rejected with {@code PERMISSION_DENIED}.
 * <p>
 * The read ({@code executeQuery}), command ({@code executeCommand}) and write ({@code createRecord})
 * cases below are representative, not exhaustive: every database-touching RPC resolves its target
 * database through the single {@code ArcadeDbGrpcService.getDatabase(...)} chokepoint, where
 * {@code validateCredentials(...)} authorizes the request-body database. The untested RPCs
 * (e.g. {@code lookupByRid}, {@code updateRecord}, {@code deleteRecord}, {@code bulkInsert}) are not
 * intentionally excluded; they share that same gate and so are covered transitively.
 */
public class Issue4794GrpcPerDbAuthorizationIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String ALLOWED_DB      = "allowed4794db";
  private static final String LIMITED_USER    = "limited4794";
  private static final String LIMITED_PASS    = "limited4794pass";

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
  void setupRestrictedUserAndChannel() {
    final ServerSecurity security = getServer(0).getSecurity();

    // The database the limited user IS authorized for.
    getServer(0).getOrCreateDatabase(ALLOWED_DB);

    // Create a user authorized ONLY for ALLOWED_DB (no wildcard, no access to the default test db).
    if (!security.existsUser(LIMITED_USER)) {
      final JSONObject config = new JSONObject();
      config.put("name", LIMITED_USER);
      config.put("password", security.encodePassword(LIMITED_PASS));
      config.put("databases", new JSONObject().put(ALLOWED_DB, new JSONArray().put("admin")));
      security.createUser(config);
    }

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
  }

  @AfterEach
  void teardown() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Builds a stub whose metadata header names {@code headerDb} but that otherwise carries the
   * limited user's basic-auth credentials.
   */
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stubWithHeaderDatabase(final String headerDb) {
    final Channel authenticated = ClientInterceptors.intercept(channel, new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
          final CallOptions callOptions, final Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
          @Override
          public void start(final Listener<RespT> responseListener, final Metadata headers) {
            headers.put(USER_HEADER, LIMITED_USER);
            headers.put(PASSWORD_HEADER, LIMITED_PASS);
            headers.put(DATABASE_HEADER, headerDb);
            super.start(responseListener, headers);
          }
        };
      }
    });
    return ArcadeDbServiceGrpc.newBlockingStub(authenticated);
  }

  private DatabaseCredentials limitedCredentials() {
    return DatabaseCredentials.newBuilder().setUsername(LIMITED_USER).setPassword(LIMITED_PASS).build();
  }

  @Test
  void crossDatabaseQueryIsDenied() {
    // Header names the database the user CAN access; request body targets the default test database
    // (getDatabaseName()) which the limited user is NOT authorized for. The query RPC surfaces the
    // failure as a gRPC error status, and the denial must come from the database-authorization gate
    // (not, e.g., a leaked record-level error) so the bypass is closed for every operation.
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

    final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(limitedCredentials())
        .setQuery("SELECT FROM V1")
        .build();

    assertThatThrownBy(() -> stub.executeQuery(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("has not access to database '" + getDatabaseName() + "'");
  }

  @Test
  void crossDatabaseCommandIsDenied() {
    // The command RPC reports execution errors via the response (success=false) rather than a gRPC
    // error status. The denial must be raised by the per-database authorization gate before any work
    // happens against the request-body database.
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(limitedCredentials())
        .setCommand("INSERT INTO Person SET name = 'intruder'")
        .build();

    final ExecuteCommandResponse response = stub.executeCommand(request);

    assertThat(response.getSuccess()).isFalse();
    assertThat(response.getMessage()).contains("has not access to database '" + getDatabaseName() + "'");
    assertThat(response.getAffectedRecords()).isZero();
  }

  @Test
  void crossDatabaseCreateRecordIsDenied() {
    // Write path: createRecord resolves its target database from the request body through the same
    // getDatabase() authorization chokepoint. A header naming the allowed database must not grant a
    // write into a different body database the user cannot access. createRecord surfaces failures via
    // a gRPC error status (its catch block masks the PERMISSION_DENIED code as INTERNAL but preserves
    // the authorization message), so the denial is asserted on the message.
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

    final CreateRecordRequest request = CreateRecordRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(limitedCredentials())
        .setType("Person")
        .setRecord(GrpcRecord.newBuilder()
            .putProperties("name", GrpcValue.newBuilder().setStringValue("intruder").build())
            .build())
        .build();

    assertThatThrownBy(() -> stub.createRecord(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("has not access to database '" + getDatabaseName() + "'");
  }

  @Test
  void authorizedDatabaseAccessSucceeds() {
    // Positive control: same user, body database == the database it is authorized for.
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = stubWithHeaderDatabase(ALLOWED_DB);

    final ExecuteCommandRequest createType = ExecuteCommandRequest.newBuilder()
        .setDatabase(ALLOWED_DB)
        .setCredentials(limitedCredentials())
        .setCommand("CREATE DOCUMENT TYPE Doc4794 IF NOT EXISTS")
        .build();

    final ExecuteCommandResponse response = stub.executeCommand(createType);
    assertThat(response.getSuccess()).isTrue();
  }
}

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
package com.arcadedb.remote.grpc;

import com.arcadedb.ContextConfiguration;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7374: a {@link RemoteGrpcDatabase} takes its own user, puts it in every request BODY, and
 * then issued every call on a stub whose METADATA carried the {@link RemoteGrpcServer}'s user
 * instead. The server prefers the metadata-authenticated principal, so the user the caller passed to
 * the database was silently discarded on gRPC while the HTTP half of the same object still used it.
 * <p>
 * These cases read the credentials back off the stubs {@link RemoteGrpcDatabase} actually builds,
 * with the two users deliberately different. No RPC is issued, so no server has to be listening: a
 * gRPC channel connects lazily.
 */
class Issue7374GrpcDatabaseCredentialsTest {

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private static final String DATABASE      = "graph7374";
  /**
   * A port nothing listens on. {@code RemoteDatabase}'s constructor asks the HTTP port for the cluster
   * configuration and falls back to a direct connection when it cannot reach it, so a refused connection
   * is the quiet path - whereas a real server answering on 2480 would refuse these invented credentials
   * and fail the construction. This test is about the metadata the client builds, not about any server.
   */
  private static final int    DEAD_HTTP_PORT = 59374;
  private static final String SERVER_USER   = "root";
  private static final String SERVER_PWD    = "rootsecret";
  private static final String DATABASE_USER = "scoped7374";
  private static final String DATABASE_PWD  = "scopedsecret";

  private RemoteGrpcServer server() {
    return new RemoteGrpcServer("localhost", 50051, SERVER_USER, SERVER_PWD, true, List.of());
  }

  private RemoteGrpcDatabase database(final RemoteGrpcServer server, final String user, final String password) {
    return new RemoteGrpcDatabase(server, "localhost", 50051, DEAD_HTTP_PORT, DATABASE, user, password,
        new ContextConfiguration());
  }

  /**
   * Runs the credentials the way gRPC runs them on every call and returns the metadata they produced.
   */
  private static Metadata requestMetadata(final AbstractStub<?> stub) {
    final CallCredentials credentials = stub.getCallOptions().getCredentials();
    assertThat(credentials).as("the stub carries call credentials").isNotNull();

    final AtomicReference<Metadata> captured = new AtomicReference<>();
    credentials.applyRequestMetadata(null, Runnable::run, new CallCredentials.MetadataApplier() {
      @Override
      public void apply(final Metadata headers) {
        captured.set(headers);
      }

      @Override
      public void fail(final Status status) {
        throw new AssertionError("credentials refused to apply: " + status);
      }
    });

    final Metadata headers = captured.get();
    assertThat(headers).as("credentials applied request metadata").isNotNull();
    return headers;
  }

  /**
   * The blocking stub, which carries every unary and server-streaming data-plane RPC.
   */
  @Test
  void blockingStubCarriesTheDatabaseUser() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabase database = database(server, DATABASE_USER, DATABASE_PWD)) {

      final Metadata headers = requestMetadata(database.createBlockingStub());

      assertThat(headers.get(USER_HEADER)).isEqualTo(DATABASE_USER);
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo(DATABASE_PWD);
      // #7320's fix must survive: the database is still named on the same metadata.
      assertThat(headers.get(DATABASE_HEADER)).isEqualTo(DATABASE);
    }
  }

  /**
   * The async stub, which carries bidirectional ingestion and the graph batch loader. It builds its
   * own {@link CallCredentials} instance, so it would keep sending the server's user if only the
   * blocking one had been fixed.
   */
  @Test
  void asyncStubCarriesTheDatabaseUser() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabase database = database(server, DATABASE_USER, DATABASE_PWD)) {

      final Metadata headers = requestMetadata(database.createAsyncStub());

      assertThat(headers.get(USER_HEADER)).isEqualTo(DATABASE_USER);
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo(DATABASE_PWD);
      assertThat(headers.get(DATABASE_HEADER)).isEqualTo(DATABASE);
    }
  }

  /**
   * {@code getProgress()} polls the admin plane on the shared channel. Its request body already
   * carried this database's credentials; the metadata did not.
   */
  @Test
  void adminStubCarriesTheDatabaseUser() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabase database = database(server, DATABASE_USER, DATABASE_PWD)) {

      final Metadata headers = requestMetadata(database.createAdminBlockingStub());

      assertThat(headers.get(USER_HEADER)).isEqualTo(DATABASE_USER);
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo(DATABASE_PWD);
      // The admin plane authenticates from the request body and reads no database key, so none is
      // sent - unchanged by this fix.
      assertThat(headers.containsKey(DATABASE_HEADER)).isFalse();
    }
  }

  /**
   * The body credentials and the metadata credentials must now name the same principal. That is the
   * property the issue is about, stated directly rather than through either half of it.
   */
  @Test
  void bodyAndMetadataNameTheSamePrincipal() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabase database = database(server, DATABASE_USER, DATABASE_PWD)) {

      final Metadata headers = requestMetadata(database.createBlockingStub());

      assertThat(headers.get(USER_HEADER)).isEqualTo(database.buildCredentials().getUsername());
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo(database.buildCredentials().getPassword());
    }
  }

  /**
   * The subclass overrides neither factory, so it inherits the fix. Asserted rather than assumed:
   * it is a public class a caller can pick instead.
   */
  @Test
  void compressionSubclassInheritsTheDatabaseUser() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabaseWithCompression database = new RemoteGrpcDatabaseWithCompression(server,
            "localhost", 50051, DEAD_HTTP_PORT, DATABASE, DATABASE_USER, DATABASE_PWD, new ContextConfiguration())) {

      assertThat(requestMetadata(database.createBlockingStub()).get(USER_HEADER)).isEqualTo(DATABASE_USER);
      assertThat(requestMetadata(database.createAsyncStub()).get(USER_HEADER)).isEqualTo(DATABASE_USER);
    }
  }

  /**
   * A database constructed with no user of its own keeps speaking as the server's account, which is
   * what it did before this fix. Without the fallback the client would put a null on the metadata and
   * fail with an NPE on the first call instead.
   */
  @Test
  void blankDatabaseUserFallsBackToTheServerUser() {
    try (final RemoteGrpcServer server = server();
        final RemoteGrpcDatabase database = database(server, null, null)) {

      final Metadata headers = requestMetadata(database.createBlockingStub());

      assertThat(headers.get(USER_HEADER)).isEqualTo(SERVER_USER);
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo(SERVER_PWD);
    }
  }

  /**
   * The pre-existing server-scoped overloads are untouched: they are how {@link RemoteGrpcServer}
   * issues its OWN calls, where its user is the principal.
   */
  @Test
  void serverScopedStubStillCarriesTheServerUser() {
    try (final RemoteGrpcServer server = server()) {
      assertThat(requestMetadata(server.newBlockingStub(1_000, DATABASE)).get(USER_HEADER))
          .isEqualTo(SERVER_USER);
      assertThat(requestMetadata(server.newAdminBlockingStub(1_000)).get(USER_HEADER))
          .isEqualTo(SERVER_USER);
    }
  }
}

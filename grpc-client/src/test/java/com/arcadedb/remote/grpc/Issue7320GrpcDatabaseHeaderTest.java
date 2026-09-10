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

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7320: {@code GrpcAuthInterceptor} authenticates a data-plane call against the database named
 * in the {@code x-arcade-database} metadata, and the shipped client never sent that key - so every
 * call was authenticated against the literal name {@code "default"}.
 * <p>
 * These cases read the credentials back off the stub that {@link RemoteGrpcServer} actually hands to
 * {@link RemoteGrpcDatabase}, rather than off a factory method, so the assertion covers the wiring and
 * not only the header-building code. No RPC is issued, so no server has to be listening: a gRPC
 * channel connects lazily.
 */
class Issue7320GrpcDatabaseHeaderTest {

  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);

  private static final String DATABASE = "graph7320";

  private RemoteGrpcServer server() {
    return new RemoteGrpcServer("localhost", 50051, "root", "secret", true, List.of());
  }

  /**
   * Runs the credentials the way gRPC runs them on every call and returns the metadata they produced.
   */
  private static Metadata requestMetadata(final CallCredentials credentials) {
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

  @Test
  void blockingStubCredentialsCarryTheTargetDatabase() {
    try (final RemoteGrpcServer server = server()) {
      final Metadata headers = requestMetadata(
          server.newBlockingStub(1_000, DATABASE).getCallOptions().getCredentials());

      assertThat(headers.get(DATABASE_HEADER)).isEqualTo(DATABASE);
      assertThat(headers.get(USER_HEADER)).isEqualTo("root");
      assertThat(headers.get(PASSWORD_HEADER)).isEqualTo("secret");
    }
  }

  @Test
  void asyncStubCredentialsCarryTheTargetDatabase() {
    try (final RemoteGrpcServer server = server()) {
      final Metadata headers = requestMetadata(
          server.newAsyncStub(1_000, DATABASE).getCallOptions().getCredentials());

      assertThat(headers.get(DATABASE_HEADER)).isEqualTo(DATABASE);
      assertThat(headers.get(USER_HEADER)).isEqualTo("root");
    }
  }

  /**
   * A database-less stub is still reachable through the pre-#7320 overloads. It sends no database key
   * rather than an empty or invented one, which is what lets the server authenticate it at server
   * level instead of against a name nobody asked for.
   */
  @Test
  void databaseLessStubSendsNoDatabaseKeyAtAll() {
    try (final RemoteGrpcServer server = server()) {
      assertThat(requestMetadata(server.newBlockingStub(1_000).getCallOptions().getCredentials())
          .containsKey(DATABASE_HEADER)).isFalse();
      assertThat(requestMetadata(server.newAsyncStub(1_000).getCallOptions().getCredentials())
          .containsKey(DATABASE_HEADER)).isFalse();
    }
  }

  /**
   * A blank database name is treated as no name at all, so the client cannot send an empty header that
   * the interceptor would have to special-case.
   */
  @Test
  void blankDatabaseNameSendsNoDatabaseKey() {
    try (final RemoteGrpcServer server = server()) {
      assertThat(requestMetadata(server.newBlockingStub(1_000, "  ").getCallOptions().getCredentials())
          .containsKey(DATABASE_HEADER)).isFalse();
    }
  }
}

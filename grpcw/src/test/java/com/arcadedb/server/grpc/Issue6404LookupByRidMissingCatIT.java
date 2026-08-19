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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6404: {@code RemoteGrpcDatabase.lookupByRID()} threw {@code RecordNotFoundException}
 * for an existing, previously persisted vertex, even though the same RID was returned by a SQL query on the same
 * connection and was loadable through the HTTP client.
 * <p>
 * Root cause, at the wire level pinned here: {@link ArcadeDbGrpcService#convertToGrpcRecord} (and its
 * {@code convertResultToGrpcRecord} sibling used by queries/commands) never sent the {@code @cat} metadata property
 * that {@code JsonSerializer} always sends over HTTP for every Document/Vertex/Edge. The client's
 * {@code grpcRecordToDBRecord()} treats a missing {@code @cat} as "resolve it through the remote schema instead",
 * and {@code lookupByRID()} - unlike the query path, which has a property-only {@code Result} fallback - has no
 * fallback for a {@code Record} it cannot categorize: a schema resolution that is stale, lagging, or simply not
 * yet warm turns a real "found" answer into a spurious "not found" exception.
 * <p>
 * The fix sends {@code @cat} on the wire for every element, exactly as HTTP does, so the client never needs the
 * schema round-trip for this at all. Pinned directly against the RPC response rather than through the schema-cache
 * warm-up timing of the high-level client, which is not itself deterministic enough to fail reliably pre-fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6404LookupByRidMissingCatIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private static final Metadata.Key<String> USER_HEADER =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private static final String VERTEX_TYPE = "Issue6404Vertex";

  private ManagedChannel                                    channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

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
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
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

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private ExecuteCommandResponse executeCommand(final String command) {
    return authenticatedStub.executeCommand(
        ExecuteCommandRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setCommand(command)
            .setReturnRows(true)
            .build());
  }

  @Test
  void lookupByRidResponseCarriesCatForAVertex() {
    executeCommand("CREATE VERTEX TYPE `" + VERTEX_TYPE + "` IF NOT EXISTS");
    executeCommand("CREATE PROPERTY `" + VERTEX_TYPE + "`.ldapId IF NOT EXISTS STRING");
    executeCommand("DELETE FROM `" + VERTEX_TYPE + "`");

    final ExecuteCommandResponse inserted = executeCommand("INSERT INTO `" + VERTEX_TYPE + "` SET ldapId = 'heimdall'");
    assertThat(inserted.getRecordsCount()).isEqualTo(1);
    final String rid = inserted.getRecords(0).getRid();
    assertThat(rid).startsWith("#");

    final LookupByRidResponse response = authenticatedStub.lookupByRid(
        LookupByRidRequest.newBuilder()
            .setDatabase(getDatabaseName())
            .setCredentials(credentials())
            .setRid(rid)
            .build());

    assertThat(response.getFound()).as("the record must be found").isTrue();

    final GrpcRecord record = response.getRecord();
    assertThat(record.getRid()).isEqualTo(rid);
    assertThat(record.getType()).isEqualTo(VERTEX_TYPE);

    // The bug: this property used to be entirely absent, forcing the client to re-derive the category through the
    // remote schema - the only conversion path (lookupByRID) that has no fallback when that re-derivation fails.
    assertThat(record.getPropertiesMap())
        .as("the record must carry its own category rather than relying on the client's schema cache")
        .containsEntry("@cat", GrpcValue.newBuilder().setStringValue("v").build());
  }
}

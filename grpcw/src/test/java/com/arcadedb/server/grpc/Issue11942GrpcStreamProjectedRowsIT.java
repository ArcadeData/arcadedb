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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11942: {@code StreamQuery} (CURSOR, PAGED and MATERIALIZE_ALL) sent every row that wraps a
 * record as the whole backing record, so a {@code SELECT *, !secret} streamed {@code secret} back and a computed alias of
 * {@code SELECT *, x AS y} was lost, while the unary {@code ExecuteQuery} answered the projected row. The two calls must
 * answer the same rows for the same query.
 * <p>
 * The queries carry no ORDER BY on purpose: the SQL planner's after-ORDER-BY projection brings an excluded column back
 * (issue #12453), which is the engine's row and not this serializer's, so the rows are compared without regard to order.
 */
public class Issue11942GrpcStreamProjectedRowsIT extends BaseGrpcServerTest {
  private static final String TYPE = "Issue11942P";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private static final List<StreamQueryRequest.RetrievalMode> MODES = List.of(StreamQueryRequest.RetrievalMode.CURSOR,
      StreamQueryRequest.RetrievalMode.PAGED, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL);

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> {
      db.getSchema().getOrCreateDocumentType(TYPE);
      for (int i = 0; i < 3; i++)
        db.newDocument(TYPE).set("name", "n" + i).set("secret", "s" + i).set("seq", i).save();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, new AuthClientInterceptor()));
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void streamedRowsHonourAnExcludedColumn() {
    for (final StreamQueryRequest.RetrievalMode mode : MODES) {
      final List<GrpcRecord> rows = stream("SELECT *, !secret FROM " + TYPE, mode);
      assertThat(rows).as(mode.name()).hasSize(3);
      for (final GrpcRecord row : rows) {
        assertThat(row.getPropertiesMap()).as("%s: SELECT *, !secret must not stream 'secret'", mode).doesNotContainKey("secret");
        assertThat(row.getPropertiesMap()).as(mode.name()).containsKey("name");
      }
    }
  }

  @Test
  void streamedRowsCarryAComputedAlias() {
    for (final StreamQueryRequest.RetrievalMode mode : MODES) {
      final List<GrpcRecord> rows = stream("SELECT *, name AS alias FROM " + TYPE, mode);
      assertThat(rows).as(mode.name()).hasSize(3);
      for (final GrpcRecord row : rows) {
        final GrpcValue alias = row.getPropertiesMap().get("alias");
        assertThat(alias).as("%s: SELECT *, name AS alias must stream 'alias'", mode).isNotNull();
        assertThat(alias.getStringValue()).as(mode.name()).isEqualTo("n" + row.getPropertiesMap().get("seq").getInt32Value());
        assertThat(row.getPropertiesMap()).as(mode.name()).containsKey("secret");
      }
    }
  }

  @Test
  void streamedRowsHonourAnExcludedRid() {
    for (final StreamQueryRequest.RetrievalMode mode : MODES)
      for (final GrpcRecord row : stream("SELECT *, !@rid FROM " + TYPE, mode)) {
        assertThat(row.getPropertiesMap()).as("%s: SELECT *, !@rid must not stream '@rid'", mode).doesNotContainKey("@rid");
        assertThat(row.getRid()).as("%s: SELECT *, !@rid must not stream the rid field", mode).isEmpty();
      }
  }

  @Test
  void unaryRowsHonourAnExcludedRid() {
    for (final GrpcRecord row : unary("SELECT *, !@rid FROM " + TYPE)) {
      assertThat(row.getPropertiesMap()).as("ExecuteQuery: SELECT *, !@rid must not answer '@rid'").doesNotContainKey("@rid");
      assertThat(row.getRid()).as("ExecuteQuery: SELECT *, !@rid must not answer the rid field").isEmpty();
    }
  }

  @Test
  void nestedRowsHonourAnExcludedRid() {
    final String query = "SELECT $rows AS rows LET $rows = (SELECT *, !@rid FROM " + TYPE + ")";
    final List<GrpcRecord> unaryRows = unary(query);
    assertThat(unaryRows).hasSize(1);
    assertNestedRowsExcludeRid(unaryRows.getFirst(), "ExecuteQuery");
    for (final StreamQueryRequest.RetrievalMode mode : MODES) {
      final List<GrpcRecord> rows = stream(query, mode);
      assertThat(rows).as(mode.name()).hasSize(1);
      assertNestedRowsExcludeRid(rows.getFirst(), mode.name());
    }
  }

  private static void assertNestedRowsExcludeRid(final GrpcRecord row, final String call) {
    final GrpcValue rows = row.getPropertiesMap().get("rows");
    assertThat(rows).as("%s: the LET rows column", call).isNotNull();
    assertThat(rows.getListValue().getValuesList()).as("%s: %s", call, rows).hasSize(3);
    for (final GrpcValue nested : rows.getListValue().getValuesList()) {
      assertThat(nested.hasMapValue()).as("%s: a nested SELECT *, !@rid row is a map: %s", call, nested).isTrue();
      assertThat(nested.getMapValue().getEntriesMap()).as("%s: a nested SELECT *, !@rid row must not answer '@rid'", call)
          .doesNotContainKey("@rid").containsKey("name").containsKey("secret");
    }
  }

  @Test
  void streamedRowsMatchTheUnaryRows() {
    for (final String query : List.of(//
        "SELECT FROM " + TYPE,//
        "SELECT * FROM " + TYPE,//
        "SELECT *, !secret FROM " + TYPE,//
        "SELECT *, !@rid, !@type FROM " + TYPE,//
        "SELECT *, seq + 1 AS next FROM " + TYPE,//
        "SELECT name, seq FROM " + TYPE,//
        "SELECT count(*) AS c FROM " + TYPE)) {
      final List<GrpcRecord> expected = unary(query);
      assertThat(expected).as(query).isNotEmpty();
      for (final StreamQueryRequest.RetrievalMode mode : MODES)
        assertThat(stream(query, mode)).as("%s over %s must answer the ExecuteQuery rows", query, mode)
            .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  private List<GrpcRecord> stream(final String query, final StreamQueryRequest.RetrievalMode mode) {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setQuery(query).setBatchSize(2).setRetrievalMode(mode).build();
    final List<GrpcRecord> rows = new ArrayList<>();
    for (final Iterator<QueryResult> results = stub.streamQuery(request); results.hasNext(); )
      rows.addAll(results.next().getRecordsList());
    return rows;
  }

  private List<GrpcRecord> unary(final String query) {
    final ExecuteQueryResponse response = stub.executeQuery(
        ExecuteQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).build());
    final List<GrpcRecord> rows = new ArrayList<>();
    for (final QueryResult result : response.getResultsList())
      rows.addAll(result.getRecordsList());
    return rows;
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
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
}

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
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11800: a {@code StreamQuery} in PAGED retrieval mode re-ran the SQL query once per page, wrapped
 * in {@code SKIP n LIMIT batch}, so every page paid for all the rows before it and a stream of N rows cost O(N^2 / batch).
 * <p>
 * The cost is asserted as what it is, the number of rows the query evaluates, through a function that counts its calls:
 * streaming N rows evaluates N rows whatever the batch size, where it used to evaluate them again for every page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11800GrpcPagedSingleExecutionIT extends BaseGrpcServerTest {
  private static final int    ROWS     = 60;
  private static final String FUNCTION = "issue11800Counted";

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  // static: the function registry is shared, so the function registered first is the one every test of the class calls
  private static final AtomicInteger evaluatedRows = new AtomicInteger();

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        db.newVertex(VERTEX1_TYPE_NAME).set("id", 11800_000L + i).set("seq", i).save();
    });

    ((SQLQueryEngine) db.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        evaluatedRows.incrementAndGet();
        return params[0];
      }

      @Override
      public String getSyntax() {
        return FUNCTION + "(<value>)";
      }
    });

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
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

  @Test
  void pagedEvaluatesEveryRowOnceWhateverTheBatchSize() {
    final String query = "SELECT " + FUNCTION + "(seq) AS seq FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL";

    for (final int batchSize : new int[] { 1, 7, 20, 1000 }) {
      evaluatedRows.set(0);
      final List<QueryResult> batches = stream(query, batchSize, StreamQueryRequest.RetrievalMode.PAGED);

      final List<Long> seq = seqOf(batches);
      final List<Long> expected = new ArrayList<>();
      for (long i = 0; i < ROWS; i++)
        expected.add(i);
      assertThat(seq).as("batch size %d: every row exactly once", batchSize).containsExactlyInAnyOrderElementsOf(expected);

      assertThat(evaluatedRows.get())
          .as("batch size %d: PAGED runs the query once, so it evaluates each of the %d rows once (issue #11800)", batchSize, ROWS)
          .isEqualTo(ROWS);

      for (int i = 0; i < batches.size(); i++) {
        final QueryResult batch = batches.get(i);
        final boolean last = i == batches.size() - 1;
        assertThat(batch.getIsLastBatch()).as("batch size %d, batch %d", batchSize, i).isEqualTo(last);
        if (!last)
          assertThat(batch.getRecordsCount()).as("batch size %d, batch %d", batchSize, i).isEqualTo(batchSize);
      }
      assertThat(batches).hasSize((ROWS + batchSize - 1) / batchSize);
    }
  }

  @Test
  void pagedAnswersTheRowsOfCursorInTheSameOrder() {
    for (final String query : List.of("SELECT seq FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL",
        "SELECT seq FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL ORDER BY seq DESC",
        "SELECT seq FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL ORDER BY seq SKIP 5 LIMIT 31"))
      assertThat(seqOf(stream(query, 7, StreamQueryRequest.RetrievalMode.PAGED))).as(query)
          .containsExactlyElementsOf(seqOf(stream(query, 7, StreamQueryRequest.RetrievalMode.CURSOR)));
  }

  private List<QueryResult> stream(final String query, final int batchSize, final StreamQueryRequest.RetrievalMode mode) {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getDatabaseName()).setCredentials(credentials())
        .setQuery(query).setBatchSize(batchSize).setRetrievalMode(mode).build();
    final List<QueryResult> batches = new ArrayList<>();
    for (final Iterator<QueryResult> results = authenticatedStub.streamQuery(request); results.hasNext(); )
      batches.add(results.next());
    return batches;
  }

  private static List<Long> seqOf(final List<QueryResult> batches) {
    final List<Long> seq = new ArrayList<>();
    for (final QueryResult batch : batches)
      for (final GrpcRecord record : batch.getRecordsList()) {
        final GrpcValue value = record.getPropertiesMap().get("seq");
        assertThat(value).as("every row must carry the 'seq' property").isNotNull();
        seq.add(value.hasInt64Value() ? value.getInt64Value() : value.getInt32Value());
      }
    return seq;
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

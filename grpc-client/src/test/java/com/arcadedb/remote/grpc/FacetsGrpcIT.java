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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.http.FacetsHttpIT;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.ExecuteQueryRequest;
import com.arcadedb.server.grpc.ExecuteQueryResponse;
import com.arcadedb.server.grpc.FacetProtocol;
import com.arcadedb.server.grpc.ProjectionSettings.ProjectionEncoding;
import com.arcadedb.server.grpc.QueryResult;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import io.grpc.Status;
import io.grpc.stub.BlockingClientCall;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The facets of a FACETS query travel over every gRPC path: once on the unary responses, once on the terminal batch of a CURSOR or
 * MATERIALIZE_ALL stream (an empty terminal batch carries them when no row matched), and PAGED refuses the query.
 */
class FacetsGrpcIT extends BaseGrpcClientServerTest {
  private static final String TYPE        = ColumnarTestData.TYPE;
  private static final String WHERE       = " WHERE fileSize > 0";
  private static final String FACETS      = " FACETS brand TERMS TOP 5, tags TERMS TOP 3";
  private static final String QUERY       = "SELECT FROM " + TYPE + WHERE + FACETS;
  private static final String EMPTY_QUERY = "SELECT FROM " + TYPE + " WHERE fileSize < 0" + FACETS;
  private static final int    BATCH       = 7;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    final ColumnarTestData data = new ColumnarTestData(11);
    data.createSchema(db);
    data.insertAssets(db, 100);
    FacetOracle.awaitCaughtUp(db);
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null)
      database.close();
    if (grpcServer != null)
      grpcServer.close();
    super.endTest();
  }

  /**
   * Final review C-I8: the facet values travel as the rows' values do, so the client reads a DATE, a DATETIME, a DECIMAL and a NaN term
   * as objects equal to (and of the class of) the same property's values in a row, on the unary and the streamed path.
   */
  @Test
  void facetValuesDecodeAsRowValues() {
    FacetsHttpIT.createTypedValues(getServerDatabase(0, getDatabaseName()));
    final String facets = "SELECT FROM Typed FACETS d: day TERMS, a: at TERMS, p: price TERMS, s: score TERMS";
    final Map<String, Set<Object>> rows = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT day AS d, at AS a, price AS p, score AS s FROM Typed")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        for (final String alias : List.of("d", "a", "p", "s"))
          rows.computeIfAbsent(alias, k -> new HashSet<>()).add(row.getProperty(alias));
      }
    }
    final FacetResults unary;
    try (final ResultSet rs = database.query("sql", facets)) {
      unary = rs.getFacets();
    }
    final FacetResults streamed;
    try (final ResultSet rs = database.queryStream("sql", facets, BATCH)) {
      while (rs.hasNext())
        rs.next();
      streamed = rs.getFacets();
    }
    for (final String alias : List.of("d", "a", "p", "s")) {
      assertThat(termValues(unary.get(alias))).as("unary " + alias).isEqualTo(rows.get(alias));
      assertThat(termValues(streamed.get(alias))).as("streamed " + alias).isEqualTo(rows.get(alias));
    }
    assertThat(rows.get("p").iterator().next()).isInstanceOf(BigDecimal.class);
    assertThat(rows.get("s")).contains(Double.NaN);
  }

  private static Set<Object> termValues(final FacetResult result) {
    final Set<Object> values = new HashSet<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        values.add(bucket.value());
    return values;
  }

  @Test
  void unaryQueryCarriesFacets() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      assertThat(rs.stream().count()).isEqualTo(embeddedRowCount(QUERY));
      assertSameFacets(rs.getFacets(), QUERY);
    }
  }

  @Test
  void commandCarriesFacets() {
    try (final ResultSet rs = database.command("sql", QUERY)) {
      assertSameFacets(rs.getFacets(), QUERY);
      assertThat(rs.stream().count()).isEqualTo(embeddedRowCount(QUERY));
    }
  }

  @Test
  void cursorStreamFacetsOnLastBatch() throws Exception {
    final long rows = embeddedRowCount(QUERY);
    assertThat(rows).isGreaterThan(2L * BATCH);

    final List<QueryResult> messages = rawStream(QUERY, StreamQueryRequest.RetrievalMode.CURSOR);
    assertOnlyTheTerminalBatchCarriesFacets(messages, rows);

    try (final ResultSet rs = database.queryStream("sql", QUERY, BATCH)) {
      long seen = 0;
      while (rs.hasNext()) {
        rs.next();
        ++seen;
      }
      assertThat(seen).isEqualTo(rows);
      assertSameFacets(rs.getFacets(), QUERY);
    }
  }

  @Test
  void cursorStreamWithZeroRowsStillSendsFacets() throws Exception {
    assertThat(embeddedRowCount(EMPTY_QUERY)).isZero();

    final List<QueryResult> messages = rawStream(EMPTY_QUERY, StreamQueryRequest.RetrievalMode.CURSOR);
    assertThat(messages).hasSize(1);
    assertThat(messages.getFirst().getRecordsCount()).isZero();
    assertThat(messages.getFirst().getIsLastBatch()).isTrue();
    assertThat(messages.getFirst().hasFacets()).isTrue();

    final FacetResults facets = FacetProtocol.fromProto(messages.getFirst().getFacets(), ProtoUtils::fromGrpcValue);
    assertThat(facets.exact()).isTrue();
    for (final String alias : facets.aliases()) {
      final FacetResult result = facets.get(alias);
      assertThat(result.missing()).isZero();
      assertThat(result.otherCount()).isZero();
      assertThat(result.buckets()).allSatisfy(bucket -> assertThat(bucket.count()).isZero());
    }

    try (final ResultSet rs = database.queryStream("sql", EMPTY_QUERY, BATCH)) {
      assertThat(rs.hasNext()).isFalse();
      assertSameFacets(rs.getFacets(), EMPTY_QUERY);
    }
  }

  @Test
  void materializeAllCarriesFacets() throws Exception {
    final long rows = embeddedRowCount(QUERY);
    assertOnlyTheTerminalBatchCarriesFacets(rawStream(QUERY, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL), rows);

    final List<QueryResult> empty = rawStream(EMPTY_QUERY, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL);
    assertThat(empty).hasSize(1);
    assertThat(empty.getFirst().getIsLastBatch()).isTrue();
    assertThat(empty.getFirst().hasFacets()).isTrue();

    try (final ResultSet rs = database.queryStream("sql", QUERY, getDefaultConfig(), BATCH, StreamQueryRequest.RetrievalMode.MATERIALIZE_ALL)) {
      assertThat(rs.stream().count()).isEqualTo(rows);
      assertSameFacets(rs.getFacets(), QUERY);
    }
  }

  @Test
  void batchesIteratorCarriesFacetsOnTheLastBatch() {
    for (final String query : List.of(QUERY, EMPTY_QUERY)) {
      final List<QueryBatch> batches = new ArrayList<>();
      final Iterator<QueryBatch> it = database.queryStreamBatchesIterator("sql", query, Map.of(), BATCH,
          StreamQueryRequest.RetrievalMode.CURSOR);
      while (it.hasNext())
        batches.add(it.next());

      assertThat(batches).as(query).isNotEmpty();
      long rows = 0;
      for (int i = 0; i < batches.size(); i++) {
        final QueryBatch batch = batches.get(i);
        rows += batch.results().size();
        if (i < batches.size() - 1)
          assertThat(batch.facets()).as("batch %d of %d", i, batches.size()).isNull();
      }
      assertThat(rows).isEqualTo(embeddedRowCount(query));
      assertThat(batches.getLast().isLastBatch()).isTrue();
      assertSameFacets(batches.getLast().facets(), query);
    }
  }

  @Test
  void pagedIsRefused() {
    assertThatThrownBy(() -> rawStream(QUERY, StreamQueryRequest.RetrievalMode.PAGED)).satisfies(e -> {
      final Status status = Status.fromThrowable(e);
      assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
      assertThat(status.getDescription()).isEqualTo("PAGED retrieval cannot carry FACETS: use CURSOR or MATERIALIZE_ALL");
    });
  }

  @Test
  void maxRowsDoesNotShrinkFacets() throws Exception {
    final ExecuteQueryResponse response = stub().executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(QUERY).setLanguage("sql").setLimit(3).build());

    assertThat(response.getResults(0).getRecordsCount()).isEqualTo(3);
    assertThat(response.hasFacets()).isTrue();
    assertSameFacets(FacetProtocol.fromProto(response.getFacets(), ProtoUtils::fromGrpcValue), QUERY);
  }

  @Test
  void noFacetsWithoutClause() throws Exception {
    final String plain = "SELECT FROM " + TYPE + WHERE;

    final ExecuteQueryResponse query = stub().executeQuery(ExecuteQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(plain).setLanguage("sql").build());
    assertThat(query.hasFacets()).isFalse();
    assertThat(query.getResults(0).hasFacets()).isFalse();

    final ExecuteCommandResponse command = stub().executeCommand(ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setCommand(plain).setLanguage("sql").setReturnRows(true).build());
    assertThat(command.hasFacets()).isFalse();

    for (final StreamQueryRequest.RetrievalMode mode : StreamQueryRequest.RetrievalMode.values()) {
      if (mode == StreamQueryRequest.RetrievalMode.UNRECOGNIZED)
        continue;
      assertThat(rawStream(plain, mode)).allSatisfy(message -> assertThat(message.hasFacets()).isFalse());
      // a stream without facets still sends no empty terminal batch when nothing matched
      assertThat(rawStream("SELECT FROM " + TYPE + " WHERE fileSize < 0", mode)).isEmpty();
    }

    try (final ResultSet rs = database.query("sql", plain)) {
      assertThat(rs.getFacets()).isNull();
    }
    try (final ResultSet rs = database.queryStream("sql", plain, BATCH)) {
      assertThat(rs.stream().count()).isEqualTo(embeddedRowCount(plain));
      assertThat(rs.getFacets()).isNull();
    }
  }

  private static void assertOnlyTheTerminalBatchCarriesFacets(final List<QueryResult> messages, final long rows) {
    assertThat(messages).hasSizeGreaterThan(2);
    long total = 0;
    for (int i = 0; i < messages.size(); i++) {
      final QueryResult message = messages.get(i);
      total += message.getRecordsCount();
      final boolean last = i == messages.size() - 1;
      assertThat(message.getIsLastBatch()).isEqualTo(last);
      assertThat(message.hasFacets()).as("facets on batch %d of %d", i, messages.size()).isEqualTo(last);
    }
    assertThat(total).isEqualTo(rows);
  }

  private List<QueryResult> rawStream(final String query, final StreamQueryRequest.RetrievalMode mode) throws Exception {
    final BlockingClientCall<?, QueryResult> call = stub().streamQuery(StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName()).setCredentials(credentials()).setQuery(query).setLanguage("sql").setBatchSize(BATCH)
        .setRetrievalMode(mode).build());
    final List<QueryResult> messages = new ArrayList<>();
    while (call.hasNext()) {
      final QueryResult message = call.read();
      if (message == null)
        break;
      messages.add(message);
    }
    return messages;
  }

  private static RemoteGrpcConfig getDefaultConfig() {
    return new RemoteGrpcConfig(true, ProjectionEncoding.PROJECTION_AS_JSON, 0);
  }

  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingV2Stub stub() {
    return grpcServer.newBlockingStub(30_000, getDatabaseName());
  }

  private static DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private long embeddedRowCount(final String query) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", query)) {
      return rs.stream().count();
    }
  }

  private void assertSameFacets(final FacetResults remoteFacets, final String query) {
    assertThat(remoteFacets).isNotNull();
    final String expected;
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", query)) {
      expected = rs.getFacets().toJSON().toString();
    }
    assertThat(remoteFacets.toJSON().toString()).isEqualTo(expected);
  }
}

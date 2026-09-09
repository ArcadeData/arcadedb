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
package com.arcadedb.server.http;

import com.arcadedb.database.Database;
import com.arcadedb.query.search.VectorSearchLeg;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.ReadConsistency;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.AbstractQueryHandler;
import io.micrometer.core.instrument.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306, client half: a handler alone is invisible. The streaming query and the three vector routes only
 * become usable once {@code RemoteDatabase} can reach them, which is what these tests drive - the Java driver
 * against the real endpoints, not the endpoints against a hand-built request.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7306RemoteClientIT extends BaseGraphServerTest {

  private static final int ROW_COUNT = 5_000;

  private RemoteDatabase remote;

  @BeforeEach
  void seedAndConnect() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType("R7306Row")) {
      db.transaction(() -> {
        db.command("sql", "CREATE DOCUMENT TYPE R7306Row");
        db.command("sql", "CREATE PROPERTY R7306Row.idx INTEGER");
      });
      db.begin();
      for (int i = 0; i < ROW_COUNT; i++) {
        db.newDocument("R7306Row").set("idx", i).save();
        if (i % 2_000 == 0) {
          db.commit();
          db.begin();
        }
      }
      db.commit();

      db.transaction(() -> {
        db.command("sql", "CREATE DOCUMENT TYPE R7306Doc BUCKETS 1");
        db.command("sql", "CREATE PROPERTY R7306Doc.title STRING");
        db.command("sql", "CREATE PROPERTY R7306Doc.content STRING");
        db.command("sql", "CREATE PROPERTY R7306Doc.embedding ARRAY_OF_FLOATS");
        db.command("sql", """
            CREATE INDEX ON R7306Doc (embedding) LSM_VECTOR
            METADATA { dimensions: 3, similarity: 'COSINE' }
            """);
        db.command("sql", "CREATE INDEX ON R7306Doc (content) FULL_TEXT");
        db.newDocument("R7306Doc").set("title", "near").set("content", "flywheel bearings")
            .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
        db.newDocument("R7306Doc").set("title", "far").set("content", "rank fusion")
            .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      });
    }

    remote = new RemoteDatabase("localhost", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  // --------------------------------------------------------------------------------------------
  // Streaming query
  // --------------------------------------------------------------------------------------------

  @Test
  void queryStreamingReturnsEveryRowInOrder() {
    final List<Integer> seen = new ArrayList<>();
    try (final ResultSet rs = remote.queryStreaming("sql", "SELECT idx FROM R7306Row ORDER BY idx")) {
      while (rs.hasNext())
        seen.add(rs.next().<Integer>getProperty("idx"));
    }

    assertThat(seen).hasSize(ROW_COUNT);
    assertThat(seen.get(0)).isZero();
    assertThat(seen.get(ROW_COUNT - 1)).isEqualTo(ROW_COUNT - 1);
  }

  /**
   * The driver's result set must be lazy, not a list built behind a {@link ResultSet} facade. Reading one row and
   * asking the server how much it has produced is the same falsifiable check the HTTP-level test makes: a driver
   * that drained the body first would see the whole result set already produced.
   */
  @Test
  void queryStreamingIsConsumedLazily() {
    final double before = Metrics.counter(AbstractQueryHandler.STREAMED_ROWS_METRIC).count();

    try (final ResultSet rs = remote.queryStreaming("sql", "SELECT idx FROM R7306Row ORDER BY idx")) {
      assertThat(rs.hasNext()).isTrue();
      final Result first = rs.next();
      assertThat(first.<Integer>getProperty("idx")).isZero();

      final double produced = Metrics.counter(AbstractQueryHandler.STREAMED_ROWS_METRIC).count() - before;
      assertThat(produced)
          .as("the driver drained the whole body before handing over the first row")
          .isLessThan(ROW_COUNT);
    }
  }

  @Test
  void queryStreamingAcceptsNamedParameters() {
    try (final ResultSet rs = remote.queryStreaming("sql", "SELECT idx FROM R7306Row WHERE idx = :wanted",
        Map.of("wanted", 42))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("idx")).isEqualTo(42);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void queryStreamingReportsAServerSideFailureBeforeTheStreamBegins() {
    assertThatThrownBy(() -> remote.queryStreaming("sql", "SELECT FROM NoSuchTypeFor7306"))
        .isInstanceOf(RemoteException.class);
  }

  /**
   * The buffered and streamed driver calls describe the same rows, so they must produce the same values. A drift
   * here would mean switching to the streaming call silently changed what an application reads.
   */
  @Test
  void theStreamedAndBufferedDriverCallsAgree() {
    final List<Integer> buffered = new ArrayList<>();
    try (final ResultSet rs = remote.query("sql", "SELECT idx FROM R7306Row WHERE idx < 10 ORDER BY idx")) {
      while (rs.hasNext())
        buffered.add(rs.next().<Integer>getProperty("idx"));
    }

    final List<Integer> streamed = new ArrayList<>();
    try (final ResultSet rs = remote.queryStreaming("sql",
        "SELECT idx FROM R7306Row WHERE idx < 10 ORDER BY idx")) {
      while (rs.hasNext())
        streamed.add(rs.next().<Integer>getProperty("idx"));
    }

    assertThat(streamed).isEqualTo(buffered);
  }

  /**
   * {@code RemoteHttpComponent.httpCommand} injects the HA read-consistency headers on every request that goes
   * through it, but the streamed query and the vector routes build their own requests and so had to restate the
   * injection. Without it a client that declared READ_YOUR_WRITES would silently stop getting it on exactly these
   * two endpoints - the failure mode that is hardest to notice, because a stale answer still looks complete.
   * <p>
   * The bookmark the driver sends is the commit index it last observed, so the check is that a write raises it and
   * that the following streamed read is served by a server that has applied at least that index: the read
   * therefore sees the write. On a single-server fixture that is a barrier which cannot be violated, which is
   * exactly why the assertion is on the observed data rather than on the header string.
   */
  @Test
  void aStreamedReadUnderReadYourWritesSeesTheWriteThatPrecededIt() {
    remote.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);
    try {
      remote.command("sql", "INSERT INTO R7306Row SET idx = 999999");

      final List<Integer> seen = new ArrayList<>();
      try (final ResultSet rs = remote.queryStreaming("sql", "SELECT idx FROM R7306Row WHERE idx = 999999")) {
        while (rs.hasNext())
          seen.add(rs.next().<Integer>getProperty("idx"));
      }

      assertThat(seen).containsExactly(999999);
    } finally {
      remote.command("sql", "DELETE FROM R7306Row WHERE idx = 999999");
      remote.setReadConsistency(ReadConsistency.EVENTUAL);
    }
  }

  // --------------------------------------------------------------------------------------------
  // Vector, hybrid and full-text search
  // --------------------------------------------------------------------------------------------

  @Test
  void vectorSearchReachesTheEndpointAndRanksByDistance() {
    final JSONObject response = remote.vectorSearch(new JSONObject()
        .put("indexName", "R7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 2));

    assertThat(response.getInt("count")).isEqualTo(2);
    assertThat(response.getString("scoring")).startsWith("distance_lower_is_better:");
    assertThat(response.getJSONArray("results").getJSONObject(0).getJSONObject("properties").getString("title"))
        .isEqualTo("near");
  }

  @Test
  void fullTextSearchReachesTheEndpoint() {
    final JSONObject response = remote.fullTextSearch(new JSONObject()
        .put("indexName", "R7306Doc[content]")
        .put("queryText", "flywheel"));

    assertThat(response.getInt("count")).isEqualTo(1);
    assertThat(response.getJSONArray("results").getJSONObject(0).getJSONObject("properties").getString("title"))
        .isEqualTo("near");
  }

  @Test
  void hybridSearchReachesTheEndpoint() {
    final JSONObject response = remote.hybridSearch(new JSONObject()
        .put("vectorIndexName", "R7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(0.0f).put(1.0f).put(0.0f))
        .put("fulltextIndexName", "R7306Doc[content]")
        .put("fulltextQuery", "flywheel")
        .put("k", 2));

    assertThat(response.getBoolean("fused")).isTrue();
    assertThat(response.getInt("count")).isEqualTo(2);
  }

  /**
   * A bound violation must reach the caller carrying the server's own message rather than a bare status code, or
   * the driver is strictly less usable than curl. The message rides on the cause, which is where every other
   * {@code RemoteDatabase} failure puts the server's detail - the outer message names the operation.
   */
  @Test
  void aBoundViolationSurfacesTheServersOwnMessage() {
    assertThatThrownBy(() -> remote.vectorSearch(new JSONObject()
        .put("indexName", "R7306Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f).put(0.0f).put(0.0f))
        .put("k", 1)
        .put("efSearch", VectorSearchLeg.MAX_EF_SEARCH + 1)))
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("vector search")
        .cause()
        .hasMessageContaining("'efSearch' must be between 1 and " + VectorSearchLeg.MAX_EF_SEARCH);
  }
}

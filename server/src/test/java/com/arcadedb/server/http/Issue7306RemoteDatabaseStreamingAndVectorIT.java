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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.query.search.VectorLeg;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7306, client side: a feature that exists only on the wire is invisible. These tests drive both halves
 * of the issue through {@code RemoteDatabase}, which is what an application actually holds.
 */
public class Issue7306RemoteDatabaseStreamingAndVectorIT extends BaseGraphServerTest {
  private static final String TYPE_NAME   = "RemoteStream7306";
  private static final int    ROW_COUNT   = 25;
  private static final String DENSE_TYPE  = "RemoteVec7306";
  private static final String DENSE_INDEX = "RemoteVec7306[embedding]";
  private static final String DOC_INDEX   = "RemoteVec7306[text]";

  @Override
  protected void populateDatabase() {
    super.populateDatabase();

    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".idx INTEGER");
      for (int i = 0; i < ROW_COUNT; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();

      db.command("sql", "CREATE DOCUMENT TYPE " + DENSE_TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".text STRING");
      db.command("sql", "CREATE PROPERTY " + DENSE_TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + DENSE_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      db.command("sql", "CREATE INDEX ON " + DENSE_TYPE + " (text) FULL_TEXT");

      db.newDocument(DENSE_TYPE).set("name", "near").set("text", "alpha beta")
          .set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      db.newDocument(DENSE_TYPE).set("name", "far").set("text", "gamma delta")
          .set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
    });
  }

  private RemoteDatabase remote() {
    return new RemoteDatabase("localhost", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  /**
   * The streamed rows must be the buffered rows. A driver whose two encodings disagree about the value of a
   * column would be worse than having only one.
   */
  @Test
  void queryStreamReturnsTheSameRowsAsTheBufferedQuery() {
    try (final RemoteDatabase database = remote()) {
      final String query = "SELECT idx FROM " + TYPE_NAME + " ORDER BY idx";

      final List<Integer> buffered = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          buffered.add(rs.next().getProperty("idx"));
      }

      final List<Integer> streamed = new ArrayList<>();
      try (final ResultSet rs = database.queryStream("sql", query, Map.of())) {
        while (rs.hasNext())
          streamed.add(rs.next().getProperty("idx"));
      }

      assertThat(streamed).hasSize(ROW_COUNT).isEqualTo(buffered);
    }
  }

  /**
   * Parameters have to reach the streamed request, or half the API is unusable.
   */
  @Test
  void queryStreamPassesNamedParameters() {
    try (final RemoteDatabase database = remote()) {
      try (final ResultSet rs = database.queryStream("sql",
          "SELECT idx FROM " + TYPE_NAME + " WHERE idx >= :from ORDER BY idx", Map.of("from", ROW_COUNT - 3))) {
        final List<Integer> rows = new ArrayList<>();
        while (rs.hasNext())
          rows.add(rs.next().getProperty("idx"));
        assertThat(rows).containsExactly(ROW_COUNT - 3, ROW_COUNT - 2, ROW_COUNT - 1);
      }
    }
  }

  /**
   * The result set owns the connection until it is exhausted or closed. Closing early must not throw and must not
   * leave the caller unable to issue the next request on the same driver.
   */
  @Test
  void aStreamedResultSetCanBeAbandonedEarly() {
    try (final RemoteDatabase database = remote()) {
      try (final ResultSet rs = database.queryStream("sql", "SELECT idx FROM " + TYPE_NAME, Map.of())) {
        assertThat(rs.hasNext()).isTrue();
        final Result first = rs.next();
        assertThat(first.<Integer>getProperty("idx")).isNotNull();
      }

      // The driver is still usable afterwards.
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM " + TYPE_NAME)) {
        assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(ROW_COUNT);
      }
    }
  }

  /**
   * A failure that the server can still answer with a status code must reach the caller as the typed exception
   * the buffered path produces, not as a stream that simply ends.
   */
  @Test
  void aStreamedQueryThatCannotParseIsReportedAsAnError() {
    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.queryStream("sql", "SELECT FROM NoSuchType7306", Map.of()))
          .hasMessageContaining("NoSuchType7306");
    }
  }

  @Test
  void commandStreamStreamsTheRowsOfACommand() {
    try (final RemoteDatabase database = remote()) {
      try (final ResultSet rs = database.commandStream("sql",
          "SELECT idx FROM " + TYPE_NAME + " ORDER BY idx", Map.of())) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(ROW_COUNT);
      }
    }
  }

  // ───────────────────────────── vector ─────────────────────────────

  @Test
  void vectorSearchReachesTheNewEndpoint() {
    try (final RemoteDatabase database = remote()) {
      final JSONObject response = database.vectorSearch(new JSONObject()
          .put("indexName", DENSE_INDEX)
          .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
          .put("k", 2));

      assertThat(response.getString("indexName")).isEqualTo(DENSE_INDEX);
      assertThat(response.getInt("count")).isEqualTo(2);
      assertThat(response.getJSONArray("results").getJSONObject(0)
          .getJSONObject("properties").getString("name")).isEqualTo("near");
    }
  }

  @Test
  void fullTextSearchReachesTheNewEndpoint() {
    try (final RemoteDatabase database = remote()) {
      final JSONObject response = database.fullTextSearch(new JSONObject()
          .put("indexName", DOC_INDEX)
          .put("queryText", "gamma"));

      assertThat(response.getInt("count")).isEqualTo(1);
      assertThat(response.getJSONArray("results").getJSONObject(0)
          .getJSONObject("properties").getString("name")).isEqualTo("far");
    }
  }

  @Test
  void hybridSearchReachesTheNewEndpoint() {
    try (final RemoteDatabase database = remote()) {
      final JSONObject response = database.hybridSearch(new JSONObject()
          .put("vectorIndexName", DENSE_INDEX)
          .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
          .put("fulltextQuery", "gamma")
          .put("fulltextIndexName", DOC_INDEX)
          .put("k", 2));

      assertThat(response.getBoolean("fused")).isTrue();
      assertThat(response.getJSONObject("legs").keySet()).contains("vector", "fulltext");
    }
  }

  /**
   * A bound crossed on the client must arrive as the server's own message, not as a generic remote failure: the
   * point of sharing the implementation is that the caller reads the same explanation on every surface.
   */
  @Test
  void aBoundCrossedOverTheDriverCarriesTheServersMessage() {
    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.vectorSearch(new JSONObject()
          .put("indexName", DENSE_INDEX)
          .put("queryVector", new JSONArray(List.of(1.0, 0.0, 0.0)))
          .put("k", VectorLeg.MAX_K + 1)))
          .hasMessageContaining("'k' must be between 1 and " + VectorLeg.MAX_K);
    }
  }
}

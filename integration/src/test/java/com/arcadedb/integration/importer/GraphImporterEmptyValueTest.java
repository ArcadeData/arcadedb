/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.integration.importer.graph.XmlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An empty value means "not set" for every typed property {@code GraphImporter.readProperty}
 * dispatches on, not only for DATETIME (issue #7269, follow-up to #7265).
 * <p>
 * {@code GraphImporter.RecordReader}'s default accessors have always read it that way -
 * {@code getInt}/{@code getLong}/{@code getDouble} answer {@code 0} and {@code getFloatArray}/
 * {@code getList} answer {@code null} for an empty value - but {@code JsonlRowSource} overrides all
 * five and guarded only against {@code JSONObject.isNull()}, which is false for an explicit
 * {@code ""}. An optional numeric column left blank, the normal shape of a CSV export converted to
 * JSONL, therefore ended the whole import.
 * <p>
 * Both passes get their own case, since the edge-source pass reaches the numeric accessors through
 * {@code readInt}/{@code readLong}/{@code readDouble} rather than through {@code readProperty}. The
 * CSV and XML sources were never affected and are pinned here so they cannot start to be.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterEmptyValueTest {

  private static final String DB_PATH   = "target/databases/graph-importer-empty-value-test";
  private static final String DATA_PATH = "target/test-data/graph-importer-empty-value";

  private Database database;
  private File     dataDir;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
    dataDir = new File(DATA_PATH);
    dataDir.mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      // An import that aborts mid-row leaves the batch's transaction open, and the instance is not
      // released until it is rolled back - which would turn one real failure here into a cascade of
      // "already in use" errors in every later test of the class.
      if (database.isTransactionActive())
        database.rollback();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  private String write(final String fileName, final String... lines) throws Exception {
    final File f = new File(dataDir, fileName);
    Files.write(f.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }

  // ───────────────────────────────────────────────────────────────────
  //  Vertex pass, JSONL - one case per overridden accessor
  // ───────────────────────────────────────────────────────────────────

  /**
   * {@code JsonlRecordReader.getInt}: an explicit {@code ""} used to reach
   * {@code JSONObject.getInt}, whose {@code getAsNumber()} on a {@code JsonPrimitive("")} throws.
   * "Not set" for an int is {@code 0}, the answer the interface default already gives.
   */
  @Test
  void emptyJsonlIntLeavesTheVertexPropertyAtZero() throws Exception {
    importUsers("empty-int-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"score\": 7}",
        "{\"id\": \"3\", \"name\": \"carol\"}",
        v -> v.intProperty("score", "score"));

    assertThat(intOf("alice", "score")).isZero();
    assertThat(intOf("bob", "score")).isEqualTo(7);
    // an absent attribute already worked and must keep working
    assertThat(intOf("carol", "score")).isZero();
  }

  /**
   * {@code JsonlRecordReader.getLong}, same shape through {@code JSONObject.getLong}.
   */
  @Test
  void emptyJsonlLongLeavesTheVertexPropertyAtZero() throws Exception {
    importUsers("empty-long-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"views\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"views\": 9000000000}",
        "{\"id\": \"3\", \"name\": \"carol\"}",
        v -> v.longProperty("views", "views"));

    assertThat(longOf("alice", "views")).isZero();
    assertThat(longOf("bob", "views")).isEqualTo(9_000_000_000L);
    assertThat(longOf("carol", "views")).isZero();
  }

  /**
   * {@code JsonlRecordReader.getDouble}, same shape through {@code JSONObject.getDouble}.
   */
  @Test
  void emptyJsonlDoubleLeavesTheVertexPropertyAtZero() throws Exception {
    importUsers("empty-double-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"rating\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"rating\": 4.5}",
        "{\"id\": \"3\", \"name\": \"carol\"}",
        v -> v.doubleProperty("rating", "rating"));

    assertThat(doubleOf("alice", "rating")).isZero();
    assertThat(doubleOf("bob", "rating")).isEqualTo(4.5);
    assertThat(doubleOf("carol", "rating")).isZero();
  }

  /**
   * {@code JsonlRecordReader.getFloatArray}: an explicit {@code ""} used to reach
   * {@code JSONObject.getJSONArray}. "Not set" for a vector is {@code null}, which the vertex pass
   * drops, so the property is simply absent - a row without an embedding must not abort a load.
   */
  @Test
  void emptyJsonlVectorLeavesTheVertexPropertyUnset() throws Exception {
    importUsers("empty-vector-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"embedding\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"embedding\": [0.1, 0.2, 0.3]}",
        "{\"id\": \"3\", \"name\": \"carol\"}",
        v -> v.floatArrayProperty("embedding", "embedding"));

    assertThat(hasProperty("alice", "embedding")).isFalse();
    assertThat(vertexOf("bob").get("embedding")).isEqualTo(new float[] { 0.1f, 0.2f, 0.3f });
    assertThat(hasProperty("carol", "embedding")).isFalse();
  }

  /**
   * {@code JsonlRecordReader.getList}, same shape through {@code JSONObject.getJSONArray}.
   */
  @Test
  void emptyJsonlListLeavesTheVertexPropertyUnset() throws Exception {
    importUsers("empty-list-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"tags\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"tags\": [\"java\", \"sql\"]}",
        "{\"id\": \"3\", \"name\": \"carol\"}",
        v -> v.listProperty("tags", "tags"));

    assertThat(hasProperty("alice", "tags")).isFalse();
    assertThat(asList(vertexOf("bob").get("tags"))).containsExactly("java", "sql");
    assertThat(hasProperty("carol", "tags")).isFalse();
  }

  // ───────────────────────────────────────────────────────────────────
  //  Edge-source pass
  // ───────────────────────────────────────────────────────────────────

  /**
   * The edge-source pass fills primitive buffers through {@code readInt}/{@code readLong}/
   * {@code readDouble} instead of {@code readProperty}, so the numeric accessors reach it by a
   * second route that the vertex cases above do not exercise.
   */
  @Test
  void emptyEdgeSourceNumbersLeaveTheEdgePropertiesAtZero() throws Exception {
    final String vertices = write("empty-value-edge-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\"}",
        "{\"id\": \"2\", \"name\": \"bob\"}");
    final String edges = write("empty-value-number-edges.jsonl",
        "{\"from\": \"1\", \"to\": \"2\", \"kind\": \"blank\",  \"weight\": \"\", \"hits\": \"\", \"ratio\": \"\"}",
        "{\"from\": \"2\", \"to\": \"1\", \"kind\": \"filled\", \"weight\": 3, \"hits\": 9000000000, \"ratio\": 4.5}");

    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Knows");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
        })
        .edgeSource("Knows", new JsonlRowSource(edges), e -> {
          e.from("from", "User");
          e.to("to", "User");
          e.property("kind", "kind");
          e.intProperty("weight", "weight");
          e.longProperty("hits", "hits");
          e.doubleProperty("ratio", "ratio");
        })
        .build()) {

      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      final Edge blank = edgeOfKind("blank");
      assertThat(blank.getInteger("weight")).isZero();
      assertThat(blank.getLong("hits")).isZero();
      assertThat(blank.getDouble("ratio")).isZero();

      final Edge filled = edgeOfKind("filled");
      assertThat(filled.getInteger("weight")).isEqualTo(3);
      assertThat(filled.getLong("hits")).isEqualTo(9_000_000_000L);
      assertThat(filled.getDouble("ratio")).isEqualTo(4.5);
    });
  }

  /**
   * FLOAT_ARRAY and LIST fall into {@code processEdgeSource}'s default branch, which routes them
   * through the same {@code readProperty} the vertex pass uses and then drops a {@code null} in
   * {@code EdgeCollector.appendProperties}.
   */
  @Test
  void emptyEdgeSourceVectorAndListLeaveTheEdgePropertiesUnset() throws Exception {
    final String vertices = write("empty-value-edge-vertices-2.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\"}",
        "{\"id\": \"2\", \"name\": \"bob\"}");
    final String edges = write("empty-value-array-edges.jsonl",
        "{\"from\": \"1\", \"to\": \"2\", \"kind\": \"blank\",  \"embedding\": \"\", \"tags\": \"\"}",
        "{\"from\": \"2\", \"to\": \"1\", \"kind\": \"filled\", \"embedding\": [0.5], \"tags\": [\"x\"]}");

    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createEdgeType("Knows");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
        })
        .edgeSource("Knows", new JsonlRowSource(edges), e -> {
          e.from("from", "User");
          e.to("to", "User");
          e.property("kind", "kind");
          e.floatArrayProperty("embedding", "embedding");
          e.listProperty("tags", "tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      final Edge blank = edgeOfKind("blank");
      assertThat(blank.has("embedding")).isFalse();
      assertThat(blank.has("tags")).isFalse();

      final Edge filled = edgeOfKind("filled");
      assertThat(filled.get("embedding")).isEqualTo(new float[] { 0.5f });
      assertThat(asList(filled.get("tags"))).containsExactly("x");
    });
  }

  // ───────────────────────────────────────────────────────────────────
  //  Sources that were never affected, pinned so they stay that way
  // ───────────────────────────────────────────────────────────────────

  /**
   * {@code CsvRecordReader.get} maps an empty cell to {@code null} and its {@code getInt} override
   * carries the {@code !isEmpty()} guard; the other four accessors are the interface defaults,
   * which read through that same guarded {@code get()}. Pinned so the CSV source cannot start
   * handing an empty cell to a parse.
   */
  @Test
  void emptyCsvCellsLeaveEveryTypedPropertyUnset() throws Exception {
    final String vertices = write("empty-value-vertices.csv",
        "id;name;score;views;rating;embedding;tags",
        "1;alice;;;;;",
        "2;bob;7;9000000000;4.5;[0.1,0.2];[\"java\"]");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new CsvRowSource(vertices, ';', 0), v -> {
          v.id("id");
          v.property("name", "name");
          v.intProperty("score", "score");
          v.longProperty("views", "views");
          v.doubleProperty("rating", "rating");
          v.floatArrayProperty("embedding", "embedding");
          v.listProperty("tags", "tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertThat(intOf("alice", "score")).isZero();
    assertThat(longOf("alice", "views")).isZero();
    assertThat(doubleOf("alice", "rating")).isZero();
    assertThat(hasProperty("alice", "embedding")).isFalse();
    assertThat(hasProperty("alice", "tags")).isFalse();

    assertThat(intOf("bob", "score")).isEqualTo(7);
    assertThat(longOf("bob", "views")).isEqualTo(9_000_000_000L);
    assertThat(doubleOf("bob", "rating")).isEqualTo(4.5);
    assertThat(vertexOf("bob").get("embedding")).isEqualTo(new float[] { 0.1f, 0.2f });
    assertThat(asList(vertexOf("bob").get("tags"))).containsExactly("java");
  }

  /**
   * {@code XmlRowSource}'s two readers override only {@code get} and {@code getInt}, and the
   * {@code getInt} overrides carry the {@code !isEmpty()} guard; the rest are the interface
   * defaults, which guard on {@code isEmpty()} themselves. Pinned for the same reason as CSV.
   */
  @Test
  void emptyXmlAttributesLeaveEveryTypedPropertyUnset() throws Exception {
    final String vertices = write("empty-value-vertices.xml",
        "<users>",
        "  <row Id=\"1\" Name=\"alice\" Score=\"\" Views=\"\" Rating=\"\" Embedding=\"\" Tags=\"\" />",
        "  <row Id=\"2\" Name=\"bob\" Score=\"7\" Views=\"9000000000\" Rating=\"4.5\" "
            + "Embedding=\"[0.1,0.2]\" Tags=\"[&quot;java&quot;]\" />",
        "</users>");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new XmlRowSource(vertices), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.intProperty("score", "Score");
          v.longProperty("views", "Views");
          v.doubleProperty("rating", "Rating");
          v.floatArrayProperty("embedding", "Embedding");
          v.listProperty("tags", "Tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    assertThat(intOf("alice", "score")).isZero();
    assertThat(longOf("alice", "views")).isZero();
    assertThat(doubleOf("alice", "rating")).isZero();
    assertThat(hasProperty("alice", "embedding")).isFalse();
    assertThat(hasProperty("alice", "tags")).isFalse();

    assertThat(intOf("bob", "score")).isEqualTo(7);
    assertThat(longOf("bob", "views")).isEqualTo(9_000_000_000L);
    assertThat(doubleOf("bob", "rating")).isEqualTo(4.5);
    assertThat(vertexOf("bob").get("embedding")).isEqualTo(new float[] { 0.1f, 0.2f });
    assertThat(asList(vertexOf("bob").get("tags"))).containsExactly("java");
  }

  // ───────────────────────────────────────────────────────────────────
  //  What must NOT change
  // ───────────────────────────────────────────────────────────────────

  /**
   * Empty means "not set"; a value that is present but not a number is still a data error, and
   * still names the property and the source attribute. The guard is {@code isEmpty()}, not
   * {@code isBlank()} - the same split #7265 settled for DATETIME.
   */
  @Test
  void aMalformedOrBlankJsonlNumberIsStillReported() throws Exception {
    final String malformed = write("malformed-int-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": \"not-a-number\"}");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(malformed), v -> {
          v.id("id");
          v.property("name", "name");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score")
          .hasMessageContaining("declared as an integer");
    }

    if (database.isTransactionActive())
      database.rollback();

    final String blank = write("blank-int-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": \" \"}");

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(blank), v -> {
          v.id("id");
          v.property("name", "name");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("score");
    }
  }

  /**
   * A JSON {@code null}, a JSON {@code 0} and an empty JSON array are values in their own right,
   * not "empty": the fix narrows the new guard to an empty {@code String} so none of them shifts.
   */
  @Test
  void jsonNullZeroAndEmptyArrayKeepTheirOwnMeaning() throws Exception {
    importUsers("null-zero-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": null,  \"tags\": null}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"score\": 0,     \"tags\": []}",
        "{\"id\": \"3\", \"name\": \"carol\", \"score\": 5,     \"tags\": [\"x\"]}",
        v -> {
          v.intProperty("score", "score");
          v.listProperty("tags", "tags");
        });

    // JSON null was already "not set" and stays that way
    assertThat(intOf("alice", "score")).isZero();
    assertThat(hasProperty("alice", "tags")).isFalse();
    // a real 0 and a real empty array are values, and must not become "not set"
    assertThat(intOf("bob", "score")).isZero();
    assertThat(asList(vertexOf("bob").get("tags"))).isEmpty();
    assertThat(intOf("carol", "score")).isEqualTo(5);
    assertThat(asList(vertexOf("carol").get("tags"))).containsExactly("x");
  }

  /**
   * A JSONL file produced by stringifying a CSV export carries its numbers quoted, so the same
   * column that holds {@code ""} on a blank row holds {@code "7"} on a populated one. Gson parses a
   * quoted number lazily, so {@code getInt} on {@code "7"} has always returned 7 - pinned here
   * because the new guard sits on exactly that path and must not start rejecting it.
   */
  @Test
  void aQuotedNumberIsStillImported() throws Exception {
    importUsers("quoted-number-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": \"\",  \"views\": \"\",           \"rating\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"score\": \"7\", \"views\": \"9000000000\", \"rating\": \"4.5\"}",
        "{\"id\": \"3\", \"name\": \"carol\", \"score\": 7,    \"views\": 9000000000,    \"rating\": 4.5}",
        v -> {
          v.intProperty("score", "score");
          v.longProperty("views", "views");
          v.doubleProperty("rating", "rating");
        });

    assertThat(intOf("alice", "score")).isZero();
    assertThat(longOf("alice", "views")).isZero();
    assertThat(doubleOf("alice", "rating")).isZero();

    // quoted and unquoted have to agree, or a stringified export imports differently row by row
    assertThat(intOf("bob", "score")).isEqualTo(intOf("carol", "score")).isEqualTo(7);
    assertThat(longOf("bob", "views")).isEqualTo(longOf("carol", "views")).isEqualTo(9_000_000_000L);
    assertThat(doubleOf("bob", "rating")).isEqualTo(doubleOf("carol", "rating")).isEqualTo(4.5);
  }

  /**
   * The whole point of the issue: one blank optional column in the middle of a JSONL file used to
   * end the import, so the rows after it were never loaded.
   */
  @Test
  void aBlankOptionalColumnDoesNotAbortTheImport() throws Exception {
    final String vertices = write("blank-column-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"score\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"score\": 7}");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          v.intProperty("score", "score");
        })
        .build()) {

      assertThatCode(importer::run).doesNotThrowAnyException();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }
  }

  // ───────────────────────────────────────────────────────────────────
  //  Helpers
  // ───────────────────────────────────────────────────────────────────

  private void importUsers(final String fileName, final String line1, final String line2, final String line3,
                           final Consumer<GraphImporter.VertexConfig> properties) throws Exception {
    final String vertices = write(fileName, line1, line2, line3);

    database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          properties.accept(v);
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }
  }

  private Vertex vertexOf(final String name) {
    final Vertex[] result = new Vertex[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM User WHERE name = ?", name)) {
        assertThat(rs.hasNext()).isTrue();
        result[0] = rs.next().getVertex().get();
      }
    });
    return result[0];
  }

  private boolean hasProperty(final String name, final String property) {
    return vertexOf(name).has(property);
  }

  private int intOf(final String name, final String property) {
    final Vertex v = vertexOf(name);
    return v.has(property) ? v.getInteger(property) : 0;
  }

  private long longOf(final String name, final String property) {
    final Vertex v = vertexOf(name);
    return v.has(property) ? v.getLong(property) : 0L;
  }

  private double doubleOf(final String name, final String property) {
    final Vertex v = vertexOf(name);
    return v.has(property) ? v.getDouble(property) : 0.0;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> asList(final Object value) {
    return (List<Object>) value;
  }

  private Edge edgeOfKind(final String kind) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Knows WHERE kind = ?", kind)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getEdge().get();
    }
  }
}

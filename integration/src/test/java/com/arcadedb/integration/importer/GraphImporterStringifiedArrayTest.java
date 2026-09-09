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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A vector or list column that arrives as a quoted string - {@code "embedding": "[0.1,0.2]"} - is
 * the textual array form {@code GraphImporter.RecordReader}'s default accessors parse on purpose,
 * and it is what a CSV export converted to JSONL carries (issue #7285, follow-up to #7269).
 * <p>
 * {@code JsonlRecordReader} overrides {@code getFloatArray} and {@code getList} and went straight
 * to {@code JSONObject.getJSONArray}, which throws on a string, so {@code readProperty} rethrew it
 * as {@code badValue} from inside the row loop and the import ended. A quoted <i>number</i> already
 * imported, because Gson parses one lazily, so the same stringified export loaded its int, long and
 * double columns and then died on its vector column.
 * <p>
 * Both passes get their own case: the edge-source pass routes FLOAT_ARRAY and LIST through
 * {@code processEdgeSource}'s default branch into the same {@code readProperty}, a second route the
 * vertex cases do not exercise. CSV and XML are pinned against the JSONL result, since agreeing
 * with them is the whole point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterStringifiedArrayTest {

  private static final String DB_PATH   = "target/databases/graph-importer-stringified-array-test";
  private static final String DATA_PATH = "target/test-data/graph-importer-stringified-array";

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
      // A backstop, not a workaround: GraphImporter.processVertexSource resolves its own
      // transaction in a finally block, so an aborted import is expected to leave nothing active -
      // aMalformedStringifiedArrayIsStillReported asserts that outright. This only keeps one real
      // failure from cascading into "already in use" errors in every later test of the class.
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
   * {@code JsonlRecordReader.getFloatArray}: the quoted form used to reach
   * {@code JSONObject.getJSONArray}, which throws "is not a JSON array" on a string. The quoted and
   * the native row have to produce the same vector, or a stringified export imports differently
   * row by row.
   */
  @Test
  void aStringifiedVectorImportsOnTheVertexPass() throws Exception {
    importUsers("stringified-vector-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"embedding\": \"[0.1,0.2,0.3]\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"embedding\": [0.1, 0.2, 0.3]}",
        "{\"id\": \"3\", \"name\": \"carol\", \"embedding\": \"[0.5]\"}",
        v -> v.floatArrayProperty("embedding", "embedding"));

    assertThat(vertexOf("alice").get("embedding"))
        .as("a quoted vector is the textual form the interface default parses")
        .isEqualTo(new float[] { 0.1f, 0.2f, 0.3f });
    assertThat(vertexOf("bob").get("embedding"))
        .as("the native JSON array path must not regress")
        .isEqualTo(new float[] { 0.1f, 0.2f, 0.3f });
    assertThat(vertexOf("carol").get("embedding")).isEqualTo(new float[] { 0.5f });
  }

  /**
   * {@code JsonlRecordReader.getList}, same shape through {@code JSONObject.getJSONArray}. The
   * textual form here is a JSON array literal, which is what the interface default parses with
   * {@code new JSONArray(v)}.
   */
  @Test
  void aStringifiedListImportsOnTheVertexPass() throws Exception {
    importUsers("stringified-list-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"tags\": \"[\\\"java\\\", \\\"sql\\\"]\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"tags\": [\"java\", \"sql\"]}",
        "{\"id\": \"3\", \"name\": \"carol\", \"tags\": \"[1, 2]\"}",
        v -> v.listProperty("tags", "tags"));

    assertThat(asList(vertexOf("alice").get("tags")))
        .as("a quoted list is the textual form the interface default parses")
        .containsExactly("java", "sql");
    assertThat(asList(vertexOf("bob").get("tags")))
        .as("the native JSON array path must not regress")
        .containsExactly("java", "sql");
    // the elements keep the types the JSON text declares, exactly as the native array would
    assertThat(asList(vertexOf("carol").get("tags"))).containsExactly(1, 2);
  }

  // ───────────────────────────────────────────────────────────────────
  //  Edge-source pass
  // ───────────────────────────────────────────────────────────────────

  /**
   * FLOAT_ARRAY and LIST fall into {@code processEdgeSource}'s default branch, which routes them
   * through the same {@code readProperty} the vertex pass uses. A second route to the two
   * accessors, so it gets its own case rather than being assumed from the vertex ones.
   */
  @Test
  void aStringifiedVectorAndListImportOnTheEdgeSourcePass() throws Exception {
    final String vertices = write("stringified-edge-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\"}",
        "{\"id\": \"2\", \"name\": \"bob\"}");
    final String edges = write("stringified-array-edges.jsonl",
        "{\"from\": \"1\", \"to\": \"2\", \"kind\": \"quoted\", \"embedding\": \"[0.5,0.6]\", \"tags\": \"[\\\"x\\\"]\"}",
        "{\"from\": \"2\", \"to\": \"1\", \"kind\": \"native\", \"embedding\": [0.5, 0.6],   \"tags\": [\"x\"]}");

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
      final Edge quoted = edgeOfKind("quoted");
      assertThat(quoted.get("embedding")).isEqualTo(new float[] { 0.5f, 0.6f });
      assertThat(asList(quoted.get("tags"))).containsExactly("x");

      final Edge nativeRow = edgeOfKind("native");
      assertThat(nativeRow.get("embedding")).isEqualTo(new float[] { 0.5f, 0.6f });
      assertThat(asList(nativeRow.get("tags"))).containsExactly("x");
    });
  }

  // ───────────────────────────────────────────────────────────────────
  //  Agreement with the sources that already parsed the text
  // ───────────────────────────────────────────────────────────────────

  /**
   * The invariant stated as one assertion: the same textual array reads the same on all three
   * sources. CSV and XML inherit the interface defaults and have always parsed it; JSONL is the
   * one that did not.
   */
  @Test
  void aStringifiedArrayReadsTheSameOnCsvAndXmlAsOnJsonl() throws Exception {
    final String jsonl = write("agreement-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"row\", \"embedding\": \"[0.1,0.2]\", \"tags\": \"[\\\"java\\\"]\"}");
    final String csv = write("agreement-vertices.csv",
        "id;name;embedding;tags",
        "1;row;[0.1,0.2];[\"java\"]");
    final String xml = write("agreement-vertices.xml",
        "<users>",
        "  <row Id=\"1\" Name=\"row\" Embedding=\"[0.1,0.2]\" Tags=\"[&quot;java&quot;]\" />",
        "</users>");

    // one type per source, so each row is read back through the source that produced it
    database.transaction(() -> {
      database.getSchema().createVertexType("JsonlUser");
      database.getSchema().createVertexType("CsvUser");
      database.getSchema().createVertexType("XmlUser");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("JsonlUser", new JsonlRowSource(jsonl), this::lowercaseArrayProperties)
        .vertex("CsvUser", new CsvRowSource(csv, ';', 0), this::lowercaseArrayProperties)
        .vertex("XmlUser", new XmlRowSource(xml), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.floatArrayProperty("embedding", "Embedding");
          v.listProperty("tags", "Tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    for (final String type : new String[] { "JsonlUser", "CsvUser", "XmlUser" }) {
      final Vertex v = onlyVertexOf(type);
      assertThat(v.get("embedding"))
          .as("%s parses the textual vector form", type)
          .isEqualTo(new float[] { 0.1f, 0.2f });
      assertThat(asList(v.get("tags")))
          .as("%s parses the textual list form", type)
          .containsExactly("java");
    }
  }

  /**
   * A quoted {@code "[]"} is an empty array, the same value the native {@code []} carries - not
   * "not set". Only an empty <i>string</i> means "not set", which is the split #7269 settled.
   */
  @Test
  void aQuotedEmptyArrayMeansAnEmptyArrayJustLikeTheNativeOne() throws Exception {
    importUsers("quoted-empty-array-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"embedding\": \"[]\", \"tags\": \"[]\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"embedding\": [],     \"tags\": []}",
        "{\"id\": \"3\", \"name\": \"carol\", \"embedding\": \"[7]\", \"tags\": \"[7]\"}",
        v -> {
          v.floatArrayProperty("embedding", "embedding");
          v.listProperty("tags", "tags");
        });

    assertThat(vertexOf("alice").get("embedding")).isEqualTo(new float[0]);
    assertThat(asList(vertexOf("alice").get("tags"))).isEmpty();
    assertThat(vertexOf("bob").get("embedding")).isEqualTo(new float[0]);
    assertThat(asList(vertexOf("bob").get("tags"))).isEmpty();
    assertThat(vertexOf("carol").get("embedding")).isEqualTo(new float[] { 7f });
  }

  /**
   * The config-driven entry point, which is how an operator actually reaches this: {@code fromJSON}
   * picks {@code JsonlRowSource} from the {@code .jsonl} extension and turns a {@code "vector:"} /
   * {@code "list:"} property spec into the same {@code floatArrayProperty} / {@code listProperty}
   * the programmatic builder above declares. Same accessors, reached without writing Java.
   */
  @Test
  void aStringifiedArrayImportsThroughTheJsonConfigEntryPoint() throws Exception {
    write("config-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"embedding\": \"[0.1,0.2]\", \"tags\": \"[\\\"java\\\"]\"}");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    final String config = """
        {
          "vertices": [
            {
              "type": "User",
              "file": "config-vertices.jsonl",
              "id": "id",
              "properties": {
                "name": "name",
                "embedding": "vector:embedding",
                "tags": "list:tags"
              }
            }
          ]
        }""";

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, dataDir.getAbsolutePath())) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(1);
    }

    assertThat(vertexOf("alice").get("embedding")).isEqualTo(new float[] { 0.1f, 0.2f });
    assertThat(asList(vertexOf("alice").get("tags"))).containsExactly("java");
  }

  // ───────────────────────────────────────────────────────────────────
  //  What must NOT change
  // ───────────────────────────────────────────────────────────────────

  /**
   * A whitespace-only value is where the two accessors genuinely disagree, and the disagreement is
   * inherited from what they parse with rather than from the source: {@code VectorUtils} treats
   * {@code "  "} as an empty vector, {@code new JSONArray("  ")} rejects it. Widening the JSONL
   * path to the textual form therefore hands a blank vector column an empty {@code float[]} where
   * it used to end the import.
   * <p>
   * That is the intended outcome, not a side effect to fix here: the point of the issue is that
   * JSONL should read a textual array the way CSV and XML read it, and this pins that the three now
   * agree on the awkward input as well as on the ordinary one. Note that {@code ""} is a different
   * case and still means "not set" on all three - see
   * {@link #anEmptyStringIsStillNotSetForBothArrayAccessors}.
   */
  @Test
  void aWhitespaceOnlyValueReadsTheSameOnAllThreeSources() throws Exception {
    final String jsonl = write("blank-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"row\", \"embedding\": \"  \"}");
    final String csv = write("blank-vertices.csv",
        "id;name;embedding",
        "1;row;  ");
    final String xml = write("blank-vertices.xml",
        "<users>",
        "  <row Id=\"1\" Name=\"row\" Embedding=\"  \" />",
        "</users>");

    database.transaction(() -> {
      database.getSchema().createVertexType("JsonlUser");
      database.getSchema().createVertexType("CsvUser");
      database.getSchema().createVertexType("XmlUser");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("JsonlUser", new JsonlRowSource(jsonl), v -> {
          v.id("id");
          v.property("name", "name");
          v.floatArrayProperty("embedding", "embedding");
        })
        .vertex("CsvUser", new CsvRowSource(csv, ';', 0), v -> {
          v.id("id");
          v.property("name", "name");
          v.floatArrayProperty("embedding", "embedding");
        })
        .vertex("XmlUser", new XmlRowSource(xml), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.floatArrayProperty("embedding", "Embedding");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    for (final String type : new String[] { "JsonlUser", "CsvUser", "XmlUser" })
      assertThat(onlyVertexOf(type).get("embedding"))
          .as("%s reads a whitespace-only vector column the same way", type)
          .isEqualTo(new float[0]);
  }

  /**
   * The list half of the case above, pinned rather than left to be re-derived: a whitespace-only
   * value is where the two accessors genuinely disagree, because of what each parses the text
   * <i>with</i> rather than because of the source. {@code VectorUtils.toFloatArray("  ")} trims to
   * nothing and answers an empty vector, while {@code new JSONArray("  ")} rejects it, so a blank
   * list column still aborts the import where a blank vector column no longer does.
   * <p>
   * The asymmetry is inherited from the interface defaults and predates this change - CSV and XML
   * have always had it - so it is pinned on all three sources rather than fixed here. Whether the
   * empty-vector answer or the error is the better one is a question for the accessors' contract,
   * not for a JSONL override that exists to stop diverging from them.
   */
  @Test
  void aWhitespaceOnlyListStillAbortsOnEveryThreeSourcesAlike() throws Exception {
    final String jsonl = write("blank-list-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"row\", \"tags\": \"  \"}");
    final String csv = write("blank-list-vertices.csv",
        "id;name;tags",
        "1;row;  ");
    final String xml = write("blank-list-vertices.xml",
        "<users>",
        "  <row Id=\"1\" Name=\"row\" Tags=\"  \" />",
        "</users>");

    database.transaction(() -> database.getSchema().createVertexType("User"));

    assertThatThrownBy(() -> runSingleListSource(new JsonlRowSource(jsonl), "id", "name", "tags"))
        .as("JSONL rejects a whitespace-only list value")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags")
        .hasMessageContaining("declared as a list");

    assertThat(database.isTransactionActive()).isFalse();

    assertThatThrownBy(() -> runSingleListSource(new CsvRowSource(csv, ';', 0), "id", "name", "tags"))
        .as("CSV rejects it the same way, and always did")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags");

    assertThat(database.isTransactionActive()).isFalse();

    assertThatThrownBy(() -> runSingleListSource(new XmlRowSource(xml), "Id", "Name", "Tags"))
        .as("XML rejects it the same way, and always did")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags");
  }

  private void runSingleListSource(final GraphImporter.RecordSource source, final String idAttr,
                                   final String nameAttr, final String tagsAttr) throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", source, v -> {
          v.id(idAttr);
          v.property("name", nameAttr);
          v.listProperty("tags", tagsAttr);
        })
        .build()) {

      importer.run();
    }
  }

  /**
   * #7269's answer for an empty value survives: {@code ""} is still "not set" for both accessors,
   * so the property is simply absent rather than an empty array.
   */
  @Test
  void anEmptyStringIsStillNotSetForBothArrayAccessors() throws Exception {
    importUsers("still-empty-vertices.jsonl",
        "{\"id\": \"1\", \"name\": \"alice\", \"embedding\": \"\",         \"tags\": \"\"}",
        "{\"id\": \"2\", \"name\": \"bob\",   \"embedding\": \"[0.1]\",    \"tags\": \"[\\\"x\\\"]\"}",
        "{\"id\": \"3\", \"name\": \"carol\", \"embedding\": null,         \"tags\": null}",
        v -> {
          v.floatArrayProperty("embedding", "embedding");
          v.listProperty("tags", "tags");
        });

    assertThat(vertexOf("alice").has("embedding")).isFalse();
    assertThat(vertexOf("alice").has("tags")).isFalse();
    assertThat(vertexOf("bob").get("embedding")).isEqualTo(new float[] { 0.1f });
    assertThat(asList(vertexOf("bob").get("tags"))).containsExactly("x");
    assertThat(vertexOf("carol").has("embedding")).isFalse();
    assertThat(vertexOf("carol").has("tags")).isFalse();
  }

  /**
   * Text that is not an array is still a data error, and still names the property and the source
   * attribute rather than surfacing as a bare parse failure from inside the row loop.
   * <p>
   * The truncated case is deliberate. The interface default calls {@code checkNotSplit}, whose
   * message blames the source's field separator - accurate for CSV, wrong for JSONL, which has
   * none ({@code RecordSource.fieldSeparator()} returns {@code null} for it). The JSONL path must
   * therefore report the parse failure itself, not offer a delimiter the format does not have.
   */
  @Test
  void aMalformedStringifiedArrayIsStillReported() throws Exception {
    assertThatThrownBy(() -> importVector("malformed-vector-vertices.jsonl", "\"not-an-array\""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedding")
        .hasMessageContaining("declared as a vector");

    // the abort must not leave a transaction on the stack, or the next import would fail for a
    // reason unrelated to the value it is testing
    assertThat(database.isTransactionActive()).isFalse();

    final Throwable truncated = catchThrowable(() -> importVector("truncated-vector-vertices.jsonl", "\"[0.1\""));
    assertThat(truncated)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedding")
        .hasMessageContaining("declared as a vector");
    assertThat(truncated.getMessage())
        .as("JSONL has no field separator, so a truncated array must not be blamed on a delimiter")
        .doesNotContain("delimiter");

    assertThatThrownBy(() -> importList("malformed-list-vertices.jsonl", "\"not-an-array\""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags")
        .hasMessageContaining("declared as a list");

    assertThat(database.isTransactionActive()).isFalse();

    // the truncated case for the list accessor too, so the pairing is pinned symmetrically rather
    // than inferred from the vector one: this goes through new JSONArray(text) instead of
    // VectorUtils, a different parser reached by a different branch
    final Throwable truncatedList = catchThrowable(() -> importList("truncated-list-vertices.jsonl", "\"[\\\"x\\\"\""));
    assertThat(truncatedList)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags")
        .hasMessageContaining("declared as a list");
    assertThat(truncatedList.getMessage())
        .as("JSONL has no field separator here either, so this must not be blamed on a delimiter")
        .doesNotContain("delimiter");
  }

  /**
   * A JSON scalar under an array property was never the textual form and must keep failing the way
   * it always has: {@code getJSONArray}'s "is not a JSON array", reported as {@code badValue}.
   */
  @Test
  void aScalarUnderAnArrayPropertyIsStillReported() throws Exception {
    assertThatThrownBy(() -> importVector("scalar-vector-vertices.jsonl", "7"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedding")
        .hasMessageContaining("declared as a vector");

    assertThat(database.isTransactionActive()).isFalse();

    assertThatThrownBy(() -> importList("object-list-vertices.jsonl", "{\"a\": 1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags")
        .hasMessageContaining("declared as a list");
  }

  // ───────────────────────────────────────────────────────────────────
  //  Helpers
  // ───────────────────────────────────────────────────────────────────

  private void lowercaseArrayProperties(final GraphImporter.VertexConfig v) {
    v.id("id");
    v.property("name", "name");
    v.floatArrayProperty("embedding", "embedding");
    v.listProperty("tags", "tags");
  }

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

  /** Imports a single row whose {@code embedding} attribute holds the given raw JSON value. */
  private void importVector(final String fileName, final String rawJsonValue) throws Exception {
    importOne(fileName, "embedding", rawJsonValue, v -> v.floatArrayProperty("embedding", "embedding"));
  }

  /** Imports a single row whose {@code tags} attribute holds the given raw JSON value. */
  private void importList(final String fileName, final String rawJsonValue) throws Exception {
    importOne(fileName, "tags", rawJsonValue, v -> v.listProperty("tags", "tags"));
  }

  private void importOne(final String fileName, final String attribute, final String rawJsonValue,
                         final Consumer<GraphImporter.VertexConfig> properties) throws Exception {
    final String vertices = write(fileName,
        "{\"id\": \"1\", \"name\": \"alice\", \"" + attribute + "\": " + rawJsonValue + "}");

    if (!database.getSchema().existsType("User"))
      database.transaction(() -> database.getSchema().createVertexType("User"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("User", new JsonlRowSource(vertices), v -> {
          v.id("id");
          v.property("name", "name");
          properties.accept(v);
        })
        .build()) {

      importer.run();
    }
  }

  private Vertex onlyVertexOf(final String typeName) {
    final Vertex[] result = new Vertex[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
        assertThat(rs.hasNext()).isTrue();
        result[0] = rs.next().getVertex().get();
        assertThat(rs.hasNext()).as("%s holds exactly one row", typeName).isFalse();
      }
    });
    return result[0];
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

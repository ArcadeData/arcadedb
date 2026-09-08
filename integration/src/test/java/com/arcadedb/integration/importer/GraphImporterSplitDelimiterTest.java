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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The split-field delimiter is declared as a {@code String} and has to behave like one: the whole string
 * separates the values, not only its first character. Both walkers - the inline one in {@code collectEdge()}
 * and the deferred one for a self-referencing split - are exercised, through the fluent API and through the
 * JSON configuration, because a delimiter reduced to {@code charAt(0)} leaves every value after the first
 * carrying the rest of the delimiter as a prefix, which resolves against nothing and is reported as a data
 * problem the operator does not have (issue #7263).
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class GraphImporterSplitDelimiterTest {

  private static final String DB_PATH   = "target/databases/graph-importer-split-delimiter-test";
  private static final String DATA_PATH = "target/test-data/graph-importer-split-delimiter";

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
      if (database.isOpen()) {
        // a test whose importer threw mid-import can leave a transaction open, and drop() refuses
        // one - which would leak the instance and fail every later test's setup instead of this one
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(DATA_PATH));
  }

  /**
   * The reporter's case, on the inline walker: a {@code ", "} delimiter over {@code "scifi, drama, horror"}.
   * Splitting on {@code ','} alone leaves {@code " drama"} and {@code " horror"} still carrying the space,
   * so two of the three edges resolve against nothing.
   */
  @Test
  void multiCharacterDelimiterSplitsOnTheWholeString() throws Exception {
    final String topics = write("multichar-topics.csv",
        "Code",
        "scifi",
        "drama",
        "horror");
    // ';' separates the CSV fields so that the ", " inside the Tags field is not a field separator too
    final String posts = write("multichar-posts.csv",
        "Id;Tags",
        "1;scifi, drama, horror",
        "2;drama");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(topics), v -> {
          v.idByName("Code");
          v.property("code", "Code");
        })
        .vertex("Post", new CsvRowSource(posts, ';', 0), v -> {
          v.id("Id");
          v.intProperty("postId", "Id");
          v.splitEdge("Tags", "Tagged", "Topic", ", ");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(4);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Post", "postId", 1, "Tagged", "code"))
        .containsExactlyInAnyOrder("scifi", "drama", "horror");
    assertThat(outgoingTargets("Post", "postId", 2, "Tagged", "code")).containsExactly("drama");
  }

  /**
   * The same field, wrapped in the delimiter at both ends. The wrapping is optional by convention and has to
   * consume the whole delimiter, not its first character - a leading {@code ", "} left as a bare space would
   * turn the first value into {@code " scifi"}.
   */
  @Test
  void aWrappedMultiCharacterDelimiterIsConsumedWhole() throws Exception {
    final String topics = write("wrapped-topics.csv",
        "Code",
        "scifi",
        "drama");
    final String posts = write("wrapped-posts.csv",
        "Id;Tags",
        "1;, scifi, drama");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(topics), v -> {
          v.idByName("Code");
          v.property("code", "Code");
        })
        .vertex("Post", new CsvRowSource(posts, ';', 0), v -> {
          v.id("Id");
          v.intProperty("postId", "Id");
          v.splitEdge("Tags", "Tagged", "Topic", ", ");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(2);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Post", "postId", 1, "Tagged", "code"))
        .containsExactlyInAnyOrder("scifi", "drama");
  }

  /**
   * The deferred walker, reached when the split field points at the type it lives on. It is a second copy of
   * the same loop and had the same {@code charAt(0)} truncation.
   */
  @Test
  void multiCharacterDelimiterSplitsOnTheWholeStringOnTheDeferredPath() throws Exception {
    final String vertices = write("multichar-tree.csv",
        "Code;Related",
        "a;b, c",
        "b;c",
        "c;");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createEdgeType("RelatedTo");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(vertices, ';', 0), v -> {
          v.idByName("Code");
          v.property("code", "Code");
          v.splitEdge("Related", "RelatedTo", "Topic", ", ");
        })
        .build()) {

      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(3);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Topic", "code", "a", "RelatedTo", "code"))
        .containsExactlyInAnyOrder("b", "c");
    assertThat(outgoingTargets("Topic", "code", "b", "RelatedTo", "code")).containsExactly("c");
  }

  /**
   * The JSON configuration passes {@code "split"} straight through to {@code splitEdge()}, so it is the
   * second way into both walkers.
   */
  @Test
  void jsonConfigHonoursAMultiCharacterSplitDelimiter() throws Exception {
    write("json-topics.csv",
        "Code",
        "scifi",
        "drama",
        "horror");
    write("json-posts.csv",
        "Id;Tags",
        "1;scifi, drama, horror");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    final String config = """
        {
          "vertices": [
            { "type": "Topic", "file": "json-topics.csv", "nameId": "Code",
              "properties": { "code": "Code" } },
            { "type": "Post", "file": "json-posts.csv", "id": "Id", "delimiter": ";",
              "properties": { "postId": "int:Id" },
              "edges": [ { "attribute": "Tags", "edge": "Tagged", "target": "Topic", "split": ", " } ] }
          ]
        }
        """;

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, dataDir.getAbsolutePath())) {
      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(3);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }

    assertThat(outgoingTargets("Post", "postId", 1, "Tagged", "code"))
        .containsExactlyInAnyOrder("scifi", "drama", "horror");
  }

  /**
   * An empty delimiter used to reach {@code charAt(0)} from inside the pass-1 row loop, after vertices had
   * already been committed, and surface as a bare {@code StringIndexOutOfBoundsException}. It is a
   * configuration mistake and is reported as one, before a file is opened.
   */
  @Test
  void anEmptySplitDelimiterIsRejectedBeforeAnyRowIsRead() throws Exception {
    final String vertices = write("empty-delim.csv",
        "Code,Related",
        "a,b");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createEdgeType("RelatedTo");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(vertices), v -> {
          v.idByName("Code");
          v.splitEdge("Related", "RelatedTo", "Topic", "");
        })
        .build()) {
      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RelatedTo")
          .hasMessageContaining("Related")
          .hasMessageContaining("empty");

      assertThat(importer.getVertexCount()).isZero();
    }
  }

  /**
   * A null delimiter threw an NPE from the same place.
   */
  @Test
  void aNullSplitDelimiterIsRejectedBeforeAnyRowIsRead() throws Exception {
    final String vertices = write("null-delim.csv",
        "Code,Related",
        "a,b");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createEdgeType("RelatedTo");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(vertices), v -> {
          v.idByName("Code");
          v.splitEdge("Related", "RelatedTo", "Topic", null);
        })
        .build()) {
      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RelatedTo")
          .hasMessageContaining("no delimiter");

      assertThat(importer.getVertexCount()).isZero();
    }
  }

  /**
   * The same validation through the JSON configuration, which is where a hand-edited {@code "split": ""} is
   * most likely to come from.
   */
  @Test
  void jsonConfigRejectsAnEmptySplitDelimiter() throws Exception {
    write("json-empty-delim.csv",
        "Code,Related",
        "a,b");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createEdgeType("RelatedTo");
    });

    final String config = """
        {
          "vertices": [
            { "type": "Topic", "file": "json-empty-delim.csv", "nameId": "Code",
              "edges": [ { "attribute": "Related", "edge": "RelatedTo", "target": "Topic", "split": "" } ] }
          ]
        }
        """;

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, dataDir.getAbsolutePath())) {
      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RelatedTo")
          .hasMessageContaining("empty");
    }
  }

  /**
   * The same shape one level up: a CSV source's own {@code "delimiter"} is a JSON string consumed as
   * {@code charAt(0)}. {@link CsvRowSource} takes a {@code char}, so anything else is refused with a message
   * rather than silently truncated - or, when empty, thrown as a bare index-out-of-bounds.
   */
  @Test
  void aCsvSourceDelimiterMustBeASingleCharacter() throws Exception {
    write("csv-delim.csv",
        "Code",
        "a");

    database.transaction(() -> database.getSchema().createVertexType("Topic"));

    final String twoChars = """
        {
          "vertices": [
            { "type": "Topic", "file": "csv-delim.csv", "id": "Code", "delimiter": "||" }
          ]
        }
        """;
    assertThatThrownBy(() -> GraphImporter.fromJSON(database, twoChars, dataDir.getAbsolutePath()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("csv-delim.csv")
        .hasMessageContaining("single character");

    final String empty = """
        {
          "vertices": [
            { "type": "Topic", "file": "csv-delim.csv", "id": "Code", "delimiter": "" }
          ]
        }
        """;
    assertThatThrownBy(() -> GraphImporter.fromJSON(database, empty, dataDir.getAbsolutePath()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("csv-delim.csv")
        .hasMessageContaining("single character");
  }

  /**
   * The reporter's exact scenario (issue #7268): a comma-delimited CSV with a split delimiter that
   * contains the comma. {@link CsvRowSource} cuts the row into fields on the comma before the split
   * walker ever runs, so the collision is refused before any file is opened - the same register as
   * {@code checkNotSplit()}'s diagnosis of the identical mechanism on array-valued properties.
   */
  @Test
  void aSplitDelimiterContainingTheSourcesFieldSeparatorIsRejected() throws Exception {
    final String topics = write("collide-topics.csv",
        "Code",
        "scifi",
        "drama",
        "horror");
    final String posts = write("collide-posts.csv",
        "Id,Tags",
        "1,scifi, drama, horror");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new CsvRowSource(topics), v -> {
          v.idByName("Code");
          v.property("code", "Code");
        })
        .vertex("Post", new CsvRowSource(posts), v -> {
          v.id("Id");
          v.splitEdge("Tags", "Tagged", "Topic", ", ");
        })
        .build()) {
      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Tagged")
          .hasMessageContaining("Tags")
          .hasMessageContaining(", ")
          // the separator in its quoted form: a bare "," is already inside the ", " asserted above,
          // so it would hold whatever the message said about the separator - including nothing
          .hasMessageContaining("','");

      assertThat(importer.getVertexCount()).isZero();
    }
  }

  /**
   * The same validation through the JSON configuration, which is where a hand-picked split delimiter
   * that happens to collide with the source's default comma is most likely to come from.
   */
  @Test
  void jsonConfigRejectsASplitDelimiterCollidingWithTheSourcesFieldSeparator() throws Exception {
    write("json-collide-topics.csv",
        "Code",
        "scifi",
        "drama");
    write("json-collide-posts.csv",
        "Id,Tags",
        "1,scifi, drama");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    final String config = """
        {
          "vertices": [
            { "type": "Topic", "file": "json-collide-topics.csv", "nameId": "Code" },
            { "type": "Post", "file": "json-collide-posts.csv", "id": "Id",
              "edges": [ { "attribute": "Tags", "edge": "Tagged", "target": "Topic", "split": ", " } ] }
          ]
        }
        """;

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, dataDir.getAbsolutePath())) {
      assertThatThrownBy(importer::run)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Tagged")
          .hasMessageContaining(", ");
    }
  }

  /**
   * A source with no field-separator concept - JSONL fields are already distinct JSON values, nothing
   * cuts a row apart on a delimiter character - must never be flagged for a "collision" that cannot
   * happen. {@link JsonlRowSource#fieldSeparator()} answers the {@link GraphImporter.RecordSource}
   * default of {@code null}, and {@code validateEdgeTargets()} must treat that as "nothing to check"
   * rather than as a source whose separator happens to be absent.
   */
  @Test
  void aSplitEdgeOverANonDelimitedSourceIsNeverFlaggedAsAFieldSeparatorCollision() throws Exception {
    final String topics = write("jsonl-topics.jsonl",
        "{\"Code\": \"scifi\"}",
        "{\"Code\": \"drama\"}",
        "{\"Code\": \"horror\"}");
    final String posts = write("jsonl-posts.jsonl",
        "{\"Id\": 1, \"Tags\": \"scifi, drama, horror\"}");

    database.transaction(() -> {
      database.getSchema().createVertexType("Topic");
      database.getSchema().createVertexType("Post");
      database.getSchema().createEdgeType("Tagged");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Topic", new JsonlRowSource(topics), v -> v.idByName("Code"))
        .vertex("Post", new JsonlRowSource(posts), v -> {
          v.id("Id");
          v.splitEdge("Tags", "Tagged", "Topic", ", ");
        })
        .build()) {
      importer.run();

      assertThat(importer.getEdgeCount()).isEqualTo(3);
      assertThat(importer.getUnresolvedEdgeCount()).isZero();
    }
  }

  private String write(final String fileName, final String... lines) throws Exception {
    final File f = new File(dataDir, fileName);
    Files.write(f.toPath(), String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
    return f.getAbsolutePath();
  }

  private List<String> outgoingTargets(final String vertexType, final String keyProperty, final Object key,
                                       final String edgeType, final String targetProperty) {
    final List<String> result = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM " + vertexType + " WHERE " + keyProperty + " = ?", key)) {
        assertThat(rs.hasNext()).as("vertex %s.%s = %s", vertexType, keyProperty, key).isTrue();
        final Vertex v = rs.next().getVertex().get();
        for (final Edge e : v.getEdges(Vertex.DIRECTION.OUT, edgeType))
          result.add(e.getInVertex().asVertex().getString(targetProperty));
      }
    });
    return result;
  }
}

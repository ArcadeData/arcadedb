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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.integration.importer.graph.XmlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link GraphImporter} with JSON array properties: dense float vectors (embeddings) and
 * generic lists, from JSONL and from the textual form used by CSV/XML sources (issue #7185).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class GraphImporterArrayPropsTest {

  private static final String DB_PATH      = "target/databases/graph-importer-array-props-test";
  private static final String RESOURCE_DIR = new File("src/test/resources").getAbsolutePath();

  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void importVectorAndListFromJsonlViaApi() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
          v.id("id");
          v.property("title", "title");
          v.floatArrayProperty("embedding", "embedding");
          v.listProperty("tags", "tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Dune'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();

        final Object embedding = v.get("embedding");
        assertThat(embedding).isInstanceOf(float[].class);
        assertThat((float[]) embedding)
            .containsExactly(0.11142f, -0.21346f, 0.72326f, 0.19451f, -0.17215f);

        // The vector index consumes the stored value with zero conversion
        assertThat(VectorUtils.toFloatArray(embedding)).isSameAs(embedding);

        assertThat((List<Object>) v.get("tags")).containsExactly("scifi");
      }
    });

    // An integer-only JSON array is still stored as float[] when declared as a vector
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Neuromancer'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat((float[]) v.get("embedding")).containsExactly(1f, 2f, 3f, 4f, 5f);
        // An empty JSON array is a real value, not a missing one
        assertThat((List<?>) v.get("tags")).isEmpty();
      }
    });
  }

  @Test
  void importVectorAndListFromJsonlViaJsonConfig() throws Exception {
    final String json = """
        {
          "vertices": [
            {
              "type": "Book",
              "file": "importer-embeddings.jsonl",
              "id": "id",
              "properties": {
                "title": "title",
                "year": "int:publication_year",
                "embedding": "vector:embedding",
                "tags": "list:tags"
              }
            }
          ]
        }
        """;

    final JSONObject config = new JSONObject(json);
    GraphImporter.createSchemaFromConfig(database, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, RESOURCE_DIR)) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Dune'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat(v.getInteger("year")).isEqualTo(1965);
        assertThat((float[]) v.get("embedding"))
            .containsExactly(0.11142f, -0.21346f, 0.72326f, 0.19451f, -0.17215f);
        assertThat((List<Object>) v.get("tags")).containsExactly("scifi");
      }
    });
  }


  /**
   * Edge-source properties go through the same reader as vertex properties, so a {@code vector:} or
   * {@code list:} spec means the same thing on an edge. Before the property builders were unified,
   * an edge source parsed only int/long/double and silently dropped every other spec.
   */
  @Test
  void importEdgeSourcePropertiesOfEveryType() throws Exception {
    final String json = """
        {
          "vertices": [
            {
              "type": "Book",
              "file": "importer-embeddings.jsonl",
              "id": "id",
              "properties": { "title": "title" }
            }
          ],
          "edgeSources": [
            {
              "edge": "RelatedTo",
              "file": "importer-edge-props.jsonl",
              "from": "from:Book",
              "to": "to:Book",
              "properties": {
                "kind": "kind",
                "active": "bool:active",
                "score": "int:score",
                "weights": "vector:weights",
                "labels": "list:labels",
                "since": "datetime:since"
              }
            }
          ]
        }
        """;

    final JSONObject config = new JSONObject(json);
    GraphImporter.createSchemaFromConfig(database, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, RESOURCE_DIR)) {
      importer.run();
      assertThat(importer.getEdgeCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM RelatedTo WHERE kind = 'similar'")) {
        assertThat(rs.hasNext()).isTrue();
        final Edge e = rs.next().getEdge().get();
        assertThat(e.getBoolean("active")).isTrue();
        assertThat(e.getInteger("score")).isEqualTo(7);
        assertThat((float[]) e.get("weights")).containsExactly(0.5f, 0.25f, 0.125f);
        assertThat((List<Object>) e.get("labels")).containsExactly("a", "b");
        assertThat(e.getLocalDateTime("since")).isEqualTo(LocalDateTime.of(2023, 1, 15, 8, 30, 0));
      }
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM RelatedTo WHERE kind = 'cites'")) {
        assertThat(rs.hasNext()).isTrue();
        final Edge e = rs.next().getEdge().get();
        assertThat(e.getBoolean("active")).isFalse();
        assertThat((float[]) e.get("weights")).containsExactly(1f, 2f, 3f);
        assertThat((List<Object>) e.get("labels")).isEmpty();
        assertThat(e.getLocalDateTime("since")).isEqualTo(LocalDateTime.of(2024, 6, 20, 14, 45, 30));
      }
    });
  }


  /**
   * A vertex-defined edge and an edge source can declare the same edge type between the same vertex
   * types. Only the edge source contributes properties, so sharing one collector would index its
   * property buffers against a {@code srcIdx} that already holds the vertex-derived edges: the
   * source's values would land on those edges and the flush would then run off the end of the
   * buffer. Each edge source gets a collector of its own.
   */
  @Test
  void edgeSourceSharingATypeWithVertexDefinedEdgesKeepsItsPropertiesAligned() throws Exception {
    final String json = """
        {
          "vertices": [
            {
              "type": "Node",
              "file": "importer-edge-align-nodes.jsonl",
              "id": "id",
              "properties": { "name": "name" },
              "edges": [ { "attribute": "ref", "edge": "RelatedTo", "target": "Node" } ]
            }
          ],
          "edgeSources": [
            {
              "edge": "RelatedTo",
              "file": "importer-edge-align-edges.jsonl",
              "from": "from:Node",
              "to": "to:Node",
              "properties": { "kind": "kind", "weights": "vector:weights" }
            }
          ]
        }
        """;

    final JSONObject config = new JSONObject(json);
    GraphImporter.createSchemaFromConfig(database, config);

    try (final GraphImporter importer = GraphImporter.fromJSON(database, config, RESOURCE_DIR)) {
      importer.run();
      // 2 from the vertex source (A->B, B->C) plus 1 from the edge source (C->A)
      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      // exactly one edge carries the edge-source properties, and it is the one the edge source declared
      try (final ResultSet rs = database.query("sql", "SELECT FROM RelatedTo")) {
        int withProps = 0;
        while (rs.hasNext()) {
          final Edge e = rs.next().getEdge().get();
          if (e.getString("kind") != null) {
            withProps++;
            assertThat(e.getOutVertex().getString("name")).isEqualTo("C");
            assertThat(e.getInVertex().getString("name")).isEqualTo("A");
            assertThat(e.getString("kind")).isEqualTo("extra");
            assertThat((float[]) e.get("weights")).containsExactly(0.5f, 0.25f);
          } else {
            assertThat(e.get("weights")).isNull();
          }
        }
        assertThat(withProps).isEqualTo(1);
      }
    });
  }

  /**
   * {@code listProperty} has the same textual fallback as {@code floatArrayProperty} on a source
   * with no native array representation.
   */
  @Test
  void importListFromTextualFormOnCsv() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Place"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Place", new CsvRowSource(RESOURCE_DIR + "/importer-lists.csv", ';', 0), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.listProperty("tags", "Tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Alpha'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((List<Object>) rs.next().getVertex().get().get("tags")).containsExactly("red", "green");
      }
    });
  }

  /**
   * A scalar declared as a list fails the same way one declared as a vector does.
   */
  @Test
  void scalarDeclaredAsListFailsWithAClearMessage() {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
            v.id("id");
            v.listProperty("tags", "title");
          })
          .build()) {
        importer.run();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tags")
        .hasMessageContaining("title");
  }


  /**
   * A bad element inside the array reaches the same message as a whole attribute that is not an
   * array, though it arrives by a different exception path.
   */
  @Test
  void nonNumericElementInsideAVectorFailsWithAClearMessage() {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    for (final String file : new String[] { "importer-bad-vector.jsonl", "importer-null-in-vector.jsonl" })
      assertThatThrownBy(() -> {
        try (final GraphImporter importer = GraphImporter.builder(database)
            .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, file), v -> {
              v.id("id");
              v.floatArrayProperty("embedding", "embedding");
            })
            .build()) {
          importer.run();
        }
      }).as(file)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("embedding");
  }

  /**
   * A number that does not parse names its mapping too, rather than surfacing as a bare
   * NumberFormatException from inside the row loop.
   */
  @Test
  void scalarDeclaredAsIntFailsWithAClearMessage() {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
            v.id("id");
            v.intProperty("year", "title");
          })
          .build()) {
        importer.run();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("year")
        .hasMessageContaining("title");
  }


  /**
   * {@link CsvRowSource} splits on the delimiter with no quoting, so a comma-delimited file cuts an
   * unquoted {@code [0.1,0.2,0.3]} across several fields. The parse failure has to name the
   * delimiter rather than blame the data, which is the part a reader can act on.
   */
  @Test
  void vectorColumnSplitByTheCsvDelimiterSaysSo() {
    database.transaction(() -> database.getSchema().createVertexType("Place"));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("Place", new CsvRowSource(RESOURCE_DIR + "/importer-embeddings-comma.csv"), v -> {
            v.id("Id");
            v.floatArrayProperty("embedding", "Embedding");
          })
          .build()) {
        importer.run();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedding")
        .hasMessageContaining("delimiter");
  }


  /**
   * The other half of the same split: a fragment that closes an array it never opened.
   */
  @Test
  void arrayFragmentThatNeverOpenedSaysSo() {
    database.transaction(() -> database.getSchema().createVertexType("Place"));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("Place", new CsvRowSource(RESOURCE_DIR + "/importer-array-tail.csv", ';', 0), v -> {
            v.id("Id");
            v.floatArrayProperty("embedding", "Tail");
          })
          .build()) {
        importer.run();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("never opened")
        .hasMessageContaining("delimiter");
  }

  /**
   * XML is the third source format sharing the textual fallback, and an attribute value holds the
   * array's commas without any delimiter to collide with.
   */
  @Test
  void importVectorAndListFromXml() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Place"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Place", XmlRowSource.from(RESOURCE_DIR, "importer-embeddings.xml"), v -> {
          v.id("Id");
          v.property("name", "Name");
          v.floatArrayProperty("embedding", "Embedding");
          v.listProperty("tags", "Tags");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Alpha'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat((float[]) v.get("embedding")).containsExactly(-0.31142f, 0.51346f, -0.02326f);
        assertThat((List<Object>) v.get("tags")).containsExactly("red", "green");
      }
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Beta'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat((float[]) v.get("embedding")).containsExactly(0.5f, 0.25f, 0.125f);
        assertThat((List<Object>) v.get("tags")).isEmpty();
      }
    });
  }

  /**
   * Sources with no native array representation fall back to parsing the textual form, so a
   * {@code "[0.1,0.2]"} column round-trips into the same {@code float[]}.
   */
  @Test
  void importVectorFromTextualFormOnCsv() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Place"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Place", new CsvRowSource(RESOURCE_DIR + "/importer-embeddings.csv", ';', 0), v -> {
          v.id("Id");
          v.property("name", "Title");
          v.floatArrayProperty("embedding", "Embedding");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(2);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Place WHERE name = 'Beta'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        assertThat((float[]) v.get("embedding")).containsExactly(0.5f, 0.25f, 0.125f);
      }
    });
  }

  /**
   * A non-scalar JSON value mapped as a plain string used to abort the whole import with a
   * {@code JSONException} from {@code getString()}. It now degrades to the raw JSON text.
   */
  @Test
  void nonScalarJsonValueMappedAsStringDoesNotFailTheImport() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
          v.id("id");
          v.property("title", "title");
          v.property("embedding", "embedding");
          v.property("author", "author");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Dune'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex v = rs.next().getVertex().get();
        final String embedding = v.getString("embedding");
        assertThat(embedding).startsWith("[").endsWith("]");
        // VectorUtils parses the textual form, so even the stopgap mapping feeds a vector index
        assertThat(VectorUtils.toFloatArray(embedding))
            .containsExactly(0.11142f, -0.21346f, 0.72326f, 0.19451f, -0.17215f);
        assertThat(v.getString("author")).isNull();
      }
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Neuromancer'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getVertex().get().getString("author")).contains("William Gibson");
      }
    });
  }

  /**
   * An explicit JSON {@code null} is a missing value, not a parse failure.
   */
  @Test
  void explicitJsonNullIsTreatedAsMissing() throws Exception {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
          v.id("id");
          v.property("title", "title");
          v.intProperty("year", "publication_year");
        })
        .build()) {

      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE title = 'Neuromancer'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().getVertex().get().getInteger("year")).isEqualTo(0);
      }
    });
  }

  /**
   * A scalar declared as a vector must say so, naming the property and the attribute, instead of
   * failing with a bare {@code NumberFormatException} thousands of rows into a bulk load.
   */
  @Test
  void scalarDeclaredAsVectorFailsWithAClearMessage() {
    database.transaction(() -> database.getSchema().createVertexType("Book"));

    assertThatThrownBy(() -> {
      try (final GraphImporter importer = GraphImporter.builder(database)
          .vertex("Book", JsonlRowSource.from(RESOURCE_DIR, "importer-embeddings.jsonl"), v -> {
            v.id("id");
            v.floatArrayProperty("embedding", "title");
          })
          .build()) {
        importer.run();
      }
    }).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("embedding")
        .hasMessageContaining("title");
  }
}

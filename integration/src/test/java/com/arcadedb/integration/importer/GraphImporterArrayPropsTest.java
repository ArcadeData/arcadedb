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
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.integration.importer.graph.JsonlRowSource;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
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

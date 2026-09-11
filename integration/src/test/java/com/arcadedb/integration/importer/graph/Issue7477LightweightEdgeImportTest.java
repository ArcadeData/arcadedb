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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
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

/**
 * Issue #7477, reported as discussion #7473: a bulk load of a LIGHTWEIGHT edge type through {@link GraphImporter}
 * reported 75 million edges and then showed 0 in Studio, which read as an importer that had silently done nothing.
 * <p>
 * It had not: {@code GraphBatch} honours the type's LIGHTWEIGHT declaration whatever its (deprecated) per-batch
 * {@code withLightEdges} flag says, and the edges were connected. What answered zero was every read that named the
 * type, because a lightweight edge allocates no record and the bucket those reads scan is empty by construction.
 * This pins both halves: the importer connects the edges, and the query surface now sees them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7477LightweightEdgeImportTest {

  private static final String DB_PATH  = "target/databases/issue-7477-lightweight";
  private static final String BASE_DIR = "target/issue-7477-sources";

  private Database database;

  @BeforeEach
  void setUp() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();

    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      database.getSchema().createVertexType("WORK").createProperty("id", Type.LONG);
      database.command("sql", "CREATE EDGE TYPE CITE LIGHTWEIGHT");
    });

    Files.writeString(new File(BASE_DIR, "works.csv").toPath(), """
        id
        1
        2
        3
        """, StandardCharsets.UTF_8);

    Files.writeString(new File(BASE_DIR, "cites.csv").toPath(), """
        from_id,to_id
        1,2
        1,3
        2,3
        """, StandardCharsets.UTF_8);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  @Test
  void lightweightEdgesAreLoadedAndVisibleAfterImport() throws Exception {
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("WORK", CsvRowSource.from(BASE_DIR, "works.csv"), v -> {
          v.id("id");
          v.longProperty("id", "id");
        })
        .edgeSource("CITE", CsvRowSource.from(BASE_DIR, "cites.csv"), e -> {
          e.from("from_id", "WORK");
          e.to("to_id", "WORK");
        })
        .build()) {
      importer.run();
      assertThat(importer.getVertexCount()).isEqualTo(3);
      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    database.transaction(() -> {
      long outEdges = 0;
      long inEdges = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM WORK")) {
        while (rs.hasNext()) {
          final Vertex v = rs.next().getVertex().get();
          outEdges += v.countEdges(Vertex.DIRECTION.OUT, "CITE");
          inEdges += v.countEdges(Vertex.DIRECTION.IN, "CITE");
        }
      }
      assertThat(outEdges).as("outgoing CITE edges reachable from the WORK vertices").isEqualTo(3);
      assertThat(inEdges).as("incoming CITE edges reachable from the WORK vertices").isEqualTo(3);
    });

    // The query the report ran, and the count Studio's panel shows next to it.
    database.transaction(() -> {
      final List<String> pairs = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM CITE")) {
        while (rs.hasNext()) {
          final Edge edge = rs.next().getEdge().get();
          pairs.add(edge.getOut() + "->" + edge.getIn());
        }
      }
      assertThat(pairs).as("SELECT FROM a LIGHTWEIGHT edge type must not answer an empty set (issue #7477)")
          .hasSize(3).doesNotHaveDuplicates();

      try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM CITE")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
      }

      // ...while the type itself still holds no record, which is what LIGHTWEIGHT means
      assertThat(database.countType("CITE", true)).isZero();
    });
  }

  /** The flag tooling needs to read the record count of a lightweight type as "none by design" rather than "empty". */
  @Test
  void theSchemaListingReportsTheTypeAsLightweight() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = 'CITE'")) {
      final Result type = rs.next();
      assertThat(type.<Boolean>getProperty("lightweight")).isTrue();
      assertThat(type.<Long>getProperty("records")).isZero();
    }
  }
}

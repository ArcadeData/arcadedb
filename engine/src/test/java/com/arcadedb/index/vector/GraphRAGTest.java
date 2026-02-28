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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Basic GraphRAG Example.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphRAGTest extends TestHelper {

  private static final int DIMENSIONS = 768;

  @Test
  void hybridVectorAndGraphRetrieval() {
    // Schema: Define Publication Types and Knowledge Graph
    // Create document types with vector embeddings and entity types for your knowledge graph:
    database.transaction(() -> {
      // CREATE THE SCHEMA
      database.command("sql", "CREATE VERTEX TYPE Publication");
      database.command("sql", "CREATE PROPERTY Publication.content STRING");
      database.command("sql", "CREATE PROPERTY Publication.title STRING");
      database.command("sql", "CREATE PROPERTY Publication.embedding ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Publication(embedding) LSM_VECTOR METADATA { dimensions: " + DIMENSIONS + ", similarity: 'COSINE' }");

      database.command("sql", "CREATE VERTEX TYPE Entity");
      database.command("sql", "CREATE VERTEX TYPE User EXTENDS Entity");
      database.command("sql", "CREATE VERTEX TYPE Concept EXTENDS Entity");
      database.command("sql", "CREATE VERTEX TYPE Organization EXTENDS Entity");

      database.command("sql", "CREATE EDGE TYPE MENTIONS");
      database.command("sql", "CREATE EDGE TYPE RELATES_TO");
      database.command("sql", "CREATE EDGE TYPE AUTHORED_BY");
      database.command("sql", "CREATE EDGE TYPE TOPIC_OF");
    });

    final float[] sampleVector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      sampleVector[i] = (float) Math.random();

    // Ingest: Store Publications with Vector Embeddings
    // Insert documents with their semantic embeddings and link entities:
    database.transaction(() -> {
      // Insert a document with vector embedding (from LLM)
      database.command("sql", """
          INSERT INTO Publication SET
          title = 'ArcadeDB: Multi-Model Database Architecture',
              content = 'ArcadeDB is a multi-model database supporting...',
              embedding = ?,
          timestamp = sysdate();
          """, sampleVector);

      // Create entities mentioned in the document
      database.command("sql", "CREATE VERTEX User SET name = 'NASA', role = 'Creator'");

      database.command("sql",
          "CREATE VERTEX Concept SET name = 'Multi-Model Database', description = 'Database supporting multiple data models'");
    });

    database.transaction(() -> {
      // Link document to entities
      database.command("sql", """
          CREATE EDGE MENTIONS FROM
          (SELECT FROM Publication WHERE title = 'ArcadeDB: Multi-Model Database Architecture')
          TO (SELECT FROM User WHERE name = 'NASA');
          """);

      database.command("sql", """
          CREATE EDGE TOPIC_OF FROM
          (SELECT FROM Publication WHERE title = 'ArcadeDB: Multi-Model Database Architecture')
          TO
              (SELECT FROM Concept WHERE name = 'Multi-Model Database');
          """);
    });

    // Retrieve: Hybrid Vector + Graph Query
    // Find semantically similar publications and enrich results with entity context:
    database.transaction(() -> {
      // Hybrid retrieval: vector similarity + graph context
      ResultSet result = database.query("sql", """
              SELECT title, content, $distance AS relevance
              FROM Publication
              LET $distance = (1 - `vector.cosineSimilarity`(embedding, ?))
              WHERE $distance < 0.4
              ORDER BY $distance
              LIMIT 10;
          """, sampleVector
      );

      Assertions.assertThat(result.hasNext()).isTrue();
      while (result.hasNext()) {
        var record = result.next();
        Assertions.assertThat((String) record.getProperty("title")).isEqualTo("ArcadeDB: Multi-Model Database Architecture");
        Assertions.assertThat((Float) record.getProperty("relevance")).isEqualTo(0.0F);
      }
    });
  }
}

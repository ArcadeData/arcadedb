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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for head(collect()) issue.
 * Tests that wrapped aggregation functions like head(collect(...)) work correctly in WITH clauses.
 *
 * Issue: head(collect(...)) was returning null because WithClause.hasAggregations() didn't
 * detect aggregations wrapped in non-aggregation functions, causing the query planner to use
 * WithStep instead of GroupByAggregationStep.
 */
class HeadCollectTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-head-collect").create();
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createEdgeType("BELONGS_TO");

    // Create test data similar to the user's example:
    // CHUNK nodes connected to DOCUMENT nodes
    database.command("opencypher",
        "CREATE (doc1:DOCUMENT {name: 'ORANO-MAG-2021_205x275_FR_MEL.pdf'}), " +
            "(doc2:DOCUMENT {name: 'Other-Document.pdf'}), " +
            "(chunk1:CHUNK {id: 'chunk1'}), " +
            "(chunk2:CHUNK {id: 'chunk2'}), " +
            "(chunk3:CHUNK {id: 'chunk3'}), " +
            "(chunk1)-[:BELONGS_TO]->(doc1), " +
            "(chunk2)-[:BELONGS_TO]->(doc1), " +
            "(chunk3)-[:BELONGS_TO]->(doc2)");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void testHeadCollectInWith() {
    // Test the exact pattern from the issue: head(collect(...)) in WITH clause
    final ResultSet result = database.command("opencypher",
        "MATCH (c:CHUNK {id: 'chunk1'}) " +
            "MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT) " +
            "WITH head(collect(ID(doc))) as document_id, head(collect(doc.name)) as document_name " +
            "RETURN document_id, document_name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    // Should NOT be null - this is the bug we're fixing
    final Object documentId = row.getProperty("document_id");
    final Object documentName = row.getProperty("document_name");

    assertThat(documentId).isNotNull();
    assertThat(documentName).isNotNull();
    assertThat(documentName).isEqualTo("ORANO-MAG-2021_205x275_FR_MEL.pdf");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testHeadCollectMultipleFields() {
    // Test with multiple head(collect(...)) expressions
    final ResultSet result = database.command("opencypher",
        "MATCH (c:CHUNK {id: 'chunk1'}) " +
            "MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT) " +
            "WITH head(collect(ID(doc))) as document_id, " +
            "     head(collect(ID(c))) as chunk_id, " +
            "     head(collect(doc.name)) as document_name " +
            "RETURN document_id, chunk_id, document_name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    assertThat(row.getProperty("document_id")).isNotNull();
    assertThat(row.getProperty("chunk_id")).isNotNull();
    assertThat(row.getProperty("document_name")).isNotNull();
    assertThat(row.getProperty("document_name")).isEqualTo("ORANO-MAG-2021_205x275_FR_MEL.pdf");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testCollectWithoutHead() {
    // Control test: verify that collect() without head() works (returns a list)
    final ResultSet result = database.command("opencypher",
        "MATCH (c:CHUNK {id: 'chunk1'}) " +
            "MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT) " +
            "WITH collect(doc.name) as document_names " +
            "RETURN document_names");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    final Object documentNames = row.getProperty("document_names");
    assertThat(documentNames).isInstanceOf(List.class);
    final List<?> namesList = (List<?>) documentNames;
    assertThat(namesList).hasSize(1);
    assertThat(namesList.get(0)).isEqualTo("ORANO-MAG-2021_205x275_FR_MEL.pdf");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testLastCollect() {
    // Test with last() instead of head() - similar pattern
    final ResultSet result = database.command("opencypher",
        "MATCH (c:CHUNK {id: 'chunk1'}) " +
            "MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT) " +
            "WITH last(collect(doc.name)) as document_name " +
            "RETURN document_name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();

    assertThat(row.getProperty("document_name")).isNotNull();
    assertThat(row.getProperty("document_name")).isEqualTo("ORANO-MAG-2021_205x275_FR_MEL.pdf");

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testSizeCollect() {
    // Test with size() wrapping collect() - another wrapped aggregation pattern
    final ResultSet result = database.command("opencypher",
        "MATCH (c:CHUNK) " +
            "MATCH (c)-[:BELONGS_TO]->(doc:DOCUMENT) " +
            "WITH c.id as chunk_id, size(collect(doc.name)) as doc_count " +
            "RETURN chunk_id, doc_count " +
            "ORDER BY chunk_id");

    assertThat(result.hasNext()).isTrue();

    // chunk1 -> 1 document
    final Result row1 = result.next();
    assertThat(row1.getProperty("chunk_id")).isEqualTo("chunk1");
    assertThat(((Number) row1.getProperty("doc_count")).longValue()).isEqualTo(1L);

    // chunk2 -> 1 document
    assertThat(result.hasNext()).isTrue();
    final Result row2 = result.next();
    assertThat(row2.getProperty("chunk_id")).isEqualTo("chunk2");
    assertThat(((Number) row2.getProperty("doc_count")).longValue()).isEqualTo(1L);

    // chunk3 -> 1 document
    assertThat(result.hasNext()).isTrue();
    final Result row3 = result.next();
    assertThat(row3.getProperty("chunk_id")).isEqualTo("chunk3");
    assertThat(((Number) row3.getProperty("doc_count")).longValue()).isEqualTo(1L);

    assertThat(result.hasNext()).isFalse();
  }
}

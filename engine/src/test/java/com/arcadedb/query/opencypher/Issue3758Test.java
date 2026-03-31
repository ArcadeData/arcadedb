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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3758
 * Cypher self-join on the same edge type can return incorrect empty results.
 * <p>
 * Root causes fixed:
 * 1. buildExecutionStepsWithOptimizer processed UNWIND before WITH regardless of query order.
 * 2. tryOptimizeMatchCountReturn incorrectly fired for queries with intermediate WITH/UNWIND.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3758Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue3758").create();

    database.getSchema().createVertexType("Question");
    database.getSchema().createVertexType("Tag");
    database.getSchema().createEdgeType("TAGGED_WITH");

    database.transaction(() -> {
      // Create 3 questions and 4 tags
      database.command("opencypher", "CREATE (q1:Question {Id: 1, Title: 'Q1'})");
      database.command("opencypher", "CREATE (q2:Question {Id: 2, Title: 'Q2'})");
      database.command("opencypher", "CREATE (q3:Question {Id: 3, Title: 'Q3'})");
      database.command("opencypher", "CREATE (t1:Tag {Id: 1, TagName: 'java'})");
      database.command("opencypher", "CREATE (t2:Tag {Id: 2, TagName: 'python'})");
      database.command("opencypher", "CREATE (t3:Tag {Id: 3, TagName: 'sql'})");
      database.command("opencypher", "CREATE (t4:Tag {Id: 4, TagName: 'graph'})");

      // Q1 tagged with java, python, sql (3 tags -> 3 co-occurrence pairs)
      database.command("opencypher",
          "MATCH (q:Question {Id: 1}), (t:Tag {Id: 1}) CREATE (q)-[:TAGGED_WITH]->(t)");
      database.command("opencypher",
          "MATCH (q:Question {Id: 1}), (t:Tag {Id: 2}) CREATE (q)-[:TAGGED_WITH]->(t)");
      database.command("opencypher",
          "MATCH (q:Question {Id: 1}), (t:Tag {Id: 3}) CREATE (q)-[:TAGGED_WITH]->(t)");

      // Q2 tagged with java, python (2 tags -> 1 co-occurrence pair)
      database.command("opencypher",
          "MATCH (q:Question {Id: 2}), (t:Tag {Id: 1}) CREATE (q)-[:TAGGED_WITH]->(t)");
      database.command("opencypher",
          "MATCH (q:Question {Id: 2}), (t:Tag {Id: 2}) CREATE (q)-[:TAGGED_WITH]->(t)");

      // Q3 tagged with sql, graph (2 tags -> 1 co-occurrence pair)
      database.command("opencypher",
          "MATCH (q:Question {Id: 3}), (t:Tag {Id: 3}) CREATE (q)-[:TAGGED_WITH]->(t)");
      database.command("opencypher",
          "MATCH (q:Question {Id: 3}), (t:Tag {Id: 4}) CREATE (q)-[:TAGGED_WITH]->(t)");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Tests the exact query from the issue: two MATCH clauses sharing variable q,
   * both traversing TAGGED_WITH, with WHERE t1.Id < t2.Id for unique pairs.
   *
   * Expected co-occurrences after grouping:
   * - java,python: 2 (from Q1 and Q2)
   * - java,sql: 1 (from Q1)
   * - python,sql: 1 (from Q1)
   * - sql,graph: 1 (from Q3)
   */
  @Test
  void tagCooccurrenceWithTwoMatchClauses() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (q:Question)-[:TAGGED_WITH]->(t1:Tag)
          MATCH (q)-[:TAGGED_WITH]->(t2:Tag)
          WHERE t1.Id < t2.Id
          RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs
          ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC
          """)) {

        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(4);

        assertThat(results.get(0).<String>getProperty("tag1")).isEqualTo("java");
        assertThat(results.get(0).<String>getProperty("tag2")).isEqualTo("python");
        assertThat(results.get(0).<Long>getProperty("cooccurs")).isEqualTo(2L);

        assertThat(results.get(1).<String>getProperty("tag1")).isEqualTo("java");
        assertThat(results.get(1).<String>getProperty("tag2")).isEqualTo("sql");
        assertThat(results.get(1).<Long>getProperty("cooccurs")).isEqualTo(1L);

        assertThat(results.get(2).<String>getProperty("tag1")).isEqualTo("python");
        assertThat(results.get(2).<String>getProperty("tag2")).isEqualTo("sql");
        assertThat(results.get(2).<Long>getProperty("cooccurs")).isEqualTo(1L);

        assertThat(results.get(3).<String>getProperty("tag1")).isEqualTo("sql");
        assertThat(results.get(3).<String>getProperty("tag2")).isEqualTo("graph");
        assertThat(results.get(3).<Long>getProperty("cooccurs")).isEqualTo(1L);
      }
    });
  }

  /**
   * Tests the alternative formulation from the issue using collect/UNWIND.
   * This variant exposed two bugs:
   * 1. UNWIND was processed before WITH in the optimizer path (clause ordering bug)
   * 2. tryOptimizeMatchCountReturn fired incorrectly, bypassing the UNWIND pipeline
   */
  @Test
  void tagCooccurrenceWithCollectUnwind() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
          WITH q, collect(t) AS tags
          UNWIND tags AS t1
          UNWIND tags AS t2
          WITH t1, t2
          WHERE t1.Id < t2.Id
          RETURN t1.TagName AS tag1, t2.TagName AS tag2, count(*) AS cooccurs
          ORDER BY cooccurs DESC, tag1 ASC, tag2 ASC
          """)) {

        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(4);

        assertThat(results.get(0).<String>getProperty("tag1")).isEqualTo("java");
        assertThat(results.get(0).<String>getProperty("tag2")).isEqualTo("python");
        assertThat(results.get(0).<Long>getProperty("cooccurs")).isEqualTo(2L);

        assertThat(results.get(1).<String>getProperty("tag1")).isEqualTo("java");
        assertThat(results.get(1).<String>getProperty("tag2")).isEqualTo("sql");
        assertThat(results.get(1).<Long>getProperty("cooccurs")).isEqualTo(1L);

        assertThat(results.get(2).<String>getProperty("tag1")).isEqualTo("python");
        assertThat(results.get(2).<String>getProperty("tag2")).isEqualTo("sql");
        assertThat(results.get(2).<Long>getProperty("cooccurs")).isEqualTo(1L);

        assertThat(results.get(3).<String>getProperty("tag1")).isEqualTo("sql");
        assertThat(results.get(3).<String>getProperty("tag2")).isEqualTo("graph");
        assertThat(results.get(3).<Long>getProperty("cooccurs")).isEqualTo(1L);
      }
    });
  }

  /**
   * Tests that collect/UNWIND preserves vertex properties through the pipeline.
   */
  @Test
  void collectUnwindPreservesProperties() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
          WITH q, collect(t) AS tags
          UNWIND tags AS t1
          RETURN t1.TagName AS name, t1.Id AS tid
          ORDER BY tid
          """)) {

        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(7);
        for (final Result r : results)
          assertThat(r.<String>getProperty("name")).as("TagName should not be null after UNWIND").isNotNull();
      }
    });
  }

  /**
   * Tests that the single MATCH version works correctly as a baseline.
   */
  @Test
  void singleMatchTagCountBaseline() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH (q:Question)-[:TAGGED_WITH]->(t:Tag)
          RETURN t.TagName AS tag, count(q) AS questions
          ORDER BY questions DESC, tag ASC
          """)) {

        final List<Result> results = new ArrayList<>();
        while (rs.hasNext())
          results.add(rs.next());

        assertThat(results).hasSize(4);
        assertThat(results.get(0).<String>getProperty("tag")).isEqualTo("java");
        assertThat(results.get(0).<Long>getProperty("questions")).isEqualTo(2L);
        assertThat(results.get(1).<String>getProperty("tag")).isEqualTo("python");
        assertThat(results.get(1).<Long>getProperty("questions")).isEqualTo(2L);
      }
    });
  }
}

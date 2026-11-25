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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11771: in {@code LOAD CSV ... AS row MATCH (n {id: row.id})} the cost-based optimizer ran the MATCH
 * before LOAD CSV bound {@code row}, so a read returned no rows and a CREATE / SET / MERGE wrote nothing, with no error.
 * Every clause that precedes a MATCH now keeps the statement on the ordered plan, not only WITH and UNWIND (#11586): a write
 * before a later MATCH included ({@code MATCH ... CREATE ... MATCH}, which missed the node it had just created). The first
 * read is checked against the same statement with a neutral {@code WITH row}, which always kept the clauses in order.
 */
class CypherLoadCsvMatchIssue11771Test extends TestHelper {

  private String ids;
  private String rels;

  @Override
  protected void beginTest() {
    try {
      final Path dir = Files.createDirectories(Path.of("./target/test-data/cypher-loadcsv-11771"));
      final Path idsFile = dir.resolve("ids.csv");
      final Path relsFile = dir.resolve("rels.csv");
      Files.writeString(idsFile, "id\n2\n3\n");
      Files.writeString(relsFile, "src,dst\n1,2\n2,3\n");
      ids = idsFile.toAbsolutePath().toUri().toString();
      rels = relsFile.toAbsolutePath().toUri().toString();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1}), (:P {id: 2}), (:P {id: 3})"));
  }

  @Test
  void aMatchAfterLoadCsvReadsTheRowInWhere() {
    final String q = "LOAD CSV WITH HEADERS FROM '" + ids + "' AS row MATCH (b:P) WHERE b.id = toInteger(row.id) RETURN b.id AS v ORDER BY v";
    assertThat(values(q)).isEqualTo(List.of(2L, 3L));
    assertThat(values(q.replace(" AS row ", " AS row WITH row "))).isEqualTo(List.of(2L, 3L));
  }

  @Test
  void aMatchAfterLoadCsvReadsTheRowInAnInlineMap() {
    final String q = "LOAD CSV WITH HEADERS FROM '" + ids + "' AS row MATCH (b:P {id: toInteger(row.id)}) RETURN b.id AS v ORDER BY v";
    assertThat(values(q)).isEqualTo(List.of(2L, 3L));
  }

  @Test
  void aMatchBeforeAndAfterLoadCsv() {
    final String q = "MATCH (a:P {id: 1}) LOAD CSV WITH HEADERS FROM '" + ids
        + "' AS row MATCH (b:P) WHERE b.id = toInteger(row.id) RETURN a.id * 10 + b.id AS v ORDER BY v";
    assertThat(values(q)).isEqualTo(List.of(12L, 13L));
  }

  @Test
  void createAfterLoadCsvMatchWritesEveryRow() {
    database.transaction(() -> database.command("opencypher", "LOAD CSV WITH HEADERS FROM '" + rels
        + "' AS row MATCH (a:P {id: toInteger(row.src)}), (b:P {id: toInteger(row.dst)}) CREATE (a)-[:R]->(b)"));
    assertThat(values("MATCH (a:P)-[:R]->(b:P) RETURN a.id * 10 + b.id AS v ORDER BY v")).isEqualTo(List.of(12L, 23L));
  }

  @Test
  void setAfterLoadCsvMatchWritesEveryRow() {
    database.transaction(() -> database.command("opencypher",
        "LOAD CSV WITH HEADERS FROM '" + rels + "' AS row MATCH (a:P) WHERE a.id = toInteger(row.src) SET a.seen = true"));
    assertThat(values("MATCH (a:P) WHERE a.seen RETURN a.id AS v ORDER BY v")).isEqualTo(List.of(1L, 2L));
  }

  @Test
  void mergeAfterLoadCsvMatchWritesEveryRow() {
    database.transaction(() -> database.command("opencypher", "LOAD CSV WITH HEADERS FROM '" + rels
        + "' AS row MATCH (a:P {id: toInteger(row.src)}) MERGE (a)-[:M]->(:Q {id: toInteger(row.dst)})"));
    assertThat(values("MATCH (a:P)-[:M]->(q:Q) RETURN a.id * 10 + q.id AS v ORDER BY v")).isEqualTo(List.of(12L, 23L));
  }

  @Test
  void aWriteAfterMatchThenLoadCsvReadsTheRow() {
    // MATCH, then LOAD CSV, then a write: the optimizer chained the CREATE before LOAD CSV bound row
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:P {id: 1}) LOAD CSV WITH HEADERS FROM '" + ids + "' AS row CREATE (a)-[:R]->(:Q {id: toInteger(row.id)})"));
    assertThat(values("MATCH (a:P)-[:R]->(q:Q) RETURN a.id * 10 + q.id AS v ORDER BY v")).isEqualTo(List.of(12L, 13L));
  }

  @Test
  void aMatchAfterAWriteSeesTheWrite() {
    // Every non-MATCH clause before a MATCH keeps the statement in order, a write included
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:P {id: 1}) CREATE (:Q {id: 7}) WITH a MATCH (q:Q) CREATE (a)-[:R]->(q)"));
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:P {id: 2}) CREATE (:Q {id: 8}) MATCH (q:Q {id: 8}) CREATE (a)-[:R]->(q)"));
    assertThat(values("MATCH (a:P)-[:R]->(q:Q) RETURN a.id * 10 + q.id AS v ORDER BY v")).isEqualTo(List.of(17L, 28L));
  }

  @Test
  void aCountAfterLoadCsvMatchCountsEveryRow() {
    database.transaction(() -> database.command("opencypher", "MATCH (a:P {id: 1}), (b:P {id: 2}) CREATE (a)-[:R]->(b)"));
    final String load = "LOAD CSV WITH HEADERS FROM '" + ids + "' AS row ";
    // two CSV rows, so every count is twice the graph's
    assertThat(values(load + "MATCH (n:P) RETURN count(n) AS v")).isEqualTo(List.of(6L));
    assertThat(values(load + "MATCH (n:P) RETURN count(*) AS v")).isEqualTo(List.of(6L));
    assertThat(values(load + "MATCH (a:P)-[:R]->(b) RETURN count(b) AS v")).isEqualTo(List.of(2L));
    assertThat(values(load + "MATCH (a:P)-[:R]->(b) RETURN a.id AS k, count(b) AS v")).isEqualTo(List.of(2L));
    assertThat(values(load + "MATCH (n:P) WHERE n.id = toInteger(row.id) RETURN count(n) AS v")).isEqualTo(List.of(2L));
  }

  @Test
  void aMatchAfterLoadCsvIsPlannedInOrder() {
    assertThat(explain("LOAD CSV WITH HEADERS FROM '" + ids + "' AS row MATCH (b:P) WHERE b.id = toInteger(row.id) RETURN b.id"))
        .doesNotContain("Cost-Based");
  }

  @Test
  void aWriteAfterAMatchStaysOnTheOptimizer() {
    // Control: the gate must not push plain MATCH ... CREATE ... SET shapes off the optimizer.
    assertThat(explain("MATCH (a:P) WHERE a.id = 1 CREATE (a)-[:R]->(:Q {id: 9}) SET a.seen = true")).contains("Cost-Based");
    assertThat(explain("MATCH (a:P) WHERE a.id = 1 RETURN a.id")).contains("Cost-Based");
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private List<Long> values(final String query) {
    final List<Long> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("v").longValue());
    }
    return out;
  }
}

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
import com.arcadedb.database.Database;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12640: the upserts of a bulk load, {@code UNWIND $rows AS row MERGE (n:L {uuid: row.uuid}) SET n = row} and its
 * relationship form {@code MATCH (a ..), (b ..) MERGE (a)-[e:R {uuid: row.uuid}]->(b) SET e = row}, planned an eager barrier
 * between the MERGE and the SET: the SET replaces the whole map, so it counted as writing the {@code uuid} the MERGE looks up. It
 * writes it with the value the MERGE matched it by, so no MERGE of another row can find anything else, and the SET is now written
 * with the entity the MERGE creates: each new node and relationship is stored once instead of created with its key and rewritten
 * with the row.
 */
class CypherMergeSetPairedKeyIssue12640Test extends TestHelper {
  private static final String NODE_UPSERT = "UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) SET n = row RETURN n.uuid AS uuid";
  private static final String EDGE_UPSERT =
      "UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
          + "MERGE (a)-[e:RELATES_TO {uuid: row.uuid}]->(b) SET e = row RETURN row.uuid AS uuid";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
    database.command("sql", "CREATE INDEX ON Entity (uuid) NOTUNIQUE");
    database.command("sql", "CREATE EDGE TYPE RELATES_TO");
    database.command("sql", "CREATE PROPERTY RELATES_TO.uuid STRING");
    database.command("sql", "CREATE INDEX ON RELATES_TO (uuid) NOTUNIQUE");
  }

  @Test
  void pairedMapWritesPlanNoBarrier() {
    assertThat(explain(NODE_UPSERT)).doesNotContain("EAGER").doesNotContain("SET");
    assertThat(explain(EDGE_UPSERT)).doesNotContain("EAGER").doesNotContain("SET");
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) SET n += row")).doesNotContain("EAGER");
  }

  @Test
  void writesThatCanChangeAMergeKeyKeepTheBarrier() {
    // the key is taken from another key of the row: the map write rewrites it
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.id}) SET n = row")).contains("EAGER");
    // the map is another variable than the one the key came from
    assertThat(explain("UNWIND $rows AS row UNWIND $other AS o MERGE (n:Entity {uuid: row.uuid}) SET n = o")).contains("EAGER");
    // a second key the MERGE reads is not taken from the row
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid, kind: 'x'}) SET n = row")).contains("EAGER");
    // another MERGE reads a key this node's map write could remove
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) MERGE (m:Entity {name: row.name}) SET n = row"))
        .contains("EAGER");
    // a WITH can rebind the map variable
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) WITH n, row AS r, {uuid: 'x'} AS row SET n = row"))
        .contains("EAGER");
    // an UNWIND binds the map variable anew
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) UNWIND [{uuid: 'x'}] AS row SET n = row"))
        .contains("EAGER");
    // the FOREACH variable shadows the row the key came from
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) FOREACH (row IN [{uuid: 'x'}] | SET n = row)"))
        .contains("EAGER");
  }

  @Test
  void aKeyTheMergeActionsWriteIsNotPaired() {
    // the MERGE's own ON CREATE SET / ON MATCH SET runs between the lookup and the SET, so the key no longer holds the value the
    // MERGE matched it by when the SET runs
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON CREATE SET n.uuid = 'z' SET n = row"))
        .contains("EAGER");
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON MATCH SET n.uuid = 'z' SET n = row"))
        .contains("EAGER");
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON MATCH SET n = {} SET n = row")).contains("EAGER");
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON CREATE SET n += row.extra SET n += row"))
        .contains("EAGER");
    assertThat(explain("UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
        + "MERGE (a)-[e:RELATES_TO {uuid: row.uuid}]->(b) ON MATCH SET e.uuid = 'z' SET e = row")).contains("EAGER");
    // an action that writes another key keeps the pair
    assertThat(explain("UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON CREATE SET n.created = true SET n += row"))
        .doesNotContain("EAGER");
  }

  @Test
  void aMergeActionWritingThePairedKeyMergesAsTheClauseAtATimePlanDoes() {
    // Clause at a time every MERGE runs before any SET: each row finds no node with the key, since ON CREATE SET moved it, and
    // creates one. Streamed, the SET of the first row would write the key back and every later row would match that node.
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 150; i++)
      rows.add(Map.of("uuid", "same", "name", "n" + i));
    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON CREATE SET n.uuid = 'moved' SET n = row", Map.of("rows", rows))
        .close());
    final long direct = database.countType("Entity", false);

    database.transaction(() -> database.command("sql", "DELETE FROM Entity"));
    // the WITH keeps the SET a step of its own, behind the barrier
    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) ON CREATE SET n.uuid = 'moved' WITH n, row SET n = row",
        Map.of("rows", rows)).close());
    assertThat(direct).isEqualTo(database.countType("Entity", false)).isEqualTo(150);
  }

  @Test
  void aRelationshipTypeWithDefaultsKeepsWhatTheSetStepStores() {
    database.command("sql", "CREATE EDGE TYPE WEIGHTED");
    database.command("sql", "CREATE PROPERTY WEIGHTED.uuid STRING");
    database.command("sql", "CREATE PROPERTY WEIGHTED.weight INTEGER (default 7)");
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a'}), (:Entity {uuid: 'b'})").close());

    final List<Map<String, Object>> rows = List.of(Map.of("uuid", "w1", "source", "a", "target", "b", "fact", "f"));
    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
            + "MERGE (a)-[e:WEIGHTED {uuid: row.uuid}]->(b) SET e = row", Map.of("rows", rows)).close());
    final List<Map<String, Object>> stepped = List.of(Map.of("uuid", "w2", "source", "a", "target", "b", "fact", "f"));
    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
            + "MERGE (a)-[e:WEIGHTED {uuid: row.uuid}]->(b) WITH e, row SET e = row", Map.of("rows", stepped)).close());

    final Edge direct = database.query("sql", "SELECT FROM WEIGHTED WHERE uuid = 'w1'").next().getEdge().get();
    final Edge viaStep = database.query("sql", "SELECT FROM WEIGHTED WHERE uuid = 'w2'").next().getEdge().get();
    assertThat(direct.getString("fact")).isEqualTo("f");
    assertThat(direct.has("weight")).isEqualTo(viaStep.has("weight"));
    assertThat(direct.getPropertyNames()).containsExactlyInAnyOrderElementsOf(viaStep.getPropertyNames());
  }

  @Test
  void aNodeTypeWithDefaultsKeepsWhatTheSetStepStores() {
    database.command("sql", "CREATE VERTEX TYPE Weighted");
    database.command("sql", "CREATE PROPERTY Weighted.uuid STRING");
    database.command("sql", "CREATE PROPERTY Weighted.weight INTEGER (default 7)");

    // each folded form next to the same clauses with a WITH in between, which keeps the SET a step of its own
    final String[][] forms = {
        { "UNWIND $rows AS row MERGE (n:Weighted {uuid: row.uuid}) SET n = row",
            "UNWIND $rows AS row MERGE (n:Weighted {uuid: row.uuid}) WITH n, row SET n = row" },
        { "UNWIND $rows AS row CREATE (n:Weighted) SET n = row", "UNWIND $rows AS row CREATE (n:Weighted) WITH n, row SET n = row" },
        { "UNWIND $rows AS row MERGE (n:Weighted {uuid: row.uuid}) ON CREATE SET n.weight = null",
            "UNWIND $rows AS row MERGE (n:Weighted {uuid: row.uuid}) WITH n, row SET n.weight = null" } };
    int next = 0;
    for (final String[] form : forms) {
      final String folded = "f" + next;
      final String stepped = "s" + next++;
      final int[] propertiesSet = new int[2];
      for (int i = 0; i < 2; i++) {
        final int index = i;
        final String uuid = i == 0 ? folded : stepped;
        database.transaction(() -> {
          try (final ResultSet rs = database.command("opencypher", form[index], Map.of("rows", List.of(Map.of("uuid", uuid, "name", "x"))))) {
            count(rs);
            propertiesSet[index] = rs.getStatistics().orElseThrow().getPropertiesSet();
          }
        });
      }
      final Vertex direct = database.query("sql", "SELECT FROM Weighted WHERE uuid = ?", folded).next().getVertex().get();
      final Vertex viaStep = database.query("sql", "SELECT FROM Weighted WHERE uuid = ?", stepped).next().getVertex().get();
      assertThat(direct.getPropertyNames()).as(form[0]).containsExactlyInAnyOrderElementsOf(viaStep.getPropertyNames());
      assertThat(direct.has("weight")).as(form[0]).isFalse();
      assertThat(propertiesSet[0]).as(form[0]).isEqualTo(propertiesSet[1]);
    }
  }

  @Test
  void aParameterHoldingARecordKeepsTheBarrier() {
    final Vertex[] entity = new Vertex[1];
    database.transaction(() -> entity[0] = database.newVertex("Entity").set("uuid", "u", "source", "a", "target", "b").save());
    final String query = "EXPLAIN " + EDGE_UPSERT;
    try (final ResultSet rs = database.command("opencypher", query, Map.of("rows", List.of(entity[0])))) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("EAGER");
    }
    try (final ResultSet rs = database.command("opencypher", query, Map.of("rows", List.of(Map.of("uuid", "u", "source", "a"),
        entity[0])))) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("EAGER");
    }
  }

  @Test
  void nodeUpsertStoresEachNewNodeOnceAndUpdatesMatchedOnes() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 300; i++)
      rows.add(Map.of("uuid", "u" + i, "name", "n" + i, "embedding", List.of(0.5 * i, 1.0, 2.0)));

    database.transaction(() -> {
      final long before = updates(database);
      assertThat(count(database.command("opencypher", NODE_UPSERT, Map.of("rows", rows)))).isEqualTo(300);
      assertThat(updates(database) - before).isZero();
    });
    final Vertex v = database.query("sql", "SELECT FROM Entity WHERE uuid = 'u7'").next().getVertex().get();
    assertThat(v.getString("name")).isEqualTo("n7");
    assertThat(v.get("embedding")).isEqualTo(List.of(3.5, 1.0, 2.0));

    // matched nodes are rewritten by the map: the property the new row leaves out is removed
    final List<Map<String, Object>> again = new ArrayList<>();
    for (int i = 0; i < 300; i += 2)
      again.add(Map.of("uuid", "u" + i, "name", "again" + i));
    database.transaction(() -> database.command("opencypher", NODE_UPSERT, Map.of("rows", again)).close());
    assertThat(database.countType("Entity", false)).isEqualTo(300);
    final Vertex matched = database.query("sql", "SELECT FROM Entity WHERE uuid = 'u8'").next().getVertex().get();
    assertThat(matched.getString("name")).isEqualTo("again8");
    assertThat(matched.has("embedding")).isFalse();
    assertThat(database.query("sql", "SELECT FROM Entity WHERE uuid = 'u9'").next().getVertex().get().getString("name"))
        .isEqualTo("n9");
  }

  @Test
  void rowsOfOneBatchWithTheSameKeyMergeIntoOneNode() {
    // Clause at a time: every MERGE runs before any SET, so the second row matches the node the first created and the last SET
    // wins. Streamed and folded, the first row creates the node with its map and the second matches it and rewrites it.
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 250; i++)
      rows.add(Map.of("uuid", "same", "name", "n" + i));
    database.transaction(() -> assertThat(count(database.command("opencypher", NODE_UPSERT, Map.of("rows", rows)))).isEqualTo(250));
    assertThat(database.countType("Entity", false)).isEqualTo(1);
    assertThat(database.query("sql", "SELECT FROM Entity").next().getVertex().get().getString("name")).isEqualTo("n249");
  }

  @Test
  void rowsOfOneBatchWithTheSameKeyEndWithTheLastRow() {
    // SET n += row: the properties of every row accumulate, the last row's value of each key wins
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 150; i++)
      rows.add(i == 3 ? Map.of("uuid", "plus", "name", "n" + i, "only", "row3") : Map.of("uuid", "plus", "name", "n" + i));
    database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) SET n += row",
        Map.of("rows", rows)).close());
    assertThat(database.countType("Entity", false)).isEqualTo(1);
    final Vertex node = database.query("sql", "SELECT FROM Entity WHERE uuid = 'plus'").next().getVertex().get();
    assertThat(node.getString("name")).isEqualTo("n149");
    assertThat(node.getString("only")).isEqualTo("row3");

    // the relationship form: one relationship, holding the last row's map
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a'}), (:Entity {uuid: 'b'})").close());
    final List<Map<String, Object>> edges = new ArrayList<>();
    for (int i = 0; i < 150; i++)
      edges.add(i == 3 ? Map.of("uuid", "e", "source", "a", "target", "b", "fact", "f" + i, "only", "row3")
          : Map.of("uuid", "e", "source", "a", "target", "b", "fact", "f" + i));
    database.transaction(() -> database.command("opencypher", EDGE_UPSERT, Map.of("rows", edges)).close());
    assertThat(database.countType("RELATES_TO", false)).isEqualTo(1);
    final Edge edge = database.query("sql", "SELECT FROM RELATES_TO WHERE uuid = 'e'").next().getEdge().get();
    assertThat(edge.getString("fact")).isEqualTo("f149");
    // SET e = row replaces the map, so the key only row 3 carried is gone
    assertThat(edge.has("only")).isFalse();
  }

  @Test
  void relationshipUpsertStoresEachNewRelationshipOnceAndUpdatesMatchedOnes() {
    // the same load through the SET step (the WITH keeps it one) writes every relationship a second time
    final long folded = loadRelationships(EDGE_UPSERT, "n");
    final long stepped = loadRelationships("UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
        + "MERGE (a)-[e:RELATES_TO {uuid: row.uuid}]->(b) WITH e, row SET e = row RETURN row.uuid AS uuid", "m");
    assertThat(stepped - folded).isEqualTo(400);

    assertThat(database.countType("RELATES_TO", false)).isEqualTo(800);
    final Edge e = database.query("sql", "SELECT FROM RELATES_TO WHERE uuid = 'n-e21'").next().getEdge().get();
    assertThat(e.getString("fact")).isEqualTo("f21");
    assertThat(e.getString("source")).isEqualTo("n21");
    assertThat(e.get("embedding")).isEqualTo(List.of(21.0, 2.0));
    assertThat(e.getOutVertex().getString("uuid")).isEqualTo("n21");
    assertThat(e.getInVertex().getString("uuid")).isEqualTo("n" + (21 * 7 % 50));

    final List<Map<String, Object>> again = new ArrayList<>();
    for (int i = 0; i < 400; i += 4)
      again.add(Map.of("uuid", "n-e" + i, "source", "n" + (i % 50), "target", "n" + ((i * 7) % 50), "fact", "again" + i));
    database.transaction(() -> database.command("opencypher", EDGE_UPSERT, Map.of("rows", again)).close());
    assertThat(database.countType("RELATES_TO", false)).isEqualTo(800);
    final Edge matched = database.query("sql", "SELECT FROM RELATES_TO WHERE uuid = 'n-e8'").next().getEdge().get();
    assertThat(matched.getString("fact")).isEqualTo("again8");
    assertThat(matched.has("embedding")).isFalse();
  }

  /** 50 entities named {@code prefix0..49} and 400 relationships between them, the record updates the relationships cost. */
  private long loadRelationships(final String query, final String prefix) {
    final List<Map<String, Object>> nodes = new ArrayList<>();
    for (int i = 0; i < 50; i++)
      nodes.add(Map.of("uuid", prefix + i));
    database.transaction(() -> database.command("opencypher", NODE_UPSERT, Map.of("rows", nodes)).close());

    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 400; i++)
      rows.add(Map.of("uuid", prefix + "-e" + i, "source", prefix + (i % 50), "target", prefix + ((i * 7) % 50), "fact", "f" + i,
          "embedding", List.of(1.0 * i, 2.0)));
    final long[] updates = new long[1];
    database.transaction(() -> {
      final long before = updates(database);
      try (final ResultSet rs = database.command("opencypher", query, Map.of("rows", rows))) {
        assertThat(count(rs)).isEqualTo(400);
      }
      updates[0] = updates(database) - before;
    });
    return updates[0];
  }

  @Test
  void foldedUpsertsCountThePropertiesTheSetStepCounts() {
    // the WITH between the clauses keeps the SET a step of its own
    final String nodeStep = "UNWIND $rows AS row MERGE (n:Entity {uuid: row.uuid}) WITH n, row SET n = row RETURN n.uuid AS uuid";
    assertThat(explain(nodeStep)).contains("SET");
    assertThat(propertiesSet(NODE_UPSERT, List.of(Map.of("uuid", "a", "name", "x", "kind", "k"))))
        .isEqualTo(propertiesSet(nodeStep, List.of(Map.of("uuid", "b", "name", "x", "kind", "k"))));

    final String edgeStep = "UNWIND $rows AS row MATCH (a:Entity {uuid: row.source}) MATCH (b:Entity {uuid: row.target}) "
        + "MERGE (a)-[e:RELATES_TO {uuid: row.uuid}]->(b) WITH e, row SET e = row RETURN row.uuid AS uuid";
    assertThat(explain(edgeStep)).contains("SET");
    assertThat(propertiesSet(EDGE_UPSERT, List.of(Map.of("uuid", "e1", "source", "a", "target", "b", "fact", "f"))))
        .isEqualTo(propertiesSet(edgeStep, List.of(Map.of("uuid", "e2", "source", "a", "target", "b", "fact", "f"))));
  }

  private int propertiesSet(final String query, final List<Map<String, Object>> rows) {
    final int[] set = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, Map.of("rows", rows))) {
        count(rs);
        set[0] = rs.getStatistics().orElseThrow().getPropertiesSet();
      }
    });
    return set[0];
  }

  private static long updates(final Database db) {
    return ((Number) db.getStats().get("updateRecord")).longValue();
  }

  private static int count(final ResultSet rs) {
    int n = 0;
    while (rs.hasNext()) {
      rs.next();
      ++n;
    }
    return n;
  }

  /** The plan of {@code query} for a batch of plain rows, as an application sends it over HTTP or Bolt. */
  private String explain(final String query) {
    final Map<String, Object> rows = Map.of("rows", List.of(Map.of("uuid", "u", "source", "a", "target", "b")), "other",
        List.of(Map.of("uuid", "o")));
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + query, rows)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}

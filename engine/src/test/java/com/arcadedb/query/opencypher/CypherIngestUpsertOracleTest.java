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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests for the batched {@code UNWIND $rows ... MERGE ... SET ... FOREACH ... REMOVE} upsert shapes of a bulk ingest. Every
 * expected value is what Neo4j 5.26 answers to the same statement and parameters (checked against a Neo4j container, and by a
 * metamorphic harness that applies one batch as one UNWIND, one statement per row, two halves and a replay and compares each against
 * Neo4j). Each shape runs with and without a UNIQUE index on the merge key, because the index changes the MERGE access path.
 */
class CypherIngestUpsertOracleTest {
  private static final String ENTITY_UPSERT =
      "UNWIND $rows AS row MERGE (n:Entity {normalized_name: row.norm}) "
          + "WITH n, row, (n.embedding IS NULL OR n.embedding_model IS NULL OR n.embedding_model <> row.model OR row.force = true) AS vector_stale "
          + "WITH n, row, vector_stale, (vector_stale OR n.created_at IS NULL OR n.external_id IS NULL OR n.deleted_at IS NOT NULL "
          + "OR n.deleted IS NULL OR n.deleted <> false "
          + "OR any(k IN keys(row.props) WHERE NOT coalesce(n[k] = row.props[k], false))) AS wrote "
          + "FOREACH (_ IN CASE WHEN wrote THEN [1] ELSE [] END | SET n += row.props, "
          + "n.external_id = CASE WHEN n.deleted_at IS NULL THEN coalesce(n.external_id, row.eid) ELSE row.eid END, "
          + "n.created_at = coalesce(n.created_at, row.now), n.deleted = false REMOVE n.deleted_at) "
          + "RETURN n.normalized_name AS nm, n.external_id AS eid, vector_stale, wrote";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-ingest-upsert-oracle").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  private void schema(final boolean unique, final String type, final String property) {
    database.command("sql", "CREATE VERTEX TYPE " + type + " IF NOT EXISTS");
    if (unique) {
      database.command("sql", "CREATE PROPERTY " + type + "." + property + " IF NOT EXISTS STRING");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON " + type + " (" + property + ") UNIQUE NULL_STRATEGY SKIP");
    }
  }

  private List<Map<String, Object>> command(final String query, final Object... params) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query, params.length == 0 ? new HashMap<>() : paramMap(params))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Map<String, Object> row = new TreeMap<>();
        for (final String name : r.getPropertyNames())
          row.put(name, r.getProperty(name));
        rows.add(row);
      }
    }
    return rows;
  }

  private static Map<String, Object> paramMap(final Object[] params) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < params.length; i += 2)
      map.put((String) params[i], params[i + 1]);
    return map;
  }

  private static Map<String, Object> map(final Object... kv) {
    final Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2)
      m.put((String) kv[i], kv[i + 1]);
    return m;
  }

  private Map<String, Object> node(final String label, final String keyProperty, final Object key) {
    final List<Map<String, Object>> rows = command("MATCH (n:" + label + " {" + keyProperty + ": $k}) RETURN properties(n) AS p", "k", key);
    assertThat(rows).hasSize(1);
    return properties(rows.get(0).get("p"));
  }

  /** The user properties of a node, whole numbers as Long: a literal is stored as the narrowest integer type the engine picks. */
  private static Map<String, Object> properties(final Object value) {
    final Map<String, Object> p = new TreeMap<>(castMap((Map<?, ?>) value));
    p.keySet().removeIf(k -> k.startsWith("@"));
    p.replaceAll((k, v) -> v instanceof Integer || v instanceof Short || v instanceof Byte ? (Object) ((Number) v).longValue() : v);
    return p;
  }

  private Map<String, Object> onlyNode(final String label) {
    final List<Map<String, Object>> rows = command("MATCH (n:" + label + ") RETURN properties(n) AS p");
    assertThat(rows).hasSize(1);
    return properties(rows.get(0).get("p"));
  }

  private long count(final String match) {
    return ((Number) command(match + " RETURN count(*) AS c").get(0).get("c")).longValue();
  }

  private static Map<String, Object> entityRow(final String norm, final String eid, final String now, final Map<String, Object> props,
      final String model, final Boolean force) {
    return map("norm", norm, "eid", eid, "now", now, "props", props, "model", model, "force", force);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void entityUpsertSecondRowOfTheSameKeyMatchesTheFirstRowsNode(final boolean unique) {
    schema(unique, "Entity", "normalized_name");
    final List<Map<String, Object>> rows = List.of(
        entityRow("a", "e1", "t1", map("name", "A", "score", 1L, "tag", "x"), "m", false),
        entityRow("a", "e2", "t2", map("name", "B", "tag", null), "m", false),
        entityRow("b", "e3", "t3", map(), "m", null));

    final List<Map<String, Object>> returned = command(ENTITY_UPSERT, "rows", rows);

    assertThat(count("MATCH (n:Entity)")).isEqualTo(2);
    // The first row created the node, the second MATCHED it: the stored id wins over the second row's id, the second row's
    // name wins, the null "tag" REMOVES the stored property, and the birth stamp is the first row's
    final Map<String, Object> a = node("Entity", "normalized_name", "a");
    assertThat(a).containsEntry("name", "B").containsEntry("score", 1L).containsEntry("external_id", "e1")
        .containsEntry("created_at", "t1").containsEntry("deleted", false).doesNotContainKey("tag").doesNotContainKey("deleted_at");
    assertThat(node("Entity", "normalized_name", "b")).containsEntry("external_id", "e3").containsEntry("created_at", "t3")
        .containsEntry("deleted", false);
    // Every row answers once, in order, and the embedding-less node is stale every time
    assertThat(returned).extracting(r -> r.get("nm")).containsExactly("a", "a", "b");
    assertThat(returned).extracting(r -> r.get("eid")).containsExactly("e1", "e1", "e3");
    assertThat(returned).allSatisfy(r -> assertThat(r.get("vector_stale")).isEqualTo(true));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void entityUpsertGateKeepsAFreshRowAndRevivesATombstone(final boolean unique) {
    schema(unique, "Entity", "normalized_name");
    command("CREATE (:Entity {normalized_name: 'live', external_id: 'L', created_at: 'old', deleted: false, embedding: [0.5, 0.25],"
        + " embedding_model: 'm', name: 'N'})");
    command("CREATE (:Entity {normalized_name: 'dead', external_id: 'D', created_at: 'old', deleted: true, deleted_at: 'gone',"
        + " embedding: [0.5, 0.25], embedding_model: 'm'})");
    final List<Map<String, Object>> rows = List.of(
        entityRow("live", "other", "now", map("name", "N"), "m", false),
        entityRow("dead", "revived", "now", map("name", "back"), "m", false),
        entityRow("live", "other", "now", map("name", "N"), "m2", false));

    final List<Map<String, Object>> returned = command(ENTITY_UPSERT, "rows", rows);

    // Row 1: same model, same props, live, stamped: wrote = false. Row 2: tombstone: wrote, takes the caller's id, loses deleted_at.
    // Row 3: another model: the vector is stale, so the write runs again
    assertThat(returned).extracting(r -> r.get("wrote")).containsExactly(false, true, true);
    assertThat(returned).extracting(r -> r.get("vector_stale")).containsExactly(false, false, true);
    assertThat(node("Entity", "normalized_name", "live")).containsEntry("external_id", "L").containsEntry("created_at", "old")
        .containsEntry("name", "N");
    final Map<String, Object> dead = node("Entity", "normalized_name", "dead");
    assertThat(dead).containsEntry("external_id", "revived").containsEntry("name", "back").containsEntry("deleted", false)
        .containsEntry("created_at", "old").doesNotContainKey("deleted_at");
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void entityUpsertBatchEqualsRowByRowAcrossPullBatchesAndIsIdempotent(final boolean unique) {
    schema(unique, "Entity", "normalized_name");
    // 250 rows over 20 keys cross the 100-row pull batch of the executor; each key's props are the same on every row so the
    // outcome does not depend on the order, only on every row being applied once
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 250; i++) {
      final int key = (i * 7) % 20;
      rows.add(entityRow("k" + key, "id" + key, "now", map("name", "n" + key, "score", (long) key, "ratio", key % 2 == 0 ? 1.5 : 2L), "m",
          false));
    }
    command(ENTITY_UPSERT, "rows", rows);
    final List<Map<String, Object>> expected = state("Entity");
    assertThat(expected).hasSize(20);

    // The replay of the whole batch changes nothing
    command(ENTITY_UPSERT, "rows", rows);
    assertThat(state("Entity")).isEqualTo(expected);

    // One statement per row, from scratch, gives the same graph
    command("MATCH (n:Entity) DETACH DELETE n");
    for (final Map<String, Object> row : rows)
      command(ENTITY_UPSERT, "rows", List.of(row));
    assertThat(state("Entity")).isEqualTo(expected);
  }

  private List<Map<String, Object>> state(final String label) {
    final List<Map<String, Object>> out = new ArrayList<>();
    for (final Map<String, Object> r : command("MATCH (n:" + label + ") RETURN properties(n) AS p ORDER BY n.normalized_name")) {
      out.add(properties(r.get("p")));
    }
    return out;
  }

  @Test
  void setPlusEqualsMapRemovesNullValuesAndSetNullRemovesTheProperty() {
    schema(false, "N", "k");
    command("CREATE (:N {k: 'x', a: 1, b: 2, c: 3})");
    command("MATCH (n:N {k: 'x'}) SET n += {a: null, b: 20, d: 4}");
    assertThat(node("N", "k", "x")).isEqualTo(map("k", "x", "b", 20L, "c", 3L, "d", 4L));

    command("MATCH (n:N {k: 'x'}) SET n.b = null, n.zz = null");
    assertThat(node("N", "k", "x")).isEqualTo(map("k", "x", "c", 3L, "d", 4L));

    command("MATCH (n:N {k: 'x'}) SET n = {e: 5, f: null}");
    assertThat(onlyNode("N")).isEqualTo(map("e", 5L));

    command("UNWIND [{k: 'x', v: null}] AS row MATCH (n:N {e: 5}) SET n.e = row.v, n.g = coalesce(row.v, 'dflt')");
    assertThat(onlyNode("N")).isEqualTo(map("g", "dflt"));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void mergeOnCreateAndOnMatchActionsAreAppliedPerRowInOrder(final boolean unique) {
    schema(unique, "E", "k");
    final List<Map<String, Object>> rows = List.of(map("k", "a", "v", 1L), map("k", "a", "v", null), map("k", "b", "v", 3L),
        map("k", "a", "v", 4L));
    final List<Map<String, Object>> returned = command(
        "UNWIND $rows AS row MERGE (n:E {k: row.k}) ON CREATE SET n.c = 1, n.p = row.v ON MATCH SET n.c = n.c + 1, n.q = row.v RETURN n.c AS c",
        "rows", rows);
    assertThat(returned).extracting(r -> r.get("c")).containsExactly(3L, 3L, 1L, 3L);
    // The rows answer after the whole batch has written (the write is a barrier for the RETURN, as Neo4j's Eager is): the three rows
    // of key "a" all read c = 3. ON MATCH SET q = NULL removes q (row 2) and the next match sets it again
    assertThat(node("E", "k", "a")).isEqualTo(map("k", "a", "c", 3L, "p", 1L, "q", 4L));
    assertThat(node("E", "k", "b")).isEqualTo(map("k", "b", "c", 1L, "p", 3L));
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void mergeWithANullKeyRaisesAndRollsTheWholeStatementBack(final boolean unique) {
    schema(unique, "E", "k");
    final List<Map<String, Object>> rows = List.of(map("k", "a"), map("k", "b"), map("k", null), map("k", "c"));
    assertThatThrownBy(() -> command("UNWIND $rows AS row MERGE (n:E {k: row.k}) RETURN n.k AS k", "rows", rows))
        .hasStackTraceContaining("null");
    assertThat(count("MATCH (n:E)")).isZero();
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void mergeKeyOfAnotherNumericKindMatchesTheStoredNode(final boolean unique) {
    // 1 = 1.0 under Cypher equality, so one node, whichever kind came first
    schema(false, "N", "j");
    final List<Map<String, Object>> rows = List.of(map("j", 1L), map("j", 1.0), map("j", 2.0), map("j", 2L), map("j", "1"));
    command("UNWIND $rows AS row MERGE (n:N {j: row.j}) RETURN n.j AS j", "rows", rows);
    assertThat(count("MATCH (n:N)")).isEqualTo(3);
    assertThat(count("MATCH (n:N {j: 1})")).isEqualTo(1);
    assertThat(count("MATCH (n:N {j: 2.0})")).isEqualTo(1);
    assertThat(count("MATCH (n:N {j: '1'})")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void relationshipMergeWithAndWithoutPropertiesIsOnePerKeyWithinOneBatch(final boolean unique) {
    schema(unique, "E", "k");
    final List<Map<String, Object>> rows = List.of(map("a", "x", "b", "y", "w", 1L), map("a", "x", "b", "y", "w", 1.0),
        map("a", "x", "b", "y", "w", 2L), map("a", "y", "b", "x", "w", 1L), map("a", "x", "b", "x", "w", 1L));
    command("UNWIND $rows AS row MERGE (a:E {k: row.a}) MERGE (b:E {k: row.b}) MERGE (a)-[r:PLAIN]->(b) RETURN a.k AS a", "rows", rows);
    command("UNWIND $rows AS row MERGE (a:E {k: row.a}) MERGE (b:E {k: row.b}) MERGE (a)-[r:WEIGHTED {w: row.w}]->(b) RETURN a.k AS a",
        "rows", rows);
    assertThat(count("MATCH (:E)-[r:PLAIN]->(:E)")).isEqualTo(3);
    // w = 1 and w = 1.0 are the same relationship, w = 2 another one, the other two pairs one each
    assertThat(count("MATCH (:E)-[r:WEIGHTED]->(:E)")).isEqualTo(4);
    assertThat(count("MATCH (:E {k: 'x'})-[r:WEIGHTED]->(:E {k: 'y'})")).isEqualTo(2);
    // MERGE with a null relationship property raises, as for a node
    assertThatThrownBy(() -> command("UNWIND [{a: 'x', b: 'y', w: null}] AS row MATCH (a:E {k: row.a}), (b:E {k: row.b}) MERGE (a)-[r:WEIGHTED {w: row.w}]->(b)"))
        .hasStackTraceContaining("null");
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void graphitiEdgeUpsertReplacesThePropertiesAndDropsNulls(final boolean unique) {
    schema(unique, "Entity", "uuid");
    command("CREATE (:Entity {uuid: 'u1'}), (:Entity {uuid: 'u2'})");
    final String upsert = "UNWIND $rows AS edge MATCH (s:Entity {uuid: edge.src}) MATCH (t:Entity {uuid: edge.dst}) "
        + "MERGE (s)-[e:RELATES_TO {uuid: edge.uuid}]->(t) SET e = edge RETURN edge.uuid AS uuid";
    final List<Map<String, Object>> first = List.of(map("uuid", "g1", "src", "u1", "dst", "u2", "fact", "f", "weight", 1L, "expired", null));
    assertThat(command(upsert, "rows", first)).extracting(r -> r.get("uuid")).containsExactly("g1");
    final List<Map<String, Object>> second = List.of(map("uuid", "g1", "src", "u1", "dst", "u2", "fact", "g", "valid_at", "t"));
    command(upsert, "rows", second);
    assertThat(count("MATCH ()-[e:RELATES_TO]->()")).isEqualTo(1);
    final Map<String, Object> props = castMap((Map<?, ?>) command("MATCH ()-[e:RELATES_TO]->() RETURN properties(e) AS p").get(0).get("p"));
    props.keySet().removeIf(k -> k.startsWith("@"));
    // SET e = edge replaces: "weight" of the first batch is gone, the null "expired" never existed
    assertThat(new TreeMap<>(props)).isEqualTo(map("dst", "u2", "fact", "g", "src", "u1", "uuid", "g1", "valid_at", "t"));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(final Map<?, ?> m) {
    return (Map<String, Object>) m;
  }

  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void callInTransactionsAppliesEveryRowOfEveryBatch(final boolean unique) {
    schema(unique, "E", "k");
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 9; i++)
      rows.add(map("k", "k" + (i % 4), "props", map("seen", (long) i, "even", i % 2 == 0 ? true : null)));
    command("UNWIND $rows AS row CALL { WITH row MERGE (n:E {k: row.k}) SET n += row.props } IN TRANSACTIONS OF 2 ROWS", "rows", rows);
    assertThat(count("MATCH (n:E)")).isEqualTo(4);
    // The last row of each key wins, and a null "even" removes the flag a previous row of the key set
    assertThat(node("E", "k", "k0")).isEqualTo(map("k", "k0", "seen", 8L, "even", true));
    assertThat(node("E", "k", "k1")).isEqualTo(map("k", "k1", "seen", 5L));
    assertThat(node("E", "k", "k2")).isEqualTo(map("k", "k2", "seen", 6L, "even", true));
    assertThat(node("E", "k", "k3")).isEqualTo(map("k", "k3", "seen", 7L));
  }

  @Test
  void factResidualShapeStampsOnceRemovesAndCopiesTheRecordTombstone() {
    schema(true, "Fact", "external_id");
    schema(true, "Record", "external_id");
    command("CREATE (:Fact {external_id: 'f1', deleted_at: 'old'}), (:Fact {external_id: 'f2', created_at: 'keep'})");
    command("CREATE (:Record {external_id: 'r1'}), (:Record {external_id: 'r2', deleted_at: 'rdead'})");
    final List<Map<String, Object>> rows = List.of(map("eid", "f2", "rid", "missing", "now", "n4"), map("eid", "f1", "rid", "r1", "now", "n1"),
        map("eid", "f2", "rid", "r2", "now", "n2"), map("eid", "f1", "rid", "r1", "now", "n3"));
    command("UNWIND $rows AS row MATCH (f:Fact {external_id: row.eid}) SET f.created_at = coalesce(f.created_at, row.now) "
        + "REMOVE f.deleted_at WITH f, row MATCH (r:Record {external_id: row.rid}) MERGE (f)-[:OF_RECORD]->(r) "
        + "SET f.deleted_at = r.deleted_at, f.deleted = r.deleted_at IS NOT NULL", "rows", rows);
    // f1: stamped by its first row, tombstone removed, record alive: deleted_at stays absent, deleted = false
    assertThat(node("Fact", "external_id", "f1")).isEqualTo(map("external_id", "f1", "created_at", "n1", "deleted", false));
    // f2: the dead record's tombstone is copied; the row naming a missing record writes nothing past the MATCH
    assertThat(node("Fact", "external_id", "f2")).isEqualTo(map("external_id", "f2", "created_at", "keep", "deleted_at", "rdead", "deleted", true));
    assertThat(count("MATCH (:Fact)-[:OF_RECORD]->(:Record)")).isEqualTo(2);
  }

  @Test
  void foreachOverANullListDoesNothingAndOverAScalarRunsOnce() {
    schema(false, "T", "v");
    // A row without the list (row.xs is null) is skipped, the row with a list iterates it
    command("UNWIND [{xs: null}, {xs: [1, 2]}, {}] AS row FOREACH (x IN row.xs | CREATE (:T {v: x}))");
    assertThat(count("MATCH (n:T)")).isEqualTo(2);
    command("FOREACH (x IN null | CREATE (:T {v: x}))");
    assertThat(count("MATCH (n:T)")).isEqualTo(2);
    // Neo4j 5.26 treats a non-list value as a one element list
    command("FOREACH (x IN 5 | CREATE (:T {v: x}))");
    command("FOREACH (x IN 'ab' | CREATE (:T {v: x}))");
    assertThat(count("MATCH (n:T)")).isEqualTo(4);
    assertThat(count("MATCH (n:T {v: 5})")).isEqualTo(1);
    assertThat(count("MATCH (n:T {v: 'ab'})")).isEqualTo(1);
  }
}

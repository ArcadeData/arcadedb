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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
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
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The vector index on the facts of a knowledge graph (an edge type, {@code fact_embedding}) written the way Graphiti writes
 * them: {@code MERGE (s)-[e:RELATES_TO {uuid: ...}]->(t) SET e = $edge_data}, where the map carries the embedding, a null
 * embedding or no embedding at all, {@code DELETE e}, and {@code DETACH DELETE n} of an endpoint, which takes the edges with
 * it. A brute-force model decides the answer of every search, inside the transaction that wrote and after the commit, a
 * rollback, a graph build and a reopen: the facts of the tenant that hold a vector, each at the distance to its CURRENT
 * vector, and nothing else - not a fact that was deleted, replaced by a map without its vector, or rolled back.
 */
class VectorGraphitiLifecycleOracleTest extends TestHelper {
  private static final int    DIM       = 8;
  private static final int    NODES     = 6;
  private static final String INDEX     = "RELATES_TO[fact_embedding]";
  private static final String SAVE      = """
      MATCH (source:Entity {uuid: $edge_data.source_uuid})
      MATCH (target:Entity {uuid: $edge_data.target_uuid})
      MERGE (source)-[e:RELATES_TO {uuid: $edge_data.uuid}]->(target)
      SET e = $edge_data
      RETURN e.uuid AS uuid""";
  private static final String SAVE_NODE = """
      MERGE (n:Entity {uuid: $entity_data.uuid})
      SET n = $entity_data
      RETURN n.uuid AS uuid""";

  /** What the store holds for one fact; {@code vector} is null for a fact with a null embedding or none. */
  private record Fact(float[] vector, String group, String source, String target) {
  }

  private Map<String, Fact> committed      = new LinkedHashMap<>();
  private Map<String, Fact> current        = new LinkedHashMap<>();
  private Set<String>       committedNodes = new TreeSet<>();
  private Set<String>       currentNodes   = new TreeSet<>();

  @BeforeEach
  void freezeTheGraph() {
    // Nothing may fold a pending or buffered row into the graph behind the test's back: the transaction overlay and the
    // delta buffer are what is under test next to the graph the explicit build makes.
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(1_000_000);
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.setValue(0f);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void thawTheGraph() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.reset();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO.reset();
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.reset();
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Entity BUCKETS 1");
      database.command("sql", "CREATE EDGE TYPE RELATES_TO BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
      database.command("sql", "CREATE PROPERTY RELATES_TO.fact_embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY RELATES_TO.group_id STRING");
    });
    database.command("opencypher", "CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)").close();
    database.command("opencypher", "CREATE VECTOR INDEX edge_fact_embedding IF NOT EXISTS FOR ()-[e:RELATES_TO]-() "
        + "ON e.fact_embedding OPTIONS {indexConfig: {`vector.dimensions`: " + DIM + ", `vector.similarity_function`: 'cosine', "
        + "`vector.quantization.type`: 'none'}}").close();
    for (int i = 0; i < NODES; i++) {
      database.command("opencypher", SAVE_NODE, Map.of("entity_data", Map.of("uuid", "n" + i))).close();
      currentNodes.add("n" + i);
    }
    settle();
  }

  // ---------------------------------------------------------------------------------------------------- the scenarios

  @Test
  void aFactSavedInATransactionIsSearchableByItAndGoneAfterARollback() {
    seed(20);
    final float[] v = vector(new Random(1));

    begin();
    save("fresh", "n0", "n1", "g1", v, true);
    assertThat(search(v, 3, null)).as("the fact it just saved is the nearest to its own vector").containsKey("fresh");
    assertMatchesModel("inside the transaction");

    rollback();
    assertThat(search(v, 50, null)).doesNotContainKey("fresh");
    assertMatchesModel("after the rollback");
  }

  @Test
  void savingAFactAgainWithAnotherVectorLeavesOneRowScoredAtTheLastOne() {
    seed(20);
    final Random random = new Random(2);

    begin();
    save("e3", "n3", "n0", "g1", vector(random), true);
    final float[] last = vector(random);
    save("e3", "n3", "n0", "g1", last, true);
    assertThat(search(last, 50, null).get("e3")).as("scored once, at the vector it holds now").isCloseTo(0.0, within(1e-4));
    assertMatchesModel("inside the transaction");
    commit();

    assertThat(search(last, 50, null).get("e3")).isCloseTo(0.0, within(1e-4));
    assertMatchesModel("after the commit");
  }

  @Test
  void aMapWithANullOrNoEmbeddingTakesTheVectorOutOfTheIndex() {
    seed(30);
    final Random random = new Random(3);

    // inside one transaction: a null embedding, a map without the key, and a new vector, in turn
    begin();
    save("e1", "n1", "n2", "g2", null, true);
    save("e2", "n2", "n3", "g3", null, false);
    save("e3", "n3", "n0", "g1", vector(random), true);
    assertThat(search(vector(random), 100, null)).doesNotContainKeys("e1", "e2").containsKey("e3");
    assertMatchesModel("inside the transaction");
    commit();
    assertMatchesModel("after the commit");

    // and the same replacement of a vector that is already committed, outside any transaction
    save("e4", "n0", "n1", "g2", null, true);
    save("e5", "n1", "n2", "g3", null, false);
    settle();
    assertThat(search(vector(random), 100, null)).doesNotContainKeys("e1", "e2", "e4", "e5");
    assertMatchesModel("after the autocommitted re-saves");

    reopen();
    assertMatchesModel("after the reopen");
  }

  @Test
  void aDeletedFactAnswersNothingInItsTransactionAndAfterTheCommit() {
    seed(20);
    final float[] v = current.get("e5").vector();

    begin();
    delete("e5");
    assertThat(search(v, 50, null)).doesNotContainKey("e5");
    assertMatchesModel("inside the transaction");
    final float[] born = vector(new Random(4));
    save("born", "n2", "n3", "g2", born, true);
    delete("born");
    assertThat(search(born, 50, null)).as("added and deleted by the same transaction").doesNotContainKey("born");
    assertMatchesModel("added and deleted");
    commit();

    assertThat(search(v, 50, null)).doesNotContainKey("e5");
    assertMatchesModel("after the commit");
    reopen();
    assertMatchesModel("after the reopen");
  }

  @Test
  void detachDeletingAnEndpointTakesTheVectorsOfItsFactsOutOfTheIndex() {
    seed(24);
    final List<String> facts = new ArrayList<>();
    for (final Map.Entry<String, Fact> e : current.entrySet())
      if (e.getValue().source().equals("n0") || e.getValue().target().equals("n0"))
        facts.add(e.getKey());
    assertThat(facts).as("fixture: the endpoint has facts").isNotEmpty();

    begin();
    detachDelete("n0");
    assertThat(search(vector(new Random(5)), 100, null)).doesNotContainKeys(facts.toArray(new String[0]));
    assertMatchesModel("inside the transaction");
    commit();
    assertMatchesModel("after the commit");

    buildGraphs();
    assertMatchesModel("after a graph build");
    reopen();
    assertMatchesModel("after the reopen");
  }

  /**
   * Seeded random lifecycles against the model: saves of new and existing facts (with a vector, a null one, none), deletes, the
   * detach delete of an endpoint, begin/commit/rollback, graph builds and reopens, a search inside and outside the transaction
   * after every few operations.
   */
  @ParameterizedTest
  @ValueSource(longs = { 11, 12, 13, 14, 15, 16, 17, 18 })
  void aRandomLifecycleOfFactsMatchesABruteForceModel(final long seed) {
    final Random random = new Random(seed);
    seed(15);
    int nextId = 1000;

    for (int op = 0; op < 70; op++) {
      final int r = random.nextInt(100);
      final List<String> facts = new ArrayList<>(current.keySet());
      if (r < 28) {
        final boolean existing = random.nextInt(3) == 0 && !facts.isEmpty();
        final String uuid = existing ? facts.get(random.nextInt(facts.size())) : "f" + nextId++;
        final Fact old = current.get(uuid);
        final String source = old != null ? old.source() : "n" + random.nextInt(NODES);
        final String target = old != null ? old.target() : "n" + random.nextInt(NODES);
        final int mode = random.nextInt(8);
        save(uuid, source, target, "g" + (1 + random.nextInt(3)), mode == 0 ? null : vector(random), mode != 1);
      } else if (r < 38) {
        if (!facts.isEmpty())
          delete(facts.get(random.nextInt(facts.size())));
      } else if (r < 42) {
        detachDelete("n" + random.nextInt(NODES));
      } else if (r < 52) {
        if (!database.isTransactionActive())
          begin();
      } else if (r < 64) {
        if (database.isTransactionActive())
          commit();
      } else if (r < 72) {
        if (database.isTransactionActive())
          rollback();
      } else if (r < 76) {
        if (!database.isTransactionActive())
          buildGraphs();
      } else if (r < 79) {
        if (!database.isTransactionActive())
          reopen();
      }
      settle();
      if (random.nextInt(3) == 0)
        assertMatchesModel("seed " + seed + " op " + op + (database.isTransactionActive() ? " (in a transaction)" : ""));
    }
    if (database.isTransactionActive())
      commit();
    assertMatchesModel("seed " + seed + " end");
    reopen();
    assertMatchesModel("seed " + seed + " after the reopen");
  }

  // ----------------------------------------------------------------------------------------------- statements and model

  private void seed(final int facts) {
    final Random random = new Random(facts);
    for (int i = 0; i < facts; i++)
      save("e" + i, "n" + (i % 4), "n" + ((i + 1) % 4), "g" + (1 + i % 3), vector(random), true);
    settle();
    buildGraphs();
  }

  /** Outside a transaction every statement committed itself: the model's committed view catches up. */
  private void settle() {
    if (!database.isTransactionActive()) {
      committed = new LinkedHashMap<>(current);
      committedNodes = new TreeSet<>(currentNodes);
    }
  }

  private void begin() {
    database.begin();
  }

  private void commit() {
    database.commit();
    committed = new LinkedHashMap<>(current);
    committedNodes = new TreeSet<>(currentNodes);
  }

  private void rollback() {
    database.rollback();
    current = new LinkedHashMap<>(committed);
    currentNodes = new TreeSet<>(committedNodes);
  }

  private void reopen() {
    reopenDatabase();
  }

  private void save(final String uuid, final String source, final String target, final String group, final float[] vector,
      final boolean keyPresent) {
    final Map<String, Object> data = new HashMap<>();
    data.put("uuid", uuid);
    data.put("source_uuid", source);
    data.put("target_uuid", target);
    data.put("group_id", group);
    if (keyPresent)
      data.put("fact_embedding", vector == null ? null : toList(vector));
    database.command("opencypher", SAVE, Map.of("edge_data", data)).close();
    // the statement matches nothing when an endpoint is gone: no fact is written
    if (currentNodes.contains(source) && currentNodes.contains(target))
      current.put(uuid, new Fact(keyPresent ? vector : null, group, source, target));
  }

  private void delete(final String uuid) {
    database.command("opencypher", "MATCH ()-[e:RELATES_TO {uuid: $uuid}]->() DELETE e", Map.of("uuid", uuid)).close();
    current.remove(uuid);
  }

  private void detachDelete(final String node) {
    database.command("opencypher", "MATCH (n:Entity {uuid: $uuid}) DETACH DELETE n", Map.of("uuid", node)).close();
    currentNodes.remove(node);
    current.values().removeIf(f -> f.source().equals(node) || f.target().equals(node));
  }

  private void assertMatchesModel(final String when) {
    final Random random = new Random(when.hashCode());
    for (final String group : new String[] { null, "g1", "g2", "g3" }) {
      final float[] q = vector(random);
      final Map<String, float[]> expected = new LinkedHashMap<>();
      for (final Map.Entry<String, Fact> e : current.entrySet())
        if (e.getValue().vector() != null && (group == null || group.equals(e.getValue().group())))
          expected.put(e.getKey(), e.getValue().vector());

      final Map<String, Double> answer = search(q, expected.size() + 10, group);
      assertThat(answer.keySet()).as("%s: exhaustive search of group %s", when, group).isEqualTo(expected.keySet());
      for (final Map.Entry<String, Double> row : answer.entrySet())
        assertThat(row.getValue()).as("%s: distance of %s (group %s)", when, row.getKey(), group)
            .isCloseTo(distance(q, expected.get(row.getKey())), within(1e-4));

      if (expected.size() > 3) {
        final List<String> nearest = new ArrayList<>(expected.keySet());
        nearest.sort((a, b) -> Double.compare(distance(q, expected.get(a)), distance(q, expected.get(b))));
        assertThat(search(q, 3, group).keySet()).as("%s: the three nearest of group %s", when, group)
            .containsExactlyInAnyOrderElementsOf(nearest.subList(0, 3));
      }
    }
    if (!database.isTransactionActive())
      assertThat(countEntries()).as("%s: entries of the vector index", when)
          .isEqualTo(current.values().stream().filter(f -> f.vector() != null).count());
  }

  private long countEntries() {
    long total = 0;
    for (final Index index : database.getSchema().getType("RELATES_TO").getAllIndexes(true))
      if (index instanceof TypeIndex typeIndex && typeIndex.getName().contains("fact_embedding"))
        total += typeIndex.countEntries();
    return total;
  }

  private void buildGraphs() {
    for (final Index index : database.getSchema().getType("RELATES_TO").getAllIndexes(true))
      if (index instanceof TypeIndex typeIndex)
        for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
          if (bucketIndex instanceof LSMVectorIndex vectorIndex)
            vectorIndex.buildVectorGraphNow();
  }

  private Map<String, Double> search(final float[] q, final int k, final String group) {
    final Map<String, Object> params = new HashMap<>();
    params.put("q", q);
    params.put("k", k);
    String options = "";
    if (group != null) {
      params.put("g", group);
      options = ", {where: 'group_id = :g'}";
    }
    final Map<String, Double> answer = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT uuid, distance FROM (SELECT expand(vector.neighbors('" + INDEX + "', :q, :k" + options + ")))", params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String uuid = row.getProperty("uuid");
        assertThat(answer.put(uuid, ((Number) row.getProperty("distance")).doubleValue())).as("fact %s answered twice", uuid).isNull();
      }
    }
    return answer;
  }

  private static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }

  private static float[] vector(final Random random) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) random.nextGaussian();
    return v;
  }

  private static List<Double> toList(final float[] v) {
    final List<Double> list = new ArrayList<>(v.length);
    for (final float f : v)
      list.add((double) f);
    return list;
  }
}

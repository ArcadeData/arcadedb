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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * What Graphiti's ArcadeDB driver relies on from {@code CALL db.index.vector.queryNodes(index, k, vector[, {where}])}: the score
 * scale ({@code (1 + cosine) / 2}), the order, a {@code k} larger than the data, the error text that tells a missing index, a
 * {@code YIELD ... WHERE} and a filter after the call over the over-fetched window, the pre-filter {@code where} option, the call
 * inside an UNWIND, the {@code count/min} window query, and a relationship vector index.
 * <p>
 * The oracle is brute force: the cosine of the query with every stored vector, computed here from the floats the test wrote.
 */
class CypherVectorQueryNodesGraphitiContractTest extends TestHelper {
  private static final int    DIMENSIONS = 8;
  private static final int    ENTITIES   = 60;
  private static final int    EDGES      = 30;
  private static final double EPSILON    = 1e-4;

  private final Map<String, float[]> entityVectors = new HashMap<>();
  private final Map<String, float[]> edgeVectors   = new HashMap<>();
  private final Map<String, String>  groups        = new HashMap<>();
  private final Map<String, String>  kinds         = new HashMap<>();

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
    database.command("sql", "CREATE PROPERTY Entity.group_id STRING");
    database.command("sql", "CREATE PROPERTY Entity.kind STRING");
    database.command("sql", "CREATE PROPERTY Entity.name_embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Entity (group_id) NOTUNIQUE");
    database.command("sql",
        "CREATE INDEX ON Entity (name_embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");
    database.command("sql", "CREATE EDGE TYPE RELATES_TO");
    database.command("sql", "CREATE PROPERTY RELATES_TO.uuid STRING");
    database.command("sql", "CREATE PROPERTY RELATES_TO.fact_embedding ARRAY_OF_FLOATS");
    database.command("sql",
        "CREATE INDEX ON RELATES_TO (fact_embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");

    final Random random = new Random(20261123L);
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < ENTITIES; i++) {
        final String uuid = "u" + i;
        final float[] vector = randomVector(random);
        final MutableVertex v = database.newVertex("Entity").set("uuid", uuid).set("group_id", "g" + (i % 3))
            .set("kind", "k" + (i % 2)).set("name_embedding", vector);
        v.save();
        vertices.add(v);
        entityVectors.put(uuid, vector);
        groups.put(uuid, "g" + (i % 3));
        kinds.put(uuid, "k" + (i % 2));
      }
      for (int i = 0; i < EDGES; i++) {
        final String uuid = "r" + i;
        final float[] vector = randomVector(random);
        final MutableDocument edge = vertices.get(i).newEdge("RELATES_TO", vertices.get(i + 1)).set("uuid", uuid)
            .set("fact_embedding", vector);
        edge.save();
        edgeVectors.put(uuid, vector);
      }
    });
  }

  // ---- score scale, order, k ----------------------------------------------------------------------------------------------------

  @Test
  void scoresAreOnePlusCosineOverTwoNearestFirst() {
    final float[] query = randomVector(new Random(1));
    final List<Hit> expected = bruteForce(entityVectors, query, null);

    final List<Hit> hits = hits(
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', 10, $v) YIELD node, score RETURN node.uuid AS id, score",
        Map.of("v", query));

    assertThat(hits).hasSize(10);
    assertThat(hits.stream().map(h -> h.id).toList()).isEqualTo(expected.subList(0, 10).stream().map(h -> h.id).toList());
    for (int i = 0; i < 10; i++)
      assertThat(hits.get(i).score).isCloseTo(expected.get(i).score, within(EPSILON));
    for (int i = 1; i < hits.size(); i++)
      assertThat(hits.get(i).score).isLessThanOrEqualTo(hits.get(i - 1).score);
  }

  @Test
  void aKLargerThanTheDataAnswersEveryRecordOnce() {
    final float[] query = randomVector(new Random(2));
    for (final Object k : new Object[] { ENTITIES, ENTITIES + 1, 1000, Integer.MAX_VALUE, Long.MAX_VALUE, "1000", 1000.0d }) {
      final List<Hit> hits = hits(
          "CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD node, score RETURN node.uuid AS id, score",
          Map.of("v", query, "k", k));
      assertThat(hits).as("k=" + k + " (" + k.getClass().getSimpleName() + ")").hasSize(ENTITIES);
      assertThat(hits.stream().map(h -> h.id).distinct().count()).as("k=" + k).isEqualTo(ENTITIES);
    }
  }

  @Test
  void aKOfZeroAnswersNoRowAndANegativeOrNullOrTextKIsRefusedByName() {
    final float[] query = randomVector(new Random(3));
    assertThat(hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 0, $v) YIELD node, score RETURN node.uuid AS id, score",
        Map.of("v", query))).isEmpty();

    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', -1, $v) YIELD node, score RETURN node.uuid AS id, score",
        Map.of("v", query))).hasMessageContaining("k must not be negative");
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', 'many', $v) YIELD node, score RETURN node.uuid AS id, score",
        Map.of("v", query))).hasMessageContaining("k must be a number");
    final Map<String, Object> nullK = new HashMap<>();
    nullK.put("v", query);
    nullK.put("k", null);
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD node, score RETURN node.uuid AS id, score", nullK))
        .hasMessageContaining("k must be a number, got null");
  }

  @Test
  void theQueryVectorMayArriveAsAnyNumericArrayOrList() {
    final float[] query = new float[] { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f };
    final List<Hit> expected = bruteForce(entityVectors, query, null);
    final double[] asDoubles = new double[DIMENSIONS];
    final long[] asLongs = new long[DIMENSIONS];
    final Object[] asObjects = new Object[DIMENSIONS];
    final List<Object> asList = new ArrayList<>();
    final List<Float> asFloatList = new ArrayList<>();
    for (int i = 0; i < DIMENSIONS; i++) {
      asDoubles[i] = query[i];
      asLongs[i] = (long) query[i];
      asObjects[i] = (int) query[i];
      asList.add((double) query[i]);
      asFloatList.add(query[i]);
    }
    final String cypher = "CALL db.index.vector.queryNodes('Entity[name_embedding]', 5, $v) YIELD node, score RETURN node.uuid AS id, score";
    final List<String> ids = expected.subList(0, 5).stream().map(h -> h.id).toList();
    for (final Object form : new Object[] { query, asDoubles, asLongs, asObjects, asList, asFloatList }) {
      final List<Hit> hits = hits(cypher, Map.of("v", form));
      assertThat(hits.stream().map(h -> h.id).toList()).as(form.getClass().getSimpleName()).isEqualTo(ids);
      for (int i = 0; i < 5; i++)
        assertThat(hits.get(i).score).as(form.getClass().getSimpleName()).isCloseTo(expected.get(i).score, within(EPSILON));
    }
    // a literal list written in the statement
    final List<Hit> literal = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 5, [1, 2, 3, 4, 5, 6, 7, 8]) "
        + "YIELD node, score RETURN node.uuid AS id, score", Map.of());
    assertThat(literal.stream().map(h -> h.id).toList()).isEqualTo(ids);
  }

  @Test
  void aNullQueryVectorIsRefusedByName() {
    final Map<String, Object> params = new HashMap<>();
    params.put("v", null);
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Entity[name_embedding]', 3, $v) YIELD node, score RETURN node.uuid AS id, score", params))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("Vector parameter is null");
  }

  // ---- the error text Graphiti matches to tell that the index is missing ---------------------------------------------------------

  @Test
  void aTypeWithoutTheIndexNamesThePropertyAndTheType() {
    database.command("sql", "CREATE VERTEX TYPE Episodic");
    database.command("sql", "CREATE PROPERTY Episodic.name_embedding ARRAY_OF_FLOATS");
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Episodic[name_embedding]', 3, [1, 0, 0, 0, 0, 0, 0, 0]) YIELD node, score RETURN node", Map.of()))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("No vector index found on property 'name_embedding' for type 'Episodic'");
    assertThatThrownBy(() -> hits(
        "OPTIONAL CALL db.index.vector.queryNodes('Episodic[name_embedding]', 3, [1, 0, 0, 0, 0, 0, 0, 0]) YIELD node, score RETURN node",
        Map.of())).hasMessageContaining("No vector index found on property 'name_embedding' for type 'Episodic'");
  }

  @Test
  void aMissingTypeIsNamedAsNotFound() {
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('Community[name_embedding]', 3, [1, 0, 0, 0, 0, 0, 0, 0]) YIELD node, score RETURN node", Map.of()))
        .hasMessageContaining("Type with name 'Community' was not found");
  }

  @Test
  void aNameThatIsNeitherATypePropertyNorAnIndexIsNotAVectorIndex() {
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('nothing', 3, [1, 0, 0, 0, 0, 0, 0, 0]) YIELD node, score RETURN node", Map.of()))
        .hasMessageContaining("Index with name 'nothing' was not found");
    // a '[' as the first character is no 'Type[property]' form either
    assertThatThrownBy(() -> hits(
        "CALL db.index.vector.queryNodes('[name_embedding]', 3, [1, 0, 0, 0, 0, 0, 0, 0]) YIELD node, score RETURN node", Map.of()))
        .hasMessageContaining("Index with name '[name_embedding]' was not found");
  }

  // ---- YIELD ... WHERE, and a filter after the call over the over-fetched window ---------------------------------------------------

  @Test
  void yieldWhereKeepsTheRowsOverTheScoreInOrder() {
    final float[] query = randomVector(new Random(4));
    final List<Hit> expected = bruteForce(entityVectors, query, null);
    final double cut = expected.get(14).score;

    final List<Hit> hits = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 40, $v) YIELD node AS e, score "
        + "WHERE score > $cut RETURN e.uuid AS id, score ORDER BY score DESC", Map.of("v", query, "cut", cut));
    assertThat(hits.stream().map(h -> h.id).toList()).isEqualTo(expected.subList(0, 14).stream().map(h -> h.id).toList());
  }

  @Test
  void aFilterAfterTheCallRunsOnTheOverFetchedWindow() {
    // Graphiti asks for k = 4 * limit, then WITH ... WHERE score > min AND <filters> ... ORDER BY score DESC LIMIT limit
    final float[] query = randomVector(new Random(5));
    final int limit = 5;
    final List<Hit> window = bruteForce(entityVectors, query, null).subList(0, limit * 4);
    final double minScore = window.get(window.size() - 1).score - 0.2;
    final List<String> expected = window.stream().filter(h -> h.score > minScore && "k0".equals(kinds.get(h.id))).limit(limit)
        .map(h -> h.id).toList();

    final List<Hit> hits = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD node AS e, score "
            + "WITH e, score WHERE score > $min_score AND e.kind = $kind RETURN e.uuid AS id, score ORDER BY score DESC LIMIT $limit",
        Map.of("v", query, "k", limit * 4, "min_score", minScore, "kind", "k0", "limit", limit));
    assertThat(hits.stream().map(h -> h.id).toList()).isEqualTo(expected);
  }

  // ---- the pre-filter option ------------------------------------------------------------------------------------------------------

  @Test
  void theWhereOptionRestrictsTheSearchToTheGroupAndStillAnswersK() {
    final float[] query = randomVector(new Random(6));
    final List<Hit> inGroup = bruteForce(entityVectors, query, id -> "g1".equals(groups.get(id)));
    assertThat(inGroup).hasSize(ENTITIES / 3);

    // equality on the indexed property with a named parameter, the form Graphiti sends for one group
    final List<Hit> one = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 7, $v, {where: 'group_id = :group_id'}) "
        + "YIELD node AS e, score RETURN e.uuid AS id, score ORDER BY score DESC", Map.of("v", query, "group_id", "g1"));
    assertThat(one.stream().map(h -> h.id).toList()).isEqualTo(inGroup.subList(0, 7).stream().map(h -> h.id).toList());
    for (int i = 0; i < 7; i++)
      assertThat(one.get(i).score).isCloseTo(inGroup.get(i).score, within(EPSILON));

    // IN with a list parameter, the form for several groups
    final List<Hit> inGroups = bruteForce(entityVectors, query, id -> !"g2".equals(groups.get(id)));
    final List<Hit> several = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 9, $v, {where: 'group_id IN :group_ids'}) "
        + "YIELD node AS e, score RETURN e.uuid AS id, score ORDER BY score DESC", Map.of("v", query, "group_ids", List.of("g0", "g1")));
    assertThat(several.stream().map(h -> h.id).toList()).isEqualTo(inGroups.subList(0, 9).stream().map(h -> h.id).toList());

    // the map form, and a filter that excludes everything
    final List<Hit> asMap = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 4, $v, {where: {group_id: 'g2'}}) "
        + "YIELD node AS e, score RETURN e.uuid AS id, score", Map.of("v", query));
    assertThat(asMap).hasSize(4);
    assertThat(asMap).allSatisfy(h -> assertThat(groups.get(h.id)).isEqualTo("g2"));
    assertThat(hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 4, $v, {where: 'group_id = :g'}) "
        + "YIELD node AS e, score RETURN e.uuid AS id, score", Map.of("v", query, "g", "nobody"))).isEmpty();
  }

  @Test
  void aKLargerThanTheGroupAnswersTheWholeGroup() {
    final float[] query = randomVector(new Random(7));
    final List<Hit> hits = hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 500, $v, {where: 'group_id = :g'}) "
        + "YIELD node AS e, score RETURN e.uuid AS id, score", Map.of("v", query, "g", "g0"));
    assertThat(hits).hasSize(ENTITIES / 3);
    assertThat(hits).allSatisfy(h -> assertThat(groups.get(h.id)).isEqualTo("g0"));
  }

  @Test
  void anUnknownOptionIsRefusedByName() {
    assertThatThrownBy(() -> hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 4, [1, 0, 0, 0, 0, 0, 0, 0], {wher: 'a = 1'}) "
        + "YIELD node, score RETURN node", Map.of())).hasMessageContaining("wher");
    assertThatThrownBy(() -> hits("CALL db.index.vector.queryNodes('Entity[name_embedding]', 4, [1, 0, 0, 0, 0, 0, 0, 0], 'g0') "
        + "YIELD node, score RETURN node", Map.of())).hasMessageContaining("options must be a map");
  }

  // ---- the call inside an UNWIND, and the window query -----------------------------------------------------------------------------

  @Test
  void theCallInsideAnUnwindAnswersEveryRowItsOwnNeighbours() {
    final List<Map<String, Object>> queries = new ArrayList<>();
    final List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      final float[] v = randomVector(new Random(100 + i));
      vectors.add(v);
      queries.add(Map.of("qi", i, "vec", v));
    }
    final Map<Integer, List<String>> byQuery = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", "UNWIND $qs AS q "
        + "CALL db.index.vector.queryNodes('Entity[name_embedding]', 3, q.vec) YIELD node, score "
        + "RETURN q.qi AS qi, node.uuid AS id, score ORDER BY qi ASC, score DESC", Map.of("qs", queries))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        byQuery.computeIfAbsent(((Number) r.getProperty("qi")).intValue(), x -> new ArrayList<>()).add(r.getProperty("id"));
      }
    }
    assertThat(byQuery).hasSize(4);
    for (int i = 0; i < 4; i++)
      assertThat(byQuery.get(i)).as("query " + i)
          .isEqualTo(bruteForce(entityVectors, vectors.get(i), null).subList(0, 3).stream().map(h -> h.id).toList());
  }

  @Test
  void theWindowQueryCountsTheCandidatesAndNamesTheWeakest() {
    final float[] query = randomVector(new Random(8));
    final List<Hit> expected = bruteForce(entityVectors, query, null);
    final int k = 12;

    try (final ResultSet rs = database.query("opencypher", "CALL db.index.vector.queryNodes('Entity[name_embedding]', $k, $v) YIELD score "
        + "RETURN count(score) AS candidates, min(score) AS lowest", Map.of("v", query, "k", k))) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("candidates")).intValue()).isEqualTo(k);
      assertThat(((Number) r.getProperty("lowest")).doubleValue()).isCloseTo(expected.get(k - 1).score, within(EPSILON));
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(count("CALL db.index.vector.queryNodes('Entity[name_embedding]', 12, $v) YIELD score RETURN count(*) AS c", Map.of("v", query)))
        .isEqualTo(12);
    assertThat(count("CALL db.index.vector.queryNodes('Entity[name_embedding]', 1000, $v) YIELD score RETURN count(*) AS c", Map.of("v", query)))
        .isEqualTo(ENTITIES);
    // a YIELD WHERE drops rows the size alone still counts
    final double cut = expected.get(19).score;
    assertThat(count("CALL db.index.vector.queryNodes('Entity[name_embedding]', 40, $v) YIELD score WHERE score > $cut RETURN count(*) AS c",
        Map.of("v", query, "cut", cut))).isEqualTo(19);
    // a group with no member and one with members, the count of the group's window
    assertThat(count("CALL db.index.vector.queryNodes('Entity[name_embedding]', 50, $v, {where: 'group_id = :g'}) YIELD score RETURN count(*) AS c",
        Map.of("v", query, "g", "g1"))).isEqualTo(ENTITIES / 3);
    assertThat(count("CALL db.index.vector.queryNodes('Entity[name_embedding]', 50, $v, {where: 'group_id = :g'}) YIELD score RETURN count(*) AS c",
        Map.of("v", query, "g", "nobody"))).isEqualTo(0);
    // the same call per input row: every row counts its own window
    assertThat(count("UNWIND [1, 2, 3] AS x CALL db.index.vector.queryNodes('Entity[name_embedding]', 5, $v) YIELD score RETURN count(*) AS c",
        Map.of("v", query))).isEqualTo(15);
  }

  // ---- a relationship vector index ----------------------------------------------------------------------------------------------------

  @Test
  void aRelationshipVectorIndexAnswersTheRelationshipsNearestFirst() {
    final float[] query = randomVector(new Random(9));
    final List<Hit> expected = bruteForce(edgeVectors, query, null);

    final List<Hit> hits = hits("CALL db.index.vector.queryNodes('RELATES_TO[fact_embedding]', 6, $v) YIELD node AS e, score "
        + "RETURN e.uuid AS id, score ORDER BY score DESC", Map.of("v", query));
    assertThat(hits.stream().map(h -> h.id).toList()).isEqualTo(expected.subList(0, 6).stream().map(h -> h.id).toList());
    for (int i = 0; i < 6; i++)
      assertThat(hits.get(i).score).isCloseTo(expected.get(i).score, within(EPSILON));

    assertThat(hits("CALL db.index.vector.queryNodes('RELATES_TO[fact_embedding]', 500, $v) YIELD node AS e, score RETURN e.uuid AS id, score",
        Map.of("v", query))).hasSize(EDGES);
  }

  // ---- helpers ----------------------------------------------------------------------------------------------------------------------------

  private record Hit(String id, double score) {
  }

  private List<Hit> hits(final String cypher, final Map<String, Object> params) {
    final List<Hit> hits = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        hits.add(new Hit(r.getProperty("id"), ((Number) r.getProperty("score")).doubleValue()));
      }
    }
    return hits;
  }

  private int count(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      final int c = ((Number) rs.next().getProperty("c")).intValue();
      assertThat(rs.hasNext()).isFalse();
      return c;
    }
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = (float) (random.nextGaussian());
    return v;
  }

  /** Every vector scored as (1 + cosine) / 2, nearest first; {@code only} narrows the candidates (null: all). */
  private static List<Hit> bruteForce(final Map<String, float[]> vectors, final float[] query,
      final java.util.function.Predicate<String> only) {
    return vectors.entrySet().stream().filter(e -> only == null || only.test(e.getKey()))
        .map(e -> new Hit(e.getKey(), (1.0 + cosine(query, e.getValue())) / 2.0))
        .sorted(Comparator.comparingDouble((Hit h) -> h.score).reversed()).collect(Collectors.toList());
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return dot / (Math.sqrt(na) * Math.sqrt(nb));
  }
}

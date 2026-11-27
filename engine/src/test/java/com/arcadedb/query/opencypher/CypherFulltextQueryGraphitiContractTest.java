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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shapes Graphiti's ArcadeDB driver sends to {@code db.index.fulltext.queryNodes} / {@code queryRelationships}: a named
 * index created by Cypher DDL over {@code [name, summary, group_id]}, a Lucene query {@code (group_id:"a" OR group_id:"b") AND
 * (terms)}, the {@code {limit: $limit}} option, and a filter after the call that never changes which rows the call answered.
 * <p>
 * The oracle is the data: a node matches a term when the lower-cased words of its name or summary hold it, and a group when its
 * group_id is that one; the scores are only required to be positive and the rows to come back by score, highest first.
 */
class CypherFulltextQueryGraphitiContractTest extends TestHelper {
  private static final String[] WORDS = { "alice", "bob", "carol", "dave", "paris", "rome", "engineer", "doctor", "friend", "owner",
      "garden", "river" };
  private static final int      NODES = 150;

  private final Map<String, Set<String>> words  = new HashMap<>();
  private final Map<String, String>      groups = new HashMap<>();

  @Override
  protected void beginTest() {
    database.command("opencypher",
        "CREATE FULLTEXT INDEX node_name_and_summary IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary, n.group_id]");
    database.command("opencypher",
        "CREATE FULLTEXT INDEX edge_name_and_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]");
    final Random random = new Random(20261123L);
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < NODES; i++) {
        final String uuid = "n" + i;
        final String name = pick(random) + " " + pick(random);
        final String summary = pick(random) + " " + pick(random) + " " + pick(random);
        final String group = "g" + random.nextInt(4);
        final MutableVertex v = database.newVertex("Entity").set("uuid", uuid).set("name", name).set("summary", summary)
            .set("group_id", group);
        v.save();
        vertices.add(v);
        words.put(uuid, new HashSet<>(Arrays.asList((name + " " + summary).split(" "))));
        groups.put(uuid, group);
      }
      for (int i = 0; i < NODES - 1; i++) {
        final String uuid = "r" + i;
        final String name = pick(random);
        final String fact = pick(random) + " " + pick(random) + " " + pick(random);
        final String group = "g" + random.nextInt(4);
        vertices.get(i).newEdge("RELATES_TO", vertices.get(i + 1)).set("uuid", uuid).set("name", name).set("fact", fact)
            .set("group_id", group).save();
        words.put(uuid, new HashSet<>(Arrays.asList((name + " " + fact).split(" "))));
        groups.put(uuid, group);
      }
    });
  }

  @Test
  void aTermAnswersEveryRecordHoldingItBestFirst() {
    final String cypher = "CALL db.index.fulltext.queryNodes(\"node_name_and_summary\", $query, {limit: $limit}) "
        + "YIELD node AS n, score RETURN n.uuid AS id, score ORDER BY score DESC";
    for (final String term : new String[] { "alice", "paris", "river", "garden" }) {
      final Set<String> expected = matching("n", Set.of(term), null);
      final List<Hit> hits = hits(cypher, Map.of("query", term, "limit", 1000));
      assertThat(hits.stream().map(h -> h.id).collect(Collectors.toSet())).as(term).isEqualTo(expected);
      assertThat(hits).as(term).hasSize(expected.size());
      assertThat(hits).allSatisfy(h -> assertThat(h.score).isPositive());
      for (int i = 1; i < hits.size(); i++)
        assertThat(hits.get(i).score).isLessThanOrEqualTo(hits.get(i - 1).score);
    }
  }

  @Test
  void theGroupQueryRestrictsTheRecordsTheCallAnswers() {
    final String cypher = "CALL db.index.fulltext.queryNodes(\"node_name_and_summary\", $query, {limit: $limit}) "
        + "YIELD node AS n, score RETURN n.uuid AS id, score";
    // one group, several groups, a group no record has
    assertThat(ids(cypher, "(group_id:\"g1\") AND (alice)", 1000)).isEqualTo(matching("n", Set.of("alice"), Set.of("g1")));
    assertThat(ids(cypher, "(group_id:\"g1\" OR group_id:\"g3\") AND (bob OR rome)", 1000))
        .isEqualTo(matching("n", Set.of("bob", "rome"), Set.of("g1", "g3")));
    assertThat(ids(cypher, "(group_id:\"nobody\") AND (alice)", 1000)).isEmpty();
    assertThat(ids(cypher, "(group_id:\"g2\") AND (zzzunknown)", 1000)).isEmpty();
  }

  @Test
  void theLimitOptionKeepsTheBestRowsAndAFilterAfterTheCallNeverAddsAny() {
    final String all = "CALL db.index.fulltext.queryNodes(\"node_name_and_summary\", $query, {limit: $limit}) "
        + "YIELD node AS n, score RETURN n.uuid AS id, score ORDER BY score DESC, id";
    final List<Hit> ranking = hits(all, Map.of("query", "alice OR engineer", "limit", 1000));
    assertThat(ranking.size()).isGreaterThan(20);

    for (final int limit : new int[] { 1, 5, 20 }) {
      final List<Hit> limited = hits(all, Map.of("query", "alice OR engineer", "limit", limit));
      assertThat(limited).as("limit " + limit).hasSize(limit);
      assertThat(limited.stream().map(h -> h.score).toList()).as("limit " + limit)
          .isEqualTo(ranking.subList(0, limit).stream().map(h -> h.score).toList());
    }
    assertThat(hits(all, Map.of("query", "alice OR engineer", "limit", 0))).isEmpty();

    // Graphiti's filter runs on the rows the call answered: the group filter cannot bring back a row the limit cut off
    final String filtered = "CALL db.index.fulltext.queryNodes(\"node_name_and_summary\", $query, {limit: $limit}) "
        + "YIELD node AS n, score WHERE n.group_id IN $groups RETURN n.uuid AS id, score ORDER BY score DESC, id";
    // records tied with the 10th score may be cut either way: the rows above it are all answered, the rows beyond the tie none
    final double cut = ranking.get(9).score;
    final Set<String> above = ranking.stream().filter(h -> h.score > cut).map(h -> h.id).filter(id -> "g1".equals(groups.get(id)))
        .collect(Collectors.toSet());
    final Set<String> upToTie = ranking.stream().filter(h -> h.score >= cut).map(h -> h.id).filter(id -> "g1".equals(groups.get(id)))
        .collect(Collectors.toSet());
    final Set<String> answered = hits(filtered, Map.of("query", "alice OR engineer", "limit", 10, "groups", List.of("g1"))).stream()
        .map(h -> h.id).collect(Collectors.toSet());
    assertThat(answered).containsAll(above).isSubsetOf(upToTie);
  }

  @Test
  void theSkipOptionDropsTheBestRows() {
    final String all = "CALL db.index.fulltext.queryNodes(\"node_name_and_summary\", $query, {skip: $skip, limit: $limit}) "
        + "YIELD node AS n, score RETURN n.uuid AS id, score ORDER BY score DESC, id";
    final List<Hit> ranking = hits(all, Map.of("query", "paris OR river", "skip", 0, "limit", 1000));
    final List<Hit> page = hits(all, Map.of("query", "paris OR river", "skip", 3, "limit", 4));
    assertThat(page).hasSize(4);
    assertThat(page.stream().map(h -> h.score).toList()).isEqualTo(ranking.subList(3, 7).stream().map(h -> h.score).toList());
    assertThat(hits(all, Map.of("query", "paris OR river", "skip", 1000, "limit", 5))).isEmpty();
  }

  @Test
  void theRelationshipIndexAnswersTheSameWay() {
    final String cypher = "CALL db.index.fulltext.queryRelationships(\"edge_name_and_fact\", $query, {limit: $limit}) "
        + "YIELD relationship AS e, score RETURN e.uuid AS id, score ORDER BY score DESC";
    final Set<String> expected = matching("r", Set.of("garden"), Set.of("g0", "g2"));
    final List<Hit> hits = hits(cypher, Map.of("query", "(group_id:\"g0\" OR group_id:\"g2\") AND (garden)", "limit", 1000));
    assertThat(hits.stream().map(h -> h.id).collect(Collectors.toSet())).isEqualTo(expected);
    assertThat(hits).hasSize(expected.size());
    for (int i = 1; i < hits.size(); i++)
      assertThat(hits.get(i).score).isLessThanOrEqualTo(hits.get(i - 1).score);

    final List<Hit> limited = hits(cypher, Map.of("query", "garden", "limit", 3));
    assertThat(limited).hasSize(3);
    assertThat(hits(cypher, Map.of("query", "garden", "limit", 0))).isEmpty();
  }

  // ---- helpers ------------------------------------------------------------------------------------------------------------------

  private record Hit(String id, double score) {
  }

  private static String pick(final Random random) {
    return WORDS[random.nextInt(WORDS.length)];
  }

  /** The records of the kind (n: nodes, r: relationships) holding any term, in any of the groups (null: any). */
  private Set<String> matching(final String kind, final Set<String> terms, final Set<String> inGroups) {
    return words.entrySet().stream().filter(e -> e.getKey().startsWith(kind))
        .filter(e -> e.getValue().stream().anyMatch(terms::contains))
        .filter(e -> inGroups == null || inGroups.contains(groups.get(e.getKey()))).map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  private Set<String> ids(final String cypher, final String query, final int limit) {
    return hits(cypher, Map.of("query", query, "limit", limit)).stream().map(h -> h.id).collect(Collectors.toSet());
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
}

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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9781
 * <p>
 * #9585 made a Gremlin map result a flat map whose {@code T.id} and {@code T.label} are named {@code @rid} and {@code @type}, but only
 * for the map a traversal returns as the row. A map one {@code by()} deeper ({@code project()}, {@code group()}, {@code fold()},
 * {@code path()}) kept the {@code id}/{@code label} spelling, so the record id was overwritten by a property named {@code id}, an edge
 * disagreed with its own endpoints, and keys that print alike were merged. The rule is applied once, where the answer leaves the
 * Gremlin module, so the row, the embedded {@code Result} and its JSON, and the HTTP answer all agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9781NestedMapKeysTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9781");
    final var a = graph.addVertex(T.label, "NP", "name", "a", "id", "uid");
    final var b = graph.addVertex(T.label, "NP", "name", "b");
    a.addEdge("knows", b, "id", "eid");
    graph.tx().commit();
  }

  @AfterEach
  void teardown() {
    graph.drop();
  }

  private List<Result> run(final String query) {
    final List<Result> results = new ArrayList<>();
    try (final ResultSet resultSet = graph.gremlin(query).execute()) {
      while (resultSet.hasNext())
        results.add(resultSet.next());
    }
    return results;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(final Object value) {
    assertThat(value).isInstanceOf(Map.class);
    return (Map<String, Object>) value;
  }

  private String ridOf(final String name) {
    return run("g.V().has('name','" + name + "').elementMap()").getFirst().getProperty("@rid").toString();
  }

  @Test
  void aNestedElementMapNamesTheTokensLikeTheRowDoes() {
    final String ridOfA = ridOf("a");
    final String ridOfB = ridOf("b");

    final List<Result> rows = run("g.V().hasLabel('NP').order().by('name').project('x').by(elementMap())");
    assertThat(rows).hasSize(2);

    final Map<String, Object> x = map(rows.get(0).getProperty("x"));
    assertThat(x.keySet()).containsExactlyInAnyOrder("@rid", "@type", "id", "name");
    assertThat(x.get("@rid").toString()).as("the record id is not displaced by the property called id").isEqualTo(ridOfA);
    assertThat(x.get("@type")).isEqualTo("NP");
    assertThat(x.get("id")).isEqualTo("uid");

    final Map<String, Object> y = map(rows.get(1).getProperty("x"));
    assertThat(y.keySet()).as("a client can tell the rid from a property called id").containsExactlyInAnyOrder("@rid", "@type", "name");
    assertThat(y.get("@rid").toString()).isEqualTo(ridOfB);
  }

  @Test
  void theJsonOfANestedMapAgreesWithTheMap() {
    final String ridOfA = ridOf("a");

    final JSONObject json = run("g.V().has('name','a').project('x').by(elementMap())").getFirst().toJSON();
    final JSONObject x = json.getJSONObject("x");
    assertThat(x.getString("@rid")).isEqualTo(ridOfA);
    assertThat(x.getString("@type")).isEqualTo("NP");
    assertThat(x.getString("id")).isEqualTo("uid");
    assertThat(x.has("label")).isFalse();
  }

  @Test
  void aGroupedElementMapNamesTheTokens() {
    final String ridOfA = ridOf("a");

    final Result row = run("g.V().hasLabel('NP').group().by('name').by(elementMap())").getFirst();
    final Map<String, Object> a = map(row.getProperty("a"));
    assertThat(a.get("@rid").toString()).isEqualTo(ridOfA);
    assertThat(a.get("id")).isEqualTo("uid");
    assertThat(map(row.getProperty("b")).get("@type")).isEqualTo("NP");

    final JSONObject json = row.toJSON();
    assertThat(json.getJSONObject("a").getString("@rid")).isEqualTo(ridOfA);
  }

  @Test
  void aFoldedListOfElementMapsNamesTheTokens() {
    final String ridOfA = ridOf("a");

    final Result row = run("g.V().has('name','a').elementMap().fold()").getFirst();
    final Object folded = row.getProperty("result");
    assertThat(folded).isInstanceOf(Collection.class);
    final Map<String, Object> only = map(((Collection<?>) folded).iterator().next());
    assertThat(only.get("@rid").toString()).isEqualTo(ridOfA);
    assertThat(only.get("id")).isEqualTo("uid");

    final JSONArray array = row.toJSON().getJSONArray("result");
    assertThat(array.getJSONObject(0).getString("@rid")).isEqualTo(ridOfA);
  }

  @Test
  void aPathOfElementMapsStaysAPathAndNamesTheTokens() {
    final String ridOfA = ridOf("a");

    final Result row = run("g.V().has('name','a').path().by(elementMap())").getFirst();
    final Object path = row.getProperty("result");
    assertThat(path).as("the object a traversal answers is still a Path").isInstanceOf(Path.class);
    assertThat(map(((Path) path).get(0)).get("@rid").toString()).isEqualTo(ridOfA);

    final JSONArray array = row.toJSON().getJSONArray("result");
    assertThat(array.getJSONObject(0).getString("@rid")).isEqualTo(ridOfA);
  }

  @Test
  void anEdgeAndItsEndpointsUseTheSameNames() {
    final Map<String, Object> edge = run("g.E().hasLabel('knows').elementMap()").getFirst().toMap();
    assertThat(edge).containsKeys("@rid", "@type", "id");
    assertThat(edge.get("id")).isEqualTo("eid");

    final Map<String, Object> in = map(edge.get("IN"));
    final Map<String, Object> out = map(edge.get("OUT"));
    assertThat(in.keySet()).containsExactlyInAnyOrder("@rid", "@type");
    assertThat(out.keySet()).containsExactlyInAnyOrder("@rid", "@type");
    assertThat(out.get("@rid").toString()).isEqualTo(ridOf("a"));
    assertThat(in.get("@rid").toString()).isEqualTo(ridOf("b"));
  }

  @Test
  void aNestedMapKeyedByDifferentNumberTypesKeepsEveryEntry() {
    // 1 and 1L print alike: the entries are told apart by the type, as they are in the row
    final Map<Object, Object> counts = new LinkedHashMap<>();
    counts.put(1, 1L);
    counts.put(1L, 2L);

    final Result nested = ArcadeGremlin.mapToResult(Map.of("n", counts));

    final Map<String, Object> inner = map(nested.getProperty("n"));
    assertThat(inner).containsOnlyKeys("1:Integer", "1:Long");
    assertThat(inner.get("1:Integer")).isEqualTo(1L);
    assertThat(inner.get("1:Long")).isEqualTo(2L);
    assertThat(nested.toJSON().getJSONObject("n").keySet()).containsExactlyInAnyOrder("1:Integer", "1:Long");
  }

  @Test
  void aTokenInsideAListInsideAMapIsNamedToo() {
    final Map<Object, Object> element = new LinkedHashMap<>();
    element.put(T.id, "#1:0");
    element.put("id", "uid");

    final Result row = ArcadeGremlin.mapToResult(Map.of("items", List.of(element)));

    final Map<String, Object> only = map(((List<?>) row.getProperty("items")).getFirst());
    assertThat(only).containsOnlyKeys("@rid", "id");
    assertThat(only.get("@rid")).isEqualTo("#1:0");
  }

  @Test
  void aNestedNullKeyIsNamedNull() {
    final Result row = run("g.V().hasLabel('NP').project('g').by(group().by(constant(null)).by(count()))").getFirst();
    final Map<String, Object> grouped = map(row.getProperty("g"));
    assertThat(grouped).containsOnlyKeys("null");
    // ONE TRAVERSER PER VERTEX: THE GROUP OF EACH ROW COUNTS ITS OWN VERTEX
    assertThat(row.toJSON().getJSONObject("g").getLong("null")).isEqualTo(1L);
  }

  @Test
  void aTopLevelMapAndANestedOneAreTheSameShape() {
    final Map<String, Object> top = run("g.V().has('name','a').elementMap()").getFirst().toMap();
    final Map<String, Object> nested = map(run("g.V().has('name','a').project('x').by(elementMap())").getFirst().getProperty("x"));
    assertThat(nested).isEqualTo(top);
  }

  @Test
  void aSetHoldingMapsStaysASetOfNamedMaps() {
    final Map<Object, Object> first = new LinkedHashMap<>();
    first.put(T.id, "#1:0");
    final Map<Object, Object> second = new LinkedHashMap<>();
    second.put(T.id, "#2:0");
    final Set<Object> set = new LinkedHashSet<>(List.of(first, second));

    final Object normalized = ArcadeGremlin.normalizeNested(set);

    assertThat(normalized).isInstanceOf(Set.class);
    final List<?> entries = List.copyOf((Set<?>) normalized);
    assertThat(entries).hasSize(2);
    assertThat(map(entries.get(0))).containsOnlyKeys("@rid");
    assertThat(map(entries.get(1)).get("@rid")).isEqualTo("#2:0");
  }

  @Test
  void aBulkSetKeepsItsCountsInsteadOfExpandingEveryOccurrence() {
    final Map<Object, Object> element = new LinkedHashMap<>();
    element.put(T.id, "#1:0");
    element.put("id", "uid");
    final BulkSet<Object> bulk = new BulkSet<>();
    // A MILLION OCCURRENCES OF ONE MAP: EXPANDED INTO A LIST THEY WOULD BE A MILLION COPIES
    bulk.add(element, 1_000_000L);
    bulk.add("plain", 2L);

    final Object normalized = ArcadeGremlin.normalizeNested(bulk);

    assertThat(normalized).isInstanceOf(BulkSet.class);
    final BulkSet<?> named = (BulkSet<?>) normalized;
    assertThat(named.asBulk()).hasSize(2);
    assertThat(named.size()).isEqualTo(1_000_002);
    final Map.Entry<?, Long> first = named.asBulk().entrySet().iterator().next();
    assertThat(map(first.getKey())).containsOnlyKeys("@rid", "id");
    assertThat(first.getValue()).isEqualTo(1_000_000L);

    final BulkSet<Object> scalars = new BulkSet<>();
    scalars.add("a", 3L);
    assertThat(ArcadeGremlin.normalizeNested(scalars)).as("nothing to name, nothing rebuilt").isSameAs(scalars);
  }

  @Test
  void nothingIsRebuiltWhenNoMapIsNested() {
    final List<Object> original = new ArrayList<>(List.of("a", 1L, List.of("b")));
    assertThat(ArcadeGremlin.normalizeNested(original)).isSameAs(original);
    assertThat(ArcadeGremlin.normalizeNested("text")).isEqualTo("text");
    assertThat(ArcadeGremlin.normalizeNested(null)).isNull();
  }
}

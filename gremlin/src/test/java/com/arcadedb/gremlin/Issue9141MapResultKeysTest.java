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
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9141
 * <p>
 * {@code graph.gremlin(...)} turned every key of a map result into a string and kept the last value on a collision, so
 * {@code elementMap()} lost the element's id and label to a property named {@code id} or {@code label}, {@code groupCount()} merged
 * keys that differ by type, and a null key threw a NullPointerException.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9141MapResultKeysTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9141");
    graph.addVertex(T.label, "person", "name", "a", "id", "user-42", "label", "VIP", "val", 1);
    graph.addVertex(T.label, "person", "name", "b", "val", 1L);
    graph.addVertex(T.label, "person", "name", "c", "val", 1.0d);
    graph.addVertex(T.label, "person", "name", "d", "val", "1");
    graph.addVertex(T.label, "person", "name", "e", "val", true);
    graph.addVertex(T.label, "person", "name", "f", "val", new BigDecimal("1.0"));
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

  @Test
  void aMapWithDistinctKeysStaysFlat() {
    final List<Result> results = run("g.V().has('name','b').elementMap('name')");
    assertThat(results).hasSize(1);
    assertThat(results.get(0).<String>getProperty("name")).isEqualTo("b");
    assertThat(results.get(0).<String>getProperty("@type")).isEqualTo("person");
    assertThat(results.get(0).<Object>getProperty("@rid")).isNotNull();
  }

  // #9584, #9576: THE SHAPE IS CHOSEN BY THE QUERY, NEVER BY THE DATA. T.id / T.label ARE ALWAYS @rid / @type
  @Test
  void elementMapKeepsTheIdAndLabelBesideTheProperties() {
    final Result colliding = run("g.V().has('name','a').elementMap()").get(0);
    assertThat(colliding.<Object>getProperty("result")).isNull();
    assertThat(colliding.getPropertyNames()).containsExactlyInAnyOrder("@rid", "@type", "name", "id", "label", "val");
    assertThat(colliding.<String>getProperty("id")).isEqualTo("user-42");
    assertThat(colliding.<String>getProperty("label")).isEqualTo("VIP");
    assertThat(colliding.<String>getProperty("@type")).isEqualTo("person");

    final Result plain = run("g.V().has('name','b').elementMap()").get(0);
    assertThat(plain.getPropertyNames()).containsExactlyInAnyOrder("@rid", "@type", "name", "val");
  }

  @Test
  void valueMapWithTokensIsAFlatMapToo() {
    final Result result = run("g.V().has('name','a').valueMap(true)").get(0);
    assertThat(result.<Object>getProperty("result")).isNull();
    assertThat(result.<String>getProperty("@type")).isEqualTo("person");
    assertThat(result.<Object>getProperty("@rid")).isNotNull();
  }

  @Test
  void groupCountKeepsKeysThatDifferOnlyByType() {
    final List<Result> results = run("g.V().hasLabel('person').groupCount().by('val')");
    assertThat(results).hasSize(1);
    final Result result = results.get(0);
    assertThat(result.<Object>getProperty("result")).isNull();
    assertThat(result.getPropertyNames()).hasSize(6);
    for (final String name : result.getPropertyNames())
      assertThat(result.<Long>getProperty(name)).isEqualTo(1L);
  }

  @Test
  void threeKeysThatPrintAlikeAreAllKept() {
    final Result result = run("g.V().has('name',within('a','b','d')).groupCount().by('val')").get(0);
    assertThat(result.getPropertyNames()).containsExactlyInAnyOrder("1", "1:Long", "1:String");
  }

  @Test
  void aPropertyNamedLikeATokenKeyIsKept() {
    graph.addVertex(T.label, "person", "name", "g", "@rid", "mine");
    graph.tx().commit();
    final Result result = run("g.V().has('name','g').elementMap()").get(0);
    assertThat(result.<String>getProperty("@type")).isEqualTo("person");
    assertThat(result.<String>getProperty("@rid:String")).isEqualTo("mine");
    assertThat(result.<String>getProperty("@rid")).startsWith("#");
  }

  @Test
  void aNullKeyIsAnswered() {
    final List<Result> results = run("g.V().hasLabel('person').group().by(constant(null)).by(count())");
    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Long>getProperty("null")).isEqualTo(6L);
  }
}

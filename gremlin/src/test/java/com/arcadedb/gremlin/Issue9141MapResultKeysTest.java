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
import java.util.Map;

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
    assertThat(results.get(0).<String>getProperty("label")).isEqualTo("person");
  }

  @Test
  void elementMapKeepsTheIdAndLabelBesideTheProperties() {
    final List<Result> results = run("g.V().has('name','a').elementMap()");
    assertThat(results).hasSize(1);
    final List<Map<String, Object>> entries = results.get(0).getProperty("result");
    assertThat(entries).hasSize(6);
    assertThat(entries).extracting(e -> e.get("key") + "=" + e.get("value")).contains("id=user-42", "label=VIP", "label=person");
    assertThat(entries).filteredOn(e -> e.get("key") == T.id).hasSize(1);
    assertThat(entries).filteredOn(e -> e.get("key") == T.label).extracting(e -> e.get("value")).containsExactly("person");
  }

  @Test
  void groupCountKeepsKeysThatDifferOnlyByType() {
    final List<Result> results = run("g.V().hasLabel('person').groupCount().by('val')");
    assertThat(results).hasSize(1);
    final List<Map<String, Object>> entries = results.get(0).getProperty("result");
    assertThat(entries).hasSize(6);
    assertThat(entries).extracting(e -> e.get("value")).containsOnly(1L);
  }

  @Test
  void aNullKeyIsAnswered() {
    final List<Result> results = run("g.V().hasLabel('person').group().by(constant(null)).by(count())");
    assertThat(results).hasSize(1);
    assertThat(results.get(0).<Long>getProperty("null")).isEqualTo(6L);
  }
}

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
import com.arcadedb.serializer.json.JSONObject;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/9805
 * <p>
 * The keys of a Gremlin map one {@code by()} deeper are the {@code T} enum ({@code elementMap()}) or any value
 * ({@code groupCount()}). The shared {@code JSONObject} constructor cast every key to {@code String}, so {@code Result.toJSON()}
 * on such a row threw {@code ClassCastException: class org.apache.tinkerpop.gremlin.structure.T$2 cannot be cast to class
 * java.lang.String}, while the same traversal over HTTP answered. A JSON member name is derived from the key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9805GremlinMapKeyToJsonTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-issue9805");
    graph.addVertex(T.label, "Person", "name", "Jay", "age", 42L);
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
  void aNestedElementMapRendersWithItsTokenKeysNamedLikeTheRow() {
    final List<Result> rows = run("g.V().project('x').by(elementMap())");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("x")).isInstanceOf(Map.class);

    final JSONObject json = rows.getFirst().toJSON();
    final JSONObject x = json.getJSONObject("x");
    // #9781: the tokens are @rid / @type one level down as they are in the row, so a property called id or label cannot displace them
    assertThat(x.keySet()).containsExactlyInAnyOrder("@rid", "@type", "name", "age");
    assertThat(x.getString("@type")).isEqualTo("Person");
    assertThat(x.getString("name")).isEqualTo("Jay");
  }

  @Test
  void aGroupedElementMapRenders() {
    final List<Result> rows = run("g.V().group().by('name').by(elementMap())");
    assertThat(rows).hasSize(1);
    final JSONObject json = rows.getFirst().toJSON();
    assertThat(json.getJSONObject("Jay").getString("@type")).isEqualTo("Person");
  }

  @Test
  void aCountedMapKeyedByANumberRenders() {
    final List<Result> rows = run("g.V().project('byAge').by(groupCount().by('age'))");
    assertThat(rows).hasSize(1);
    final JSONObject byAge = rows.getFirst().toJSON().getJSONObject("byAge");
    assertThat(byAge.keySet()).hasSize(1);
  }
}
